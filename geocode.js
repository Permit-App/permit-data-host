// geocode.js
require('dotenv').config();
const path = require("path");
const { Storage } = require("@google-cloud/storage");
const { readJsonFile, writeJsonFile } = require('./utils');
const { execSync } = require('child_process');
const {
  GCS_BUCKET,
  INCOMING_FOLDER = "incoming",
  ENCODED_FOLDER = "encoded",
  GOOGLE_MAPS_API_KEY,
} = process.env;

if (!GCS_BUCKET) throw new Error("GCS_BUCKET env var is required");
if (!GOOGLE_MAPS_API_KEY) throw new Error("GOOGLE_MAPS_API_KEY env var is required");

const storage = new Storage();
const bucket = storage.bucket(GCS_BUCKET);

async function writeJsonToBucket(filename, obj) {
  const file = bucket.file(filename);
  const data = JSON.stringify(obj, null, 2);
  await file.save(data, { contentType: "application/json" });
  console.log(`Wrote ${filename} to GCS bucket ${GCS_BUCKET}`);
}

async function deleteFileFromBucket(filename) {
  const file = bucket.file(filename);
  await file.delete();
  console.log(`Deleted ${filename} from GCS bucket ${GCS_BUCKET}`);
}

async function listIncomingFiles() {
  const options = { prefix: `${INCOMING_FOLDER.replace(/\/$/, "")}/`, delimiter: undefined };
  const [files] = await bucket.getFiles(options);

  // filter out "folders" objects (GCS lists objects only). Exclude any file that is processing.json or inside processed folder
  return files.filter(f => {
    const name = f.name || "";
    if (!name) return false;
    if (!name.endsWith(".json")) return false;

    return true;
  });
}


// Geocode address
async function geoCodeAddress(address) {
  const encoded = encodeURIComponent(address);
  const url = `https://maps.googleapis.com/maps/api/geocode/json?address=${encoded}&key=${GOOGLE_MAPS_API_KEY}`;
  const res = await fetch(url);

  if (!res.ok) {
    throw new Error(`Geocoding API request failed: ${res.status} ${res.statusText}`);
  }
  const data = await res.json();
  if (data.status !== "OK" || !data.results || data.results.length === 0) {
    console.warn(`Geocode not OK for "${address}": status=${data.status}`);
    return null;
  }
  const loc = data.results[0].geometry.location;
  return { lat: loc.lat, lng: loc.lng };
}

async function geoCodeEntries(entries){
  const geoCodedEntries = [];

  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i];

    // Validate required address fields
    if (!entry['Street Address'] || !entry.City || !entry.State || !entry.ZipCode) {
      throw new Error(`Missing required address fields for entry: ${JSON.stringify(entry)}`);
    }
    
    const fullAddress = `${entry['Street Address']}, ${entry.City}, ${entry.State} ${entry.ZipCode}`;
    const geoCoded = await geoCodeAddress(fullAddress);
    geoCodedEntries.push({
      ...entry,
      Latitude: geoCoded ? geoCoded.lat : null,
      Longitude: geoCoded ? geoCoded.lng : null,
    });
  } 

  return geoCodedEntries;
}

async function main() {
  // 1) List incoming files
  const incomingFiles = await listIncomingFiles();
  const latestCleaned = readJsonFile(path.join(__dirname, 'latest_cleaned.json'));

  if (incomingFiles.length === 0) {
    console.log("No incoming files to process.");
    return true;
  }

  for (let i = 0; i < incomingFiles.length; i++) {
    try {
      const currentFile = incomingFiles[i];
      // Download the file
      const fileContents = await currentFile.download();

      // Geocode entries
      const encodedJson = await geoCodeEntries(JSON.parse(fileContents.toString('utf8')));

      // Write encoded file to ENCODED_FOLDER and delete from INCOMING_FOLDER
      await writeJsonToBucket(`${ENCODED_FOLDER}/${path.basename(currentFile.name)}`, encodedJson);
      await deleteFileFromBucket(`${INCOMING_FOLDER}/${path.basename(currentFile.name)}`);

      // Update latest_cleaned.json
      latestCleaned.push(...encodedJson);
    }
    catch (err) {
      console.error("Error processing file:", err);
    }
  }

  // Write updated latest_cleaned.json back to GitHub
  writeJsonFile(path.join(__dirname, 'latest_cleaned.json'), latestCleaned);

  // Add latest_cleaned.json to staging and commit
  execSync('git add latest_cleaned.json', { stdio: 'inherit' });
  const commitMessage = `Update latest_cleaned.json with ${incomingFiles.length} new files processed ${new Date().toISOString()}`;
  execSync(`git commit -m "${commitMessage}"`, { stdio: 'inherit' });
  execSync('git push origin main', { stdio: 'inherit' });

  return true
  
}

main().catch(err => {
    console.error("Error:", err);
    process.exit(1);
}).then(() => {
    console.log("✅ Geocoding process completed.");
    process.exit(0);
});
