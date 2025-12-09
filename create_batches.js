const fs = require("fs");
const path = require("path");

const config_file_path = path.join(__dirname, 'batch_configuration.json');
const batch_directory_path = path.join(__dirname, 'batches');

function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

function formatValue(v, key) {
  if (v === null || v === undefined) return "NULL";

  // Trim string values
  if (typeof v === "string") {
    v = v.trim();
  }

  // Treat N/A, NA, etc. as null
  if (typeof v === "string" && (v.toUpperCase() === "N/A" || v.toUpperCase() === "NA")) return "NULL";

  // Special handling for dates in the source data
  if (key === "Contract Date" && typeof v === "string") {
    v = v.replace("T", " ").replace("Z", "+00");
  }

  if (v === null || v === undefined || (typeof v === "string" && v === "")) return "NULL";

  // Escape single quotes for SQL
  return `'${String(v).replace(/'/g, "''")}'`;
}

function getBatchConfig() {
  let batchConfig = {};
  if (fs.existsSync(config_file_path)) {
    batchConfig = JSON.parse(fs.readFileSync(config_file_path, 'utf8'));
    console.log(`📂 Loaded batch configuration with ${Object.keys(batchConfig).length} entries.`);
  }

  return batchConfig;
}

function generateSql(data){
  const table = "construction_permits";
  const columns = [
    "county",
    "street_address",
    "city",
    "zip_code",
    "state",
    "latitude",
    "longitude",
    "geocode_source",
    "contract_amount",
    "contract_date",
    "contractor_name",
    "contractor_address",
    "contractor_phone",
    "permit_type",
    "owner_name",
    "owner_phone",
    "data_hash"
  ];

    const values = data.map((d) => {
        const rowFields = [
          d.County,
          d["Street Address"],
          d.City,
          d.ZipCode,
          d.State,
          d.Latitude,
          d.Longitude,
          d["Geocode Source"],
          d["Contract Amount"],
          d["Contract Date"],
          d["Contractor Name"],
          d["Contractor Address"],
          d["Contractor Phone"],
          d.Type,
          d["Owner Name"],
          d["Owner Phone"]
        ];

        const row = rowFields
          .map((v, i) => formatValue(v, [
            "County",
            "Street Address",
            "City",
            "ZipCode",
            "State",
            "Latitude",
            "Longitude",
            "Geocode Source",
            "Contract Amount",
            "Contract Date",
            "Contractor Name",
            "Contractor Address",
            "Contractor Phone",
            "Type",
            "Owner Name",
            "Owner Phone"
          ][i]))
          .join(", ");

           // Calculate data hash for the row

        const crypto = require("crypto");
        const hash = crypto.createHash("sha256");
        hash.update(row);
        const dataHash = hash.digest("hex");

        return `(${row}, '${dataHash}')`;
      })
      .join(",\n");

    const sql = `INSERT INTO ${table} (${columns.join(", ")}) VALUES\n${values} on conflict do nothing;`;

    return sql
}


function createBatchedSQLFiles(data, batchSize = 100, batchConfig) {

  // ensure output directory exists
  fs.mkdirSync(batch_directory_path, { recursive: true });


  // Loop through batch config and process failed or unprocessed entries

  let alreadyProcessedRowCount = 0;
  const oldChunks = Object.keys(batchConfig);
  const updatedFiles = [];
  // Process existing batches
  oldChunks.forEach((filename) => {
    const batch = batchConfig[filename];
    const startIndex = alreadyProcessedRowCount;
    const endIndex = alreadyProcessedRowCount + batch.row_count;
    alreadyProcessedRowCount += batch.row_count;

    if (batch.status !== "failed") {
      return;
    }

    // Rewrite the SQL file for re-processing for failed batches
    const oldChunk = data.slice(startIndex, endIndex);

    const sql = generateSql(oldChunk);

    const filePath = path.join(batch_directory_path, filename);
    fs.writeFileSync(filePath, sql, "utf8");
    updatedFiles.push({ file: filename, rows: oldChunk.length });
  });

  const newlyAddedData = data.slice(alreadyProcessedRowCount);
  const newChunks = chunkArray(newlyAddedData, batchSize);
  const writtenFiles = [];
  newChunks.forEach((chunk, idx) => {
    const sql = generateSql(chunk)
    const filename = `batch_${idx + 1 + oldChunks.length}.sql`;

    const filePath = path.join(batch_directory_path, filename);
    fs.writeFileSync(filePath, sql, "utf8");
    
    // Add to batch configuration with filename as key
    batchConfig[filename] = {
      file_name: filename,
      row_count: chunk.length,
      status: 'pending'
    };
    
    writtenFiles.push({ file: filename, rows: chunk.length });
  });

  // Save updated batch configuration
  fs.writeFileSync(config_file_path, JSON.stringify(batchConfig, null, 2), 'utf8');
  console.log(`💾 Updated batch configuration saved.`);

  return {
    updated: updatedFiles,
    writtenFiles: writtenFiles
  };
}

const permits = require('./latest_cleaned.json');
const batchConfig = getBatchConfig();

console.log(`📊 Total permits to process: ${permits.length}`);
const result = createBatchedSQLFiles(permits, 50, batchConfig);
console.log(`✅ Created ${result.writtenFiles.length} new batch file(s) and 🔄 updated ${result.updated.length} existing batch file(s).`);