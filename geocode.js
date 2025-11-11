require('dotenv').config();
const fs = require('fs');
const path = require('path');
const https = require('https');

// Sleep utility function
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const geocode = async (fullAddress) => {
  const apiKey = process.env.GOOGLE_MAPS_API_KEY;
  
  if (!apiKey) {
    console.error('❌ GOOGLE_MAPS_API_KEY not found in environment variables');
    return null;
  }

  const url = 'https://maps.googleapis.com/maps/api/geocode/json' +
    `?address=${encodeURIComponent(fullAddress)}` +
    `&key=${encodeURIComponent(apiKey)}&region=us`;

  let delay = 1100;
  
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const response = await fetch(url);
      const code = response.status;

      if (code === 200) {
        const json = await response.json();
        const status = json.status;

        if (status === 'OK' && json.results && json.results.length > 0) {
          return json.results[0].geometry.location; // { lat, lng }
        }

        // Handle API statuses explicitly
        if (['ZERO_RESULTS', 'REQUEST_DENIED', 'INVALID_REQUEST'].includes(status)) {
          return null;
        }

        if (status === 'OVER_QUERY_LIMIT') {
          console.warn(`⚠️  Rate limit hit, waiting ${delay}ms before retry ${attempt}/3`);
          await sleep(delay);
          delay = Math.min(delay * 2, 8000);
          continue;
        }

        return null;
      }

      // HTTP-level rate limits
      if (code === 429) {
        console.warn(`⚠️  HTTP 429 rate limit, waiting ${delay}ms before retry ${attempt}/3`);
        await sleep(delay);
        delay = Math.min(delay * 2, 8000);
        continue;
      }

      if (code === 402 || code === 403) {
        console.error(`❌ API quota/key issues (HTTP ${code})`);
        return null;
      }
      
      return null;
    } catch (error) {
      console.error(`❌ Network error on attempt ${attempt}/3:`, error.message);
      if (attempt < 3) {
        await sleep(delay);
        delay = Math.min(delay * 2, 8000);
      }
    }
  }
  return null;
};

const updateGeocodingInData = async () => {
  const dataPath = path.join(__dirname, 'latest_cleaned.json');
  
  if (!fs.existsSync(dataPath)) {
    console.error('❌ latest_cleaned.json file not found');
    return;
  }

  console.log('📂 Loading data from latest_cleaned.json...');
  const data = JSON.parse(fs.readFileSync(dataPath, 'utf8'));
  
  console.log(`🔍 Found ${data.length} records to check`);
  
  let updatedCount = 0;
  let errorCount = 0;
  let skippedCount = 0;

  for (let i = 0; i < data.length; i++) {
    const record = data[i];
    const fullAddress = `${record["Street Address"]}, ${record.City}, ${record.State} ${record.ZipCode}`;
    
    console.log(`🔎 [${i + 1}/${data.length}] Checking: ${fullAddress}`);
    
    try {
      const newCoords = await geocode(fullAddress);
      
      if (newCoords) {
        const currentLat = parseFloat(record.Latitude);
        const currentLng = parseFloat(record.Longitude);
        const newLat = newCoords.lat;
        const newLng = newCoords.lng;
        
        // Check if coordinates are significantly different (tolerance of 0.001 degrees ≈ 100m)
        const latDiff = Math.abs(currentLat - newLat);
        const lngDiff = Math.abs(currentLng - newLng);
        
        if (latDiff > 0.001 || lngDiff > 0.001) {
          console.log(`📍 Updating coordinates for ${record["Street Address"]}`);
          console.log(`   Old: ${currentLat}, ${currentLng}`);
          console.log(`   New: ${newLat}, ${newLng}`);
          
          record.Latitude = newLat;
          record.Longitude = newLng;
          record["Geocode Source"] = "Google Maps API";
          updatedCount++;
        } else {
          console.log(`✅ Coordinates unchanged (within tolerance)`);
          skippedCount++;
        }
      } else {
        console.warn(`⚠️  Could not geocode: ${fullAddress}`);
        errorCount++;
      }
      
      // Rate limiting - wait between requests
      await sleep(100);
      
    } catch (error) {
      console.error(`❌ Error processing ${fullAddress}:`, error.message);
      errorCount++;
    }
  }

  if (updatedCount > 0) {
    console.log('💾 Saving updated data...');
    fs.writeFileSync(dataPath, JSON.stringify(data, null, 2), 'utf8');
    console.log('✅ Data saved successfully');
  }

  console.log('🎉 Geocoding complete!');
  console.log(`📊 Summary:`);
  console.log(`   ✅ Updated: ${updatedCount}`);
  console.log(`   ⏭️  Skipped: ${skippedCount}`);
  console.log(`   ❌ Errors: ${errorCount}`);
};

// Run the geocoding update
updateGeocodingInData().catch(error => {
  console.error('💥 Fatal error:', error.message);
  process.exit(1);
});