require('dotenv').config();
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const { Client } = require('pg');
const https = require('https');
const http = require('http');
const readline = require('readline');

async function askUserForNotification() {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  return new Promise((resolve) => {
    rl.question('📧 Do you want to send a notification? (y/n): ', (answer) => {
      rl.close();
      resolve(answer.toLowerCase() === 'y' || answer.toLowerCase() === 'yes');
    });
  });
}

async function sendNotification(summary) {
  const notificationUrl = process.env.NOTIFICATION_URL;
  
  if (!notificationUrl) {
    console.warn('⚠️  NOTIFICATION_URL not set in .env file. Skipping notification.');
    return;
  }

  const url = new URL(notificationUrl);
  const protocol = url.protocol === 'https:' ? https : http;

  const postData = JSON.stringify(summary);

  const options = {
    hostname: url.hostname,
    port: url.port,
    path: url.pathname + url.search,
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(postData)
    }
  };

  return new Promise((resolve, reject) => {
    const req = protocol.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => {
        data += chunk;
      });
      res.on('end', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          console.log('✅ Notification sent successfully');
          resolve(data);
        } else {
          console.error(`❌ Notification failed with status ${res.statusCode}`);
          reject(new Error(`HTTP ${res.statusCode}`));
        }
      });
    });

    req.on('error', (error) => {
      console.error('❌ Error sending notification:', error.message);
      reject(error);
    });

    req.write(postData);
    req.end();
  });
}

async function main() {
  const logFile = path.join(__dirname, 'errors.log');
  const configPath = path.join(__dirname, 'batch_configuration.json');
  
  try {
    // Run test.js to generate batch files
    console.log('🔧 Generating batch files...');
    execSync('node create_batches.js', { stdio: 'inherit' });

    // Load batch configuration
    if (!fs.existsSync(configPath)) {
      console.error('❌ No batch configuration found.');
      return;
    }
    
    let batchConfig = JSON.parse(fs.readFileSync(configPath, 'utf8'));

    // Connect to database
    console.log('🔌 Connecting to database...');
    const client = new Client({
      connectionString: process.env.DATABASE_URL,
    });
    await client.connect();
    console.log('✅ Connected to database successfully');

    // Create done folder if it doesn't exist
    const doneDir = path.join(__dirname, 'done');
    fs.mkdirSync(doneDir, { recursive: true });

    // Process batch files based on configuration
    const batchesDir = path.join(__dirname, 'batches');

    console.log(`🗄️ Executing queries...`);
    let processedCount = 0;
    let failedCount = 0;
    let skippedCount = 0;
    let insertedRowCount = 0;
    
    for (const filename in batchConfig) {
      const batch = batchConfig[filename];
      
      // Skip already processed batches
      if (batch.status === 'done') {
        skippedCount++;
        continue;
      }

      const filePath = path.join(batchesDir, batch.file_name);
      const donePath = path.join(doneDir, batch.file_name);

      // Check if file exists
      if (!fs.existsSync(filePath)) {
        console.warn(`⚠️  File ${batch.file_name} not found, recheck batch configuration.`);
        continue;
      }

      const sql = fs.readFileSync(filePath, 'utf8');

      try {
        const response = await client.query(sql);
        insertedRowCount += response?.rowCount || 0;

        // Update status to done
        batchConfig[filename].status = 'done';
        // Move to done folder
        fs.renameSync(filePath, donePath);
        processedCount++;
      } catch (err) {
        console.error(`❌ Error processing ${batch.file_name}:`, err.message);
        // Update status to error
        batchConfig[filename].status = 'failed';
        // Log error to file with new line separator
        fs.appendFileSync(logFile, `\n${new Date().toISOString()}: Error processing ${batch.file_name}: ${err.message}`);
        failedCount++;
      }

      fs.writeFileSync(configPath, JSON.stringify(batchConfig, null, 2), 'utf8');
    }
    await client.end();
    
    const summary = {
      processedCount,
      failedCount,
      skippedCount,
      totalBatches: Object.keys(batchConfig).length,
      timestamp: new Date().toISOString()
    };
    
    console.log('🎉 All batch files processed.');
    console.log(`📊 Summary: ✅ ${processedCount} files processed, ❌ ${failedCount} failed, ${insertedRowCount} new data added, ${skippedCount} skipped`);
    
    // Ask user if they want to send notification
    if (insertedRowCount > 0) {
      const shouldNotify = await askUserForNotification();
      
      if (shouldNotify) {
        console.log('📧 Sending notification...');
        try {
          await sendNotification(summary);
        } catch (error) {
          console.error('⚠️  Failed to send notification, but processing completed.');
        }
      } else {
        console.log('⏭️  Notification skipped.');
      }
    }
  } 
  catch (err) {
    console.error('💥 An error occurred:', err.message);
    // Log general errors with new line separator
    fs.appendFileSync(logFile, `\n${new Date().toISOString()}: General error: ${err.message}\n${err.stack}\n`);
  }
  finally{
    fs.appendFileSync(logFile, `\n\n`);
  }
}

main();