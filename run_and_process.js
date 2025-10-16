require('dotenv').config();
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const { Client } = require('pg');

async function main() {
  const logFile = path.join(__dirname, 'errors.log');
  try {
    // Run test.js to generate batch files
    console.log('Running test.js to generate batch files...');
    execSync('node test.js', { stdio: 'inherit' });

    // Connect to database
    console.log('Connecting to database...');
    const client = new Client({
      connectionString: process.env.DATABASE_URL, // Set this environment variable with your database connection string
    });
    await client.connect();

    // Create done folder if it doesn't exist
    const doneDir = path.join(__dirname, 'done');
    fs.mkdirSync(doneDir, { recursive: true });

    // Process batch files
    const batchesDir = path.join(__dirname, 'batches');
    const files = fs.readdirSync(batchesDir).filter(f => f.endsWith('.sql'));

    for (const file of files) {
      const filePath = path.join(batchesDir, file);
      const donePath = path.join(doneDir, file);

      // Skip if already processed
      if (fs.existsSync(donePath)) {
        console.log(`Skipping ${file}, already processed.`);
        continue;
      }

      const sql = fs.readFileSync(filePath, 'utf8');

      try {
        console.log(`Executing queries from ${file}...`);
        await client.query(sql);
        // Move to done folder
        fs.renameSync(filePath, donePath);
        console.log(`Successfully processed and moved ${file} to done/`);
      } catch (err) {
        console.error(`Error processing ${file}:`, err.message);
        // Log error to file
        fs.appendFileSync(logFile, `${new Date().toISOString()}: Error processing ${file}: ${err.message}\n`);
        // Do not move the file if there's an error
      }
    }

    await client.end();
    console.log('All batch files processed.');
  } catch (err) {
    console.error('An error occurred:', err.message);
    // Log general errors
    fs.appendFileSync(logFile, `${new Date().toISOString()}: General error: ${err.message}\n`);
  }
}

main();