const fs = require("fs");
const path = require("path");

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

function createBatchedSQLFiles(data, batchSize = 100, outDir = "batches") {
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
  ];

  // ensure output directory exists
  fs.mkdirSync(outDir, { recursive: true });

  const chunks = chunkArray(data, batchSize);
  const writtenFiles = [];

  chunks.forEach((chunk, idx) => {
    const values = chunk
      .map((d) => {
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
          ][i]))
          .join(", ");

        return `(${row})`;
      })
      .join(",\n");

    const sql = `INSERT INTO ${table} (${columns.join(", ")}) VALUES\n${values};`;

    const filename = path.join(outDir, `batch_${idx + 1}.sql`);
    fs.writeFileSync(filename, sql, "utf8");
    writtenFiles.push({ file: filename, rows: chunk.length });
    console.log(`Wrote ${filename} (${chunk.length} rows)`);
  });

  return writtenFiles;
}

const permits = require('./latest_cleaned.json');

console.log(permits.length + " permits to be loaded.");
const result = createBatchedSQLFiles(permits, 50, path.join(__dirname, "batches"));
console.log(`Created ${result.length} batch file(s).`);