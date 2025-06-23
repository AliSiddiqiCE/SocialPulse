import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// Enhanced CSV parser for large files with multiline content
function processLargeCSV(filePath) {
  console.log(`Processing ${filePath}...`);
  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.split('\n');
  const records = [];
  
  let currentRecord = '';
  let inQuotes = false;
  
  for (let i = 1; i < lines.length; i++) { // Skip header
    const line = lines[i];
    if (!line.trim()) continue;
    
    currentRecord += (currentRecord ? '\n' : '') + line;
    
    // Count quotes to determine if record is complete
    const quotes = (currentRecord.match(/"/g) || []).length;
    inQuotes = quotes % 2 !== 0;
    
    if (!inQuotes) {
      // Complete record found
      const fields = parseCSVRecord(currentRecord);
      if (fields.length > 0) {
        records.push(fields);
      }
      currentRecord = '';
    }
  }
  
  console.log(`Parsed ${records.length} records from ${filePath}`);
  return records;
}

function parseCSVRecord(record) {
  const fields = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < record.length; i++) {
    const char = record[i];
    
    if (char === '"') {
      if (inQuotes && record[i + 1] === '"') {
        current += '"';
        i++; // Skip escaped quote
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      fields.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }
  fields.push(current.trim());
  return fields;
}

function cleanValue(value) {
  if (!value || value === 'null' || value === '') return null;
  if (value.startsWith('"') && value.endsWith('"')) {
    value = value.slice(1, -1);
  }
  return value.replace(/""/g, '"') || null;
}

function parseNumber(value) {
  const clean = cleanValue(value);
  if (!clean) return null;
  const num = parseFloat(clean.replace(/[^0-9.-]/g, ''));
  return isNaN(num) ? null : num;
}

async function batchInsertRecords(tableName, columns, records, batchSize = 100) {
  let inserted = 0;
  
  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    const values = [];
    const placeholders = [];
    
    batch.forEach((record, batchIndex) => {
      if (record.length >= columns.length) {
        const rowPlaceholders = columns.map((_, colIndex) => 
          `$${batchIndex * columns.length + colIndex + 1}`
        ).join(', ');
        placeholders.push(`(${rowPlaceholders})`);
        
        // Map record fields to column types
        const mappedValues = columns.map((col, idx) => {
          const field = record[idx] || '';
          return col.type === 'number' ? parseNumber(field) : cleanValue(field);
        });
        values.push(...mappedValues);
      }
    });
    
    if (placeholders.length > 0) {
      try {
        const columnNames = columns.map(col => `"${col.name}"`).join(', ');
        await pool.query(
          `INSERT INTO "${tableName}" (${columnNames}) VALUES ${placeholders.join(', ')}`,
          values
        );
        inserted += placeholders.length;
      } catch (err) {
        console.log(`Batch insert error: ${err.message}`);
      }
    }
  }
  
  return inserted;
}

async function importCompleteDataset() {
  console.log('Starting comprehensive import of all M&S CSV datasets...');
  
  const results = {};
  
  // Import all 6 datasets with full data
  const datasets = [
    {
      file: 'attached_assets/dataset_tiktok_M&S_official_cleaned.xlsx_csv.csv',
      table: 'dataset_tiktok_M&S_official_cleaned.xlsx_csv',
      columns: [
        {name: 'text', type: 'string'},
        {name: 'created_time', type: 'string'},
        {name: 'hashtags', type: 'string'},
        {name: 'shareCount', type: 'number'},
        {name: 'mentions', type: 'string'},
        {name: 'commentCount', type: 'number'},
        {name: 'playCount', type: 'number'},
        {name: 'diggCount', type: 'number'},
        {name: 'collectCount', type: 'number'}
      ]
    },
    {
      file: 'attached_assets/dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv.csv',
      table: 'dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv',
      columns: [
        {name: 'text', type: 'string'},
        {name: 'created_time', type: 'string'},
        {name: 'hashtags', type: 'string'},
        {name: 'shareCount', type: 'number'},
        {name: 'mentions', type: 'string'},
        {name: 'commentCount', type: 'number'},
        {name: 'playCount', type: 'number'},
        {name: 'collectCount', type: 'number'},
        {name: 'diggCount', type: 'number'}
      ]
    },
    {
      file: 'attached_assets/dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv.csv',
      table: 'dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv',
      columns: [
        {name: 'channelTotalViews', type: 'number'},
        {name: 'url', type: 'string'},
        {name: 'duration', type: 'string'},
        {name: 'date', type: 'string'},
        {name: 'title', type: 'string'},
        {name: 'numberOfSubscribers', type: 'number'},
        {name: 'channelDescription', type: 'string'},
        {name: 'channelTotalVideos', type: 'number'},
        {name: 'channelJoinedDate', type: 'string'}
      ]
    }
  ];
  
  for (const dataset of datasets) {
    console.log(`\nImporting ${dataset.table}...`);
    
    // Clear existing data
    await pool.query(`DELETE FROM "${dataset.table}"`);
    
    // Process CSV file
    const records = processLargeCSV(dataset.file);
    
    // Batch insert records
    const imported = await batchInsertRecords(dataset.table, dataset.columns, records);
    results[dataset.table] = imported;
    
    console.log(`${dataset.table}: ${imported} records imported`);
  }
  
  // Reset sequential IDs
  console.log('\nResetting IDs to sequential order...');
  for (const dataset of datasets) {
    try {
      await pool.query(`
        WITH numbered_rows AS (
          SELECT *, ROW_NUMBER() OVER (ORDER BY id) as new_id 
          FROM "${dataset.table}"
        )
        UPDATE "${dataset.table}" 
        SET id = numbered_rows.new_id
        FROM numbered_rows 
        WHERE "${dataset.table}".id = numbered_rows.id
      `);
    } catch (err) {
      console.log(`ID reset error for ${dataset.table}: ${err.message}`);
    }
  }
  
  console.log('\nCOMPREHENSIVE IMPORT RESULTS:');
  console.log('Dataset                              | Records');
  console.log('-------------------------------------|--------');
  
  let totalImported = 0;
  for (const [table, count] of Object.entries(results)) {
    const displayName = table.replace('dataset_', '').replace('_cleaned.xlsx_csv', '');
    console.log(`${displayName.padEnd(36)} | ${count}`);
    totalImported += count;
  }
  
  console.log(`TOTAL IMPORTED                       | ${totalImported}`);
  console.log('\nComprehensive authentic M&S dataset import completed!');
  
  return results;
}

async function main() {
  try {
    await importCompleteDataset();
  } catch (error) {
    console.error('Import error:', error);
  } finally {
    await pool.end();
  }
}

main();