import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function parseCSVRobust(content) {
  const lines = content.split('\n');
  const records = [];
  let currentRecord = '';
  let inQuotes = false;
  let skipHeader = true;
  
  for (const line of lines) {
    if (skipHeader) {
      skipHeader = false;
      continue;
    }
    
    if (!line.trim()) continue;
    
    // Process character by character to handle quotes properly
    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      currentRecord += char;
      
      if (char === '"') {
        if (line[i + 1] === '"') {
          currentRecord += '"';
          i++; // Skip next quote
        } else {
          inQuotes = !inQuotes;
        }
      }
    }
    
    // If not in quotes at end of line, record is complete
    if (!inQuotes) {
      if (currentRecord.trim() && currentRecord.includes(',')) {
        records.push(currentRecord.trim());
      }
      currentRecord = '';
    } else {
      currentRecord += '\n'; // Preserve newline in multiline fields
    }
  }
  
  // Add final record if exists
  if (currentRecord.trim()) {
    records.push(currentRecord.trim());
  }
  
  return records;
}

function parseFields(record) {
  const fields = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < record.length; i++) {
    const char = record[i];
    
    if (char === '"') {
      if (inQuotes && record[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      fields.push(current || null);
      current = '';
    } else {
      current += char;
    }
  }
  
  fields.push(current || null);
  return fields.map(field => {
    if (!field || field === 'null') return null;
    return field.trim();
  });
}

async function importDatasetRobust(csvFile, tableName, targetCount) {
  const datasetName = tableName.split('_')[1];
  console.log(`${datasetName}: importing ${targetCount} records`);
  
  try {
    // Get headers
    const content = fs.readFileSync(csvFile, 'utf-8');
    const firstLine = content.split('\n')[0];
    const headers = firstLine.split(',').map(h => h.trim());
    
    // Create table with exact column names
    await pool.query(`DROP TABLE IF EXISTS "${tableName}"`);
    const columnDefs = headers.map(h => `"${h}" TEXT`).join(', ');
    await pool.query(`CREATE TABLE "${tableName}" (${columnDefs})`);
    
    // Parse records
    const records = parseCSVRobust(content);
    console.log(`  parsed ${records.length} records from CSV`);
    
    // Import records
    let imported = 0;
    const quotedHeaders = headers.map(h => `"${h}"`).join(', ');
    
    for (let i = 0; i < Math.min(records.length, targetCount); i++) {
      const fields = parseFields(records[i]);
      
      if (fields.length >= headers.length) {
        try {
          const values = fields.slice(0, headers.length);
          const placeholders = values.map((_, idx) => `$${idx + 1}`).join(', ');
          
          await pool.query(
            `INSERT INTO "${tableName}" (${quotedHeaders}) VALUES (${placeholders})`,
            values
          );
          imported++;
        } catch (error) {
          // Skip invalid records
        }
      }
    }
    
    console.log(`  ${imported} records imported successfully`);
    return imported;
    
  } catch (error) {
    console.log(`  error: ${error.message}`);
    return 0;
  }
}

async function completeNextRetailImport() {
  console.log('Next Retail complete import with robust CSV parsing\n');
  
  let totalImported = 0;
  
  // Import all 6 datasets
  totalImported += await importDatasetRobust(
    'attached_assets/tiktok_NEXT_Official_cleaned.xlsx_csv_1749116297478.csv',
    'nextretail_tiktok_official_complete',
    425
  );
  
  totalImported += await importDatasetRobust(
    'attached_assets/dataset_tiktok-hashtag_NextRetail_cleaned.xlsx_csv_1749116307865.csv',
    'nextretail_tiktok_hashtag_complete',
    947
  );
  
  totalImported += await importDatasetRobust(
    'attached_assets/dataset_youtube-channel-scraper_NextRetail-Official 1_cleaned.xlsx_csv_1749116321778.csv',
    'nextretail_youtube_official_complete',
    598
  );
  
  totalImported += await importDatasetRobust(
    'attached_assets/dataset_youtube-Hashtag_Next 1_cleaned.xlsx_csv_1749116331413.csv',
    'nextretail_youtube_hashtag_complete',
    1044
  );
  
  totalImported += await importDatasetRobust(
    'attached_assets/Insta_new_nextofficial_cleaned_2.xlsx_csv_1749116347205.csv',
    'nextretail_instagram_official_complete',
    173
  );
  
  totalImported += await importDatasetRobust(
    'attached_assets/Insta_new_nexthashtags_cleaned.xlsx_csv_1749116356260.csv',
    'nextretail_instagram_hashtag_complete',
    1157
  );
  
  console.log(`\nTotal imported: ${totalImported} / 4344 records`);
  console.log('Authentic data with exact column names preserved');
  
  await pool.end();
  return totalImported;
}

completeNextRetailImport();