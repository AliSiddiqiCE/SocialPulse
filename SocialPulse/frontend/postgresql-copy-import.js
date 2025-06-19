import fs from 'fs';
import { Transform } from 'stream';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function createCleanCSVTransform() {
  let lineBuffer = '';
  let headerProcessed = false;
  
  return new Transform({
    transform(chunk, encoding, callback) {
      const data = chunk.toString();
      const lines = (lineBuffer + data).split('\n');
      lineBuffer = lines.pop(); // Keep incomplete line
      
      for (const line of lines) {
        if (!headerProcessed) {
          headerProcessed = true;
          continue; // Skip header
        }
        
        if (line.trim()) {
          // Clean and validate CSV line
          const cleanLine = line.replace(/[\r\n]+/g, ' ').trim();
          if (cleanLine.includes(',')) {
            this.push(cleanLine + '\n');
          }
        }
      }
      callback();
    },
    
    flush(callback) {
      if (lineBuffer.trim() && lineBuffer.includes(',')) {
        const cleanLine = lineBuffer.replace(/[\r\n]+/g, ' ').trim();
        this.push(cleanLine + '\n');
      }
      callback();
    }
  });
}

async function importWithCopy(filePath, tableName, columns) {
  const datasetName = tableName.replace('nextretail_', '').replace('_copy', '');
  console.log(`${datasetName}: importing with PostgreSQL COPY`);
  
  try {
    // Get headers from CSV
    const content = fs.readFileSync(filePath, 'utf-8');
    const headers = content.split('\n')[0].split(',').map(h => h.trim());
    
    // Create table with exact column structure
    await pool.query(`DROP TABLE IF EXISTS "${tableName}"`);
    const columnDefs = headers.map(h => `"${h}" TEXT`).join(', ');
    await pool.query(`CREATE TABLE "${tableName}" (${columnDefs})`);
    
    // Use COPY command for efficient import
    const quotedHeaders = headers.map(h => `"${h}"`).join(', ');
    const client = await pool.connect();
    
    try {
      const copyQuery = `COPY "${tableName}" (${quotedHeaders}) FROM STDIN WITH CSV`;
      const stream = client.query(require('pg-copy-streams').from(copyQuery));
      
      const readStream = fs.createReadStream(filePath);
      const transformStream = createCleanCSVTransform();
      
      await new Promise((resolve, reject) => {
        readStream
          .pipe(transformStream)
          .pipe(stream)
          .on('finish', resolve)
          .on('error', reject);
      });
      
      const result = await client.query(`SELECT COUNT(*) FROM "${tableName}"`);
      console.log(`  ✓ ${result.rows[0].count} records imported`);
      return parseInt(result.rows[0].count);
      
    } finally {
      client.release();
    }
    
  } catch (error) {
    console.log(`  Error with COPY, using manual import: ${error.message}`);
    return await manualImport(filePath, tableName, columns);
  }
}

async function manualImport(filePath, tableName, columns) {
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n');
  const headers = lines[0].split(',').map(h => h.trim());
  
  let imported = 0;
  const quotedHeaders = headers.map(h => `"${h}"`).join(', ');
  
  for (let i = 1; i < Math.min(lines.length, columns + 1); i++) {
    const line = lines[i].trim();
    if (!line) continue;
    
    const fields = line.split(',').map(field => {
      field = field.trim();
      if (field.startsWith('"') && field.endsWith('"')) {
        field = field.slice(1, -1);
      }
      return field || null;
    });
    
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
  
  console.log(`  ✓ ${imported} records imported manually`);
  return imported;
}

async function main() {
  console.log('PostgreSQL COPY import for Next Retail datasets\n');
  
  let total = 0;
  
  const datasets = [
    { file: 'attached_assets/tiktok_NEXT_Official_cleaned.xlsx_csv_1749116297478.csv', table: 'nextretail_tiktok_official_copy', count: 425 },
    { file: 'attached_assets/dataset_tiktok-hashtag_NextRetail_cleaned.xlsx_csv_1749116307865.csv', table: 'nextretail_tiktok_hashtag_copy', count: 947 },
    { file: 'attached_assets/dataset_youtube-channel-scraper_NextRetail-Official 1_cleaned.xlsx_csv_1749116321778.csv', table: 'nextretail_youtube_official_copy', count: 598 },
    { file: 'attached_assets/dataset_youtube-Hashtag_Next 1_cleaned.xlsx_csv_1749116331413.csv', table: 'nextretail_youtube_hashtag_copy', count: 1044 },
    { file: 'attached_assets/Insta_new_nextofficial_cleaned_2.xlsx_csv_1749116347205.csv', table: 'nextretail_instagram_official_copy', count: 173 },
    { file: 'attached_assets/Insta_new_nexthashtags_cleaned.xlsx_csv_1749116356260.csv', table: 'nextretail_instagram_hashtag_copy', count: 1157 }
  ];
  
  for (const dataset of datasets) {
    const imported = await importWithCopy(dataset.file, dataset.table, dataset.count);
    total += imported;
  }
  
  console.log(`\nTotal: ${total}/4,344 authentic Next Retail records imported`);
  console.log('Exact column names preserved from original CSV files');
  
  await pool.end();
}

main();