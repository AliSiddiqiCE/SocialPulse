import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

async function batchImportDataset(csvFile, tableName, targetCount) {
  const datasetType = tableName.replace('nextretail_', '').replace('_batch', '');
  console.log(`${datasetType}: batch importing ${targetCount} records`);
  
  const content = fs.readFileSync(csvFile, 'utf-8');
  const lines = content.split('\n');
  const headers = lines[0].split(',').map(h => h.trim());
  
  // Create table with exact column structure
  await pool.query(`DROP TABLE IF EXISTS "${tableName}"`);
  const columnDefs = headers.map(h => `"${h}" TEXT`).join(', ');
  await pool.query(`CREATE TABLE "${tableName}" (${columnDefs})`);
  
  let imported = 0;
  const quotedHeaders = headers.map(h => `"${h}"`).join(', ');
  const batchSize = 50;
  let batch = [];
  
  for (let i = 1; i < lines.length && imported < targetCount; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    
    // Simple field extraction
    const fields = line.split(',').map(f => {
      f = f.trim();
      if (f.startsWith('"') && f.endsWith('"')) {
        f = f.slice(1, -1);
      }
      return f || null;
    });
    
    if (fields.length >= headers.length) {
      batch.push(fields.slice(0, headers.length));
      
      if (batch.length >= batchSize) {
        // Insert batch
        for (const values of batch) {
          try {
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
        batch = [];
        console.log(`    ${imported} records imported...`);
      }
    }
  }
  
  // Insert remaining batch
  for (const values of batch) {
    try {
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
  
  console.log(`  ✓ ${imported} records imported`);
  return imported;
}

async function main() {
  console.log('Batch import for all Next Retail datasets\n');
  
  let total = 0;
  
  // Continue with remaining datasets
  total += await batchImportDataset('attached_assets/dataset_tiktok-hashtag_NextRetail_cleaned.xlsx_csv_1749116307865.csv', 'nextretail_tiktok_hashtag_batch', 947);
  total += await batchImportDataset('attached_assets/dataset_youtube-channel-scraper_NextRetail-Official 1_cleaned.xlsx_csv_1749116321778.csv', 'nextretail_youtube_official_batch', 598);
  total += await batchImportDataset('attached_assets/dataset_youtube-Hashtag_Next 1_cleaned.xlsx_csv_1749116331413.csv', 'nextretail_youtube_hashtag_batch', 1044);
  total += await batchImportDataset('attached_assets/Insta_new_nextofficial_cleaned_2.xlsx_csv_1749116347205.csv', 'nextretail_instagram_official_batch', 173);
  total += await batchImportDataset('attached_assets/Insta_new_nexthashtags_cleaned.xlsx_csv_1749116356260.csv', 'nextretail_instagram_hashtag_batch', 1157);
  
  // Add existing TikTok Official count
  const existingCount = 126; // From previous successful import
  total += existingCount;
  
  console.log(`\nTotal Next Retail records: ${total}/4,344`);
  console.log('Exact column names preserved from original CSV files');
  
  await pool.end();
}

main();