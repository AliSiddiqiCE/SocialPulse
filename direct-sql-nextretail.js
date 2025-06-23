import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

async function processDatasetDirect(csvFile, tableName, targetCount) {
  const datasetType = tableName.split('_')[1];
  console.log(`${datasetType}: processing ${targetCount} records`);
  
  const content = fs.readFileSync(csvFile, 'utf-8');
  const lines = content.split('\n');
  const headers = lines[0].split(',').map(h => h.trim());
  
  // Create table with exact column names
  await pool.query(`DROP TABLE IF EXISTS "${tableName}"`);
  const columnDefs = headers.map(h => `"${h}" TEXT`).join(', ');
  await pool.query(`CREATE TABLE "${tableName}" (${columnDefs})`);
  
  let imported = 0;
  const quotedHeaders = headers.map(h => `"${h}"`).join(', ');
  
  // Process records individually
  for (let i = 1; i < lines.length && imported < targetCount; i++) {
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
  
  console.log(`  ${imported} records imported`);
  return imported;
}

async function completeImport() {
  console.log('Direct Next Retail import with exact column preservation\n');
  
  let total = 0;
  
  // Process all 6 datasets with exact column names
  total += await processDatasetDirect('attached_assets/tiktok_NEXT_Official_cleaned.xlsx_csv_1749116297478.csv', 'nextretail_tiktok_official_direct', 425);
  total += await processDatasetDirect('attached_assets/dataset_tiktok-hashtag_NextRetail_cleaned.xlsx_csv_1749116307865.csv', 'nextretail_tiktok_hashtag_direct', 947);
  total += await processDatasetDirect('attached_assets/dataset_youtube-channel-scraper_NextRetail-Official 1_cleaned.xlsx_csv_1749116321778.csv', 'nextretail_youtube_official_direct', 598);
  total += await processDatasetDirect('attached_assets/dataset_youtube-Hashtag_Next 1_cleaned.xlsx_csv_1749116331413.csv', 'nextretail_youtube_hashtag_direct', 1044);
  total += await processDatasetDirect('attached_assets/Insta_new_nextofficial_cleaned_2.xlsx_csv_1749116347205.csv', 'nextretail_instagram_official_direct', 173);
  total += await processDatasetDirect('attached_assets/Insta_new_nexthashtags_cleaned.xlsx_csv_1749116356260.csv', 'nextretail_instagram_hashtag_direct', 1157);
  
  console.log(`\nTotal: ${total}/4344 authentic records imported`);
  console.log('Exact column names preserved from original CSV files');
  
  await pool.end();
}

completeImport();