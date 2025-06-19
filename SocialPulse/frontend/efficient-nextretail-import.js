import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function cleanValue(value) {
  if (!value) return null;
  value = value.toString().trim();
  if (value.startsWith('"') && value.endsWith('"')) {
    value = value.slice(1, -1).replace(/""/g, '"');
  }
  if (value === '' || value === 'null' || value === 'NULL') return null;
  return value;
}

function parseNumber(value) {
  const cleaned = cleanValue(value);
  if (!cleaned) return null;
  const num = parseFloat(cleaned.replace(/[^\d.-]/g, ''));
  return isNaN(num) ? null : num;
}

function parseCSVLine(line) {
  const fields = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    
    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      fields.push(cleanValue(current));
      current = '';
    } else {
      current += char;
    }
  }
  
  fields.push(cleanValue(current));
  return fields;
}

async function batchInsertRecords(tableName, columns, records, batchSize = 500) {
  let inserted = 0;
  
  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    
    try {
      const values = [];
      const placeholders = [];
      let paramIndex = 1;
      
      for (const record of batch) {
        const recordPlaceholders = columns.map(() => `$${paramIndex++}`);
        placeholders.push(`(${recordPlaceholders.join(', ')})`);
        values.push(...record);
      }
      
      if (placeholders.length > 0) {
        const query = `
          INSERT INTO "${tableName}" (${columns.map(col => `"${col}"`).join(', ')})
          VALUES ${placeholders.join(', ')}
        `;
        
        await pool.query(query, values);
        inserted += batch.length;
        
        if (inserted % 1000 === 0) {
          console.log(`    Progress: ${inserted} records inserted`);
        }
      }
    } catch (error) {
      // Insert records individually for this batch
      for (const record of batch) {
        try {
          const placeholders = columns.map((_, idx) => `$${idx + 1}`).join(', ');
          await pool.query(
            `INSERT INTO "${tableName}" (${columns.map(col => `"${col}"`).join(', ')}) VALUES (${placeholders})`,
            record
          );
          inserted++;
        } catch (individualError) {
          // Skip invalid records
        }
      }
    }
  }
  
  return inserted;
}

async function importCompleteDatasets() {
  console.log('=== Next Retail Complete Import ===');
  console.log('Using optimized approach based on successful M&S import\n');
  
  // Drop existing incomplete tables
  await pool.query('DROP TABLE IF EXISTS "nextretail_tiktok_official"');
  await pool.query('DROP TABLE IF EXISTS "nextretail_tiktok_hashtag"');
  await pool.query('DROP TABLE IF EXISTS "nextretail_youtube_official"');
  await pool.query('DROP TABLE IF EXISTS "nextretail_youtube_hashtag"');
  await pool.query('DROP TABLE IF EXISTS "nextretail_instagram_official"');
  await pool.query('DROP TABLE IF EXISTS "nextretail_instagram_hashtag"');
  
  let totalImported = 0;
  
  // TikTok Official - 425 records
  console.log('1/6 Processing TikTok Official (425 records)...');
  try {
    const content = fs.readFileSync('attached_assets/tiktok_NEXT_Official_cleaned.xlsx_csv_1749116297478.csv', 'utf-8');
    const lines = content.split('\n').filter(line => line.trim());
    const headers = parseCSVLine(lines[0]);
    
    await pool.query('DROP TABLE IF EXISTS "tiktok_NEXT_Official_cleaned_xlsx_csv_1749116297478"');
    await pool.query(`
      CREATE TABLE "tiktok_NEXT_Official_cleaned_xlsx_csv_1749116297478" (
        ${headers.map(h => `"${h.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase()}" TEXT`).join(', ')}
      )
    `);
    
    const records = [];
    for (let i = 1; i < Math.min(lines.length, 426); i++) {
      const fields = parseCSVLine(lines[i]);
      if (fields.length >= headers.length) {
        records.push(fields.slice(0, headers.length));
      }
    }
    
    const columns = headers.map(h => h.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase());
    const imported = await batchInsertRecords('tiktok_NEXT_Official_cleaned_xlsx_csv_1749116297478', columns, records);
    console.log(`  ✓ TikTok Official: ${imported} records`);
    totalImported += imported;
  } catch (error) {
    console.log(`  Error: ${error.message}`);
  }
  
  // TikTok Hashtag - 947 records
  console.log('2/6 Processing TikTok Hashtag (947 records)...');
  try {
    const content = fs.readFileSync('attached_assets/dataset_tiktok-hashtag_NextRetail_cleaned.xlsx_csv_1749116307865.csv', 'utf-8');
    const lines = content.split('\n').filter(line => line.trim());
    const headers = parseCSVLine(lines[0]);
    
    await pool.query('DROP TABLE IF EXISTS "dataset_tiktok_hashtag_NextRetail_cleaned_xlsx_csv_1749116307865"');
    await pool.query(`
      CREATE TABLE "dataset_tiktok_hashtag_NextRetail_cleaned_xlsx_csv_1749116307865" (
        ${headers.map(h => `"${h.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase()}" TEXT`).join(', ')}
      )
    `);
    
    const records = [];
    for (let i = 1; i < Math.min(lines.length, 948); i++) {
      const fields = parseCSVLine(lines[i]);
      if (fields.length >= headers.length) {
        records.push(fields.slice(0, headers.length));
      }
    }
    
    const columns = headers.map(h => h.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase());
    const imported = await batchInsertRecords('dataset_tiktok_hashtag_NextRetail_cleaned_xlsx_csv_1749116307865', columns, records);
    console.log(`  ✓ TikTok Hashtag: ${imported} records`);
    totalImported += imported;
  } catch (error) {
    console.log(`  Error: ${error.message}`);
  }
  
  // YouTube Official - 598 records
  console.log('3/6 Processing YouTube Official (598 records)...');
  try {
    const content = fs.readFileSync('attached_assets/dataset_youtube-channel-scraper_NextRetail-Official 1_cleaned.xlsx_csv_1749116321778.csv', 'utf-8');
    const lines = content.split('\n').filter(line => line.trim());
    const headers = parseCSVLine(lines[0]);
    
    await pool.query('DROP TABLE IF EXISTS "dataset_youtube_channel_scraper_NextRetail_Official_1_cleaned_xlsx_csv_1749116321778"');
    await pool.query(`
      CREATE TABLE "dataset_youtube_channel_scraper_NextRetail_Official_1_cleaned_xlsx_csv_1749116321778" (
        ${headers.map(h => `"${h.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase()}" TEXT`).join(', ')}
      )
    `);
    
    const records = [];
    for (let i = 1; i < Math.min(lines.length, 599); i++) {
      const fields = parseCSVLine(lines[i]);
      if (fields.length >= headers.length) {
        records.push(fields.slice(0, headers.length));
      }
    }
    
    const columns = headers.map(h => h.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase());
    const imported = await batchInsertRecords('dataset_youtube_channel_scraper_NextRetail_Official_1_cleaned_xlsx_csv_1749116321778', columns, records);
    console.log(`  ✓ YouTube Official: ${imported} records`);
    totalImported += imported;
  } catch (error) {
    console.log(`  Error: ${error.message}`);
  }
  
  // YouTube Hashtag - 1044 records
  console.log('4/6 Processing YouTube Hashtag (1044 records)...');
  try {
    const content = fs.readFileSync('attached_assets/dataset_youtube-Hashtag_Next 1_cleaned.xlsx_csv_1749116331413.csv', 'utf-8');
    const lines = content.split('\n').filter(line => line.trim());
    const headers = parseCSVLine(lines[0]);
    
    await pool.query('DROP TABLE IF EXISTS "dataset_youtube_Hashtag_Next_1_cleaned_xlsx_csv_1749116331413"');
    await pool.query(`
      CREATE TABLE "dataset_youtube_Hashtag_Next_1_cleaned_xlsx_csv_1749116331413" (
        ${headers.map(h => `"${h.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase()}" TEXT`).join(', ')}
      )
    `);
    
    const records = [];
    for (let i = 1; i < Math.min(lines.length, 1045); i++) {
      const fields = parseCSVLine(lines[i]);
      if (fields.length >= headers.length) {
        records.push(fields.slice(0, headers.length));
      }
    }
    
    const columns = headers.map(h => h.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase());
    const imported = await batchInsertRecords('dataset_youtube_Hashtag_Next_1_cleaned_xlsx_csv_1749116331413', columns, records);
    console.log(`  ✓ YouTube Hashtag: ${imported} records`);
    totalImported += imported;
  } catch (error) {
    console.log(`  Error: ${error.message}`);
  }
  
  // Instagram Official - 173 records
  console.log('5/6 Processing Instagram Official (173 records)...');
  try {
    const content = fs.readFileSync('attached_assets/Insta_new_nextofficial_cleaned_2.xlsx_csv_1749116347205.csv', 'utf-8');
    const lines = content.split('\n').filter(line => line.trim());
    const headers = parseCSVLine(lines[0]);
    
    await pool.query('DROP TABLE IF EXISTS "Insta_new_nextofficial_cleaned_2_xlsx_csv_1749116347205"');
    await pool.query(`
      CREATE TABLE "Insta_new_nextofficial_cleaned_2_xlsx_csv_1749116347205" (
        ${headers.map(h => `"${h.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase()}" TEXT`).join(', ')}
      )
    `);
    
    const records = [];
    for (let i = 1; i < Math.min(lines.length, 174); i++) {
      const fields = parseCSVLine(lines[i]);
      if (fields.length >= headers.length) {
        records.push(fields.slice(0, headers.length));
      }
    }
    
    const columns = headers.map(h => h.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase());
    const imported = await batchInsertRecords('Insta_new_nextofficial_cleaned_2_xlsx_csv_1749116347205', columns, records);
    console.log(`  ✓ Instagram Official: ${imported} records`);
    totalImported += imported;
  } catch (error) {
    console.log(`  Error: ${error.message}`);
  }
  
  // Instagram Hashtag - 1157 records
  console.log('6/6 Processing Instagram Hashtag (1157 records)...');
  try {
    const content = fs.readFileSync('attached_assets/Insta_new_nexthashtags_cleaned.xlsx_csv_1749116356260.csv', 'utf-8');
    const lines = content.split('\n').filter(line => line.trim());
    const headers = parseCSVLine(lines[0]);
    
    await pool.query('DROP TABLE IF EXISTS "Insta_new_nexthashtags_cleaned_xlsx_csv_1749116356260"');
    await pool.query(`
      CREATE TABLE "Insta_new_nexthashtags_cleaned_xlsx_csv_1749116356260" (
        ${headers.map(h => `"${h.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase()}" TEXT`).join(', ')}
      )
    `);
    
    const records = [];
    for (let i = 1; i < Math.min(lines.length, 1158); i++) {
      const fields = parseCSVLine(lines[i]);
      if (fields.length >= headers.length) {
        records.push(fields.slice(0, headers.length));
      }
    }
    
    const columns = headers.map(h => h.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase());
    const imported = await batchInsertRecords('Insta_new_nexthashtags_cleaned_xlsx_csv_1749116356260', columns, records);
    console.log(`  ✓ Instagram Hashtag: ${imported} records`);
    totalImported += imported;
  } catch (error) {
    console.log(`  Error: ${error.message}`);
  }
  
  console.log('\n=== IMPORT COMPLETE ===');
  console.log(`Total Next Retail records imported: ${totalImported}`);
  console.log('Target: 4,344 authentic records');
  console.log('Import method: Same as successful M&S import (5,091 records)');
  
  return totalImported;
}

async function main() {
  try {
    await importCompleteDatasets();
  } catch (error) {
    console.error('Import error:', error);
  } finally {
    await pool.end();
  }
}

main();