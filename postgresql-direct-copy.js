import fs from 'fs';
import pkg from 'pg';
import { execSync } from 'child_process';

const { Client } = pkg;

const client = new Client({
  connectionString: process.env.DATABASE_URL,
});

async function copyCSVToTable(csvFile, tableName, columns) {
  console.log(`Importing ${csvFile} to ${tableName}...`);
  
  try {
    // Clear existing data
    await client.query(`DELETE FROM "${tableName}"`);
    await client.query(`ALTER SEQUENCE "${tableName}_id_seq" RESTART WITH 1`);
    
    // Create absolute path for the CSV file
    const absolutePath = process.cwd() + '/' + csvFile;
    
    // Use PostgreSQL COPY command with proper CSV handling
    const copyCommand = `
      COPY "${tableName}" (${columns.map(col => `"${col}"`).join(', ')})
      FROM '${absolutePath}'
      WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ',',
        QUOTE '"',
        ESCAPE '"',
        NULL ''
      )
    `;
    
    await client.query(copyCommand);
    
    // Get final count
    const result = await client.query(`SELECT COUNT(*) as count FROM "${tableName}"`);
    const count = parseInt(result.rows[0].count);
    
    console.log(`${tableName}: ${count} rows imported from ${csvFile}`);
    return count;
    
  } catch (error) {
    console.log(`Direct COPY failed for ${tableName}: ${error.message}`);
    
    // Fallback to manual import if COPY fails
    return await manualRowImport(csvFile, tableName, columns);
  }
}

async function manualRowImport(csvFile, tableName, columns) {
  console.log(`Fallback manual import for ${tableName}...`);
  
  const content = fs.readFileSync(csvFile, 'utf8');
  const lines = content.split('\n');
  
  // Clear existing data
  await client.query(`DELETE FROM "${tableName}"`);
  await client.query(`ALTER SEQUENCE "${tableName}_id_seq" RESTART WITH 1`);
  
  let imported = 0;
  let currentLine = '';
  let inQuotes = false;
  
  for (let i = 1; i < lines.length; i++) { // Skip header
    const line = lines[i];
    if (!line.trim()) continue;
    
    currentLine += (currentLine ? '\n' : '') + line;
    
    // Check if we're still inside quotes
    const quotes = (currentLine.match(/"/g) || []).length;
    inQuotes = quotes % 2 !== 0;
    
    if (!inQuotes) {
      // Complete row, process it
      const values = parseCSVRow(currentLine, columns.length);
      
      if (values && values.length >= columns.length) {
        try {
          const placeholders = columns.map((_, i) => `$${i + 1}`).join(', ');
          const columnNames = columns.map(col => `"${col}"`).join(', ');
          
          await client.query(
            `INSERT INTO "${tableName}" (${columnNames}) VALUES (${placeholders})`,
            values.slice(0, columns.length)
          );
          imported++;
        } catch (err) {
          // Skip problematic rows
        }
      }
      currentLine = '';
    }
  }
  
  console.log(`${tableName}: ${imported} rows imported manually`);
  return imported;
}

function parseCSVRow(line, expectedColumns) {
  const values = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    
    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++; // Skip escaped quote
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      values.push(cleanValue(current));
      current = '';
    } else {
      current += char;
    }
  }
  values.push(cleanValue(current));
  
  // Ensure we have enough values
  while (values.length < expectedColumns) {
    values.push(null);
  }
  
  return values;
}

function cleanValue(value) {
  if (!value || value === 'null' || value === '') return null;
  return value.trim();
}

async function importAllData() {
  await client.connect();
  
  console.log('Starting complete import of all M&S CSV files using PostgreSQL COPY...');
  
  const results = {};
  
  // TikTok Official
  results.tiktokOfficial = await copyCSVToTable(
    'attached_assets/dataset_tiktok_M&S_official_cleaned.xlsx_csv.csv',
    'dataset_tiktok_M&S_official_cleaned.xlsx_csv',
    ['text', 'created_time', 'hashtags', 'shareCount', 'mentions', 'commentCount', 'playCount', 'diggCount', 'collectCount']
  );
  
  // TikTok Hashtag
  results.tiktokHashtag = await copyCSVToTable(
    'attached_assets/dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv.csv',
    'dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv',
    ['text', 'created_time', 'hashtags', 'shareCount', 'mentions', 'commentCount', 'playCount', 'collectCount', 'diggCount']
  );
  
  // YouTube Official
  results.youtubeOfficial = await copyCSVToTable(
    'attached_assets/dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv.csv',
    'dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv',
    ['channelTotalViews', 'url', 'duration', 'date', 'title', 'numberOfSubscribers', 'channelDescription', 'channelTotalVideos', 'channelJoinedDate']
  );
  
  // YouTube Hashtag
  results.youtubeHashtag = await copyCSVToTable(
    'attached_assets/dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv.csv',
    'dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv',
    ['text', 'hashtags', 'duration', 'date', 'url', 'commentsCount', 'title', 'numberOfSubscribers', 'viewCount', 'channelName', 'likes']
  );
  
  // Instagram Official
  results.instagramOfficial = await copyCSVToTable(
    'attached_assets/Insta_new_marksandspencer_cleaned.xlsx_csv.csv',
    'Insta_new_marksandspencer_cleaned.xlsx_csv',
    ['videoPlayCount', 'hashtags', 'url', 'locationName', 'videoViewCount', 'videoDuration', 'commentsCount', 'mentions', 'caption', 'timestamp', 'likesCount']
  );
  
  // Instagram Hashtag
  results.instagramHashtag = await copyCSVToTable(
    'attached_assets/Insta_new_mandshashtags_cleaned.xlsx_csv.csv',
    'Insta_new_mandshashtags_cleaned.xlsx_csv',
    ['hashtags', 'url', 'locationName', 'paidPartnership', 'caption', 'videoDuration', 'commentsCount', 'mentions', 'isSponsored', 'timestamp', 'likesCount']
  );
  
  console.log('\nFINAL IMPORT SUMMARY:');
  console.log('Dataset                | Imported | Expected');
  console.log('-----------------------|----------|----------');
  console.log(`TikTok Official        | ${results.tiktokOfficial.toString().padStart(8)} | 259`);
  console.log(`TikTok Hashtag         | ${results.tiktokHashtag.toString().padStart(8)} | 1017`);
  console.log(`YouTube Official       | ${results.youtubeOfficial.toString().padStart(8)} | 1000`);
  console.log(`YouTube Hashtag        | ${results.youtubeHashtag.toString().padStart(8)} | 12936`);
  console.log(`Instagram Official     | ${results.instagramOfficial.toString().padStart(8)} | 5900`);
  console.log(`Instagram Hashtag      | ${results.instagramHashtag.toString().padStart(8)} | 9727`);
  
  const total = Object.values(results).reduce((sum, count) => sum + count, 0);
  console.log(`TOTAL                  | ${total.toString().padStart(8)} | 30839`);
  
  await client.end();
  return results;
}

async function main() {
  try {
    await importAllData();
    console.log('\nComplete authentic M&S dataset import finished!');
  } catch (error) {
    console.error('Import error:', error);
    await client.end();
  }
}

main();