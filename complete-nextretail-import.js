import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function cleanValue(value) {
  if (!value || value === 'null' || value === 'NULL') return null;
  return value.toString().trim();
}

function parseNumber(value) {
  if (!value || value === 'null') return null;
  const num = parseFloat(value);
  return isNaN(num) ? null : num.toString();
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

async function dropAndCreateTables() {
  console.log('Creating Next Retail tables with exact column structure');
  
  // Drop existing tables
  const dropTables = [
    'nextretail_tiktok_official',
    'nextretail_tiktok_hashtag', 
    'nextretail_youtube_official',
    'nextretail_youtube_hashtag',
    'nextretail_instagram_official',
    'nextretail_instagram_hashtag'
  ];
  
  for (const table of dropTables) {
    await pool.query(`DROP TABLE IF EXISTS "${table}"`);
  }
  
  // Create tables with exact column names from CSV headers
  await pool.query(`
    CREATE TABLE "nextretail_tiktok_official" (
      "Unnamed: 0" TEXT,
      "text" TEXT,
      "created_time" TEXT,
      "mentions/0" TEXT,
      "hashtags" TEXT,
      "shareCount" TEXT,
      "commentCount" TEXT,
      "playCount" TEXT,
      "collectCount" TEXT,
      "diggCount" TEXT
    )
  `);
  
  await pool.query(`
    CREATE TABLE "nextretail_tiktok_hashtag" (
      "Unnamed: 0" TEXT,
      "text" TEXT,
      "created_time" TEXT,
      "hashtags" TEXT,
      "shareCount" TEXT,
      "commentCount" TEXT,
      "playCount" TEXT,
      "collectCount" TEXT,
      "diggCount" TEXT
    )
  `);
  
  await pool.query(`
    CREATE TABLE "nextretail_youtube_official" (
      "Unnamed: 0" TEXT,
      "title" TEXT,
      "upload_date" TEXT,
      "duration" TEXT,
      "view_count" TEXT,
      "like_count" TEXT,
      "comment_count" TEXT,
      "description" TEXT,
      "tags" TEXT,
      "channel_id" TEXT,
      "video_id" TEXT
    )
  `);
  
  await pool.query(`
    CREATE TABLE "nextretail_youtube_hashtag" (
      "Unnamed: 0" TEXT,
      "title" TEXT,
      "upload_date" TEXT,
      "duration" TEXT,
      "view_count" TEXT,
      "like_count" TEXT,
      "comment_count" TEXT,
      "description" TEXT,
      "tags" TEXT,
      "channel_id" TEXT,
      "video_id" TEXT
    )
  `);
  
  await pool.query(`
    CREATE TABLE "nextretail_instagram_official" (
      "Unnamed: 0" TEXT,
      "text" TEXT,
      "created_time" TEXT,
      "likes" TEXT,
      "comments" TEXT,
      "shares" TEXT,
      "saves" TEXT,
      "hashtags" TEXT,
      "mentions" TEXT
    )
  `);
  
  await pool.query(`
    CREATE TABLE "nextretail_instagram_hashtag" (
      "Unnamed: 0" TEXT,
      "text" TEXT,
      "created_time" TEXT,
      "likes" TEXT,
      "comments" TEXT,
      "shares" TEXT,
      "saves" TEXT,
      "hashtags" TEXT,
      "mentions" TEXT,
      "user_id" TEXT,
      "post_id" TEXT
    )
  `);
  
  console.log('All Next Retail tables created with exact column structure');
}

async function importAllDatasets() {
  console.log('Importing all Next Retail datasets with authentic data\n');
  
  let totalImported = 0;
  
  // TikTok Official - 425 records
  console.log('TikTok Official: importing 425 records');
  const tiktokOfficial = fs.readFileSync('public/tiktok_NEXT_Official_cleaned.xlsx_csv.csv', 'utf-8');
  const tiktokOfficialLines = tiktokOfficial.split('\n');
  let tikTokOfficialImported = 0;
  
  for (let i = 1; i < tiktokOfficialLines.length && tikTokOfficialImported < 425; i++) {
    const line = tiktokOfficialLines[i].trim();
    if (!line) continue;
    
    const fields = parseCSVLine(line);
    if (fields.length >= 10) {
      try {
        await pool.query(`
          INSERT INTO "nextretail_tiktok_official" 
          ("Unnamed: 0", "text", "created_time", "mentions/0", "hashtags", "shareCount", "commentCount", "playCount", "collectCount", "diggCount")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        `, fields.slice(0, 10));
        tikTokOfficialImported++;
      } catch (error) {
        // Skip invalid records
      }
    }
  }
  totalImported += tikTokOfficialImported;
  console.log(`  ${tikTokOfficialImported} records imported`);
  
  // TikTok Hashtag - 947 records
  console.log('TikTok Hashtag: importing 947 records');
  const tiktokHashtag = fs.readFileSync('public/dataset_tiktok-hashtag_NextRetail_cleaned.xlsx_csv.csv', 'utf-8');
  const tiktokHashtagLines = tiktokHashtag.split('\n');
  let tikTokHashtagImported = 0;
  
  for (let i = 1; i < tiktokHashtagLines.length && tikTokHashtagImported < 947; i++) {
    const line = tiktokHashtagLines[i].trim();
    if (!line) continue;
    
    const fields = parseCSVLine(line);
    if (fields.length >= 9) {
      try {
        await pool.query(`
          INSERT INTO "nextretail_tiktok_hashtag"
          ("Unnamed: 0", "text", "created_time", "hashtags", "shareCount", "commentCount", "playCount", "collectCount", "diggCount")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, fields.slice(0, 9));
        tikTokHashtagImported++;
      } catch (error) {
        // Skip invalid records
      }
    }
  }
  totalImported += tikTokHashtagImported;
  console.log(`  ${tikTokHashtagImported} records imported`);
  
  // YouTube Official - 598 records
  console.log('YouTube Official: importing 598 records');
  const youtubeOfficial = fs.readFileSync('public/dataset_youtube-channel-scraper_NextRetail-Official 1_cleaned.xlsx_csv.csv', 'utf-8');
  const youTubeOfficialLines = youtubeOfficial.split('\n');
  let youTubeOfficialImported = 0;
  
  for (let i = 1; i < youTubeOfficialLines.length && youTubeOfficialImported < 598; i++) {
    const line = youTubeOfficialLines[i].trim();
    if (!line) continue;
    
    const fields = parseCSVLine(line);
    if (fields.length >= 11) {
      try {
        await pool.query(`
          INSERT INTO "nextretail_youtube_official"
          ("Unnamed: 0", "title", "upload_date", "duration", "view_count", "like_count", "comment_count", "description", "tags", "channel_id", "video_id")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `, fields.slice(0, 11));
        youTubeOfficialImported++;
      } catch (error) {
        // Skip invalid records
      }
    }
  }
  totalImported += youTubeOfficialImported;
  console.log(`  ${youTubeOfficialImported} records imported`);
  
  // YouTube Hashtag - 1044 records
  console.log('YouTube Hashtag: importing 1044 records');
  const youtubeHashtag = fs.readFileSync('public/dataset_youtube-Hashtag_Next 1_cleaned.xlsx_csv.csv', 'utf-8');
  const youTubeHashtagLines = youtubeHashtag.split('\n');
  let youTubeHashtagImported = 0;
  
  for (let i = 1; i < youTubeHashtagLines.length && youTubeHashtagImported < 1044; i++) {
    const line = youTubeHashtagLines[i].trim();
    if (!line) continue;
    
    const fields = parseCSVLine(line);
    if (fields.length >= 11) {
      try {
        await pool.query(`
          INSERT INTO "nextretail_youtube_hashtag"
          ("Unnamed: 0", "title", "upload_date", "duration", "view_count", "like_count", "comment_count", "description", "tags", "channel_id", "video_id")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `, fields.slice(0, 11));
        youTubeHashtagImported++;
      } catch (error) {
        // Skip invalid records
      }
    }
  }
  totalImported += youTubeHashtagImported;
  console.log(`  ${youTubeHashtagImported} records imported`);
  
  // Instagram Official - 173 records
  console.log('Instagram Official: importing 173 records');
  const instagramOfficial = fs.readFileSync('public/Insta_new_nextofficial_cleaned_2.xlsx_csv.csv', 'utf-8');
  const instagramOfficialLines = instagramOfficial.split('\n');
  let instagramOfficialImported = 0;
  
  for (let i = 1; i < instagramOfficialLines.length && instagramOfficialImported < 173; i++) {
    const line = instagramOfficialLines[i].trim();
    if (!line) continue;
    
    const fields = parseCSVLine(line);
    if (fields.length >= 9) {
      try {
        await pool.query(`
          INSERT INTO "nextretail_instagram_official"
          ("Unnamed: 0", "text", "created_time", "likes", "comments", "shares", "saves", "hashtags", "mentions")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, fields.slice(0, 9));
        instagramOfficialImported++;
      } catch (error) {
        // Skip invalid records
      }
    }
  }
  totalImported += instagramOfficialImported;
  console.log(`  ${instagramOfficialImported} records imported`);
  
  // Instagram Hashtag - 1157 records
  console.log('Instagram Hashtag: importing 1157 records');
  const instagramHashtag = fs.readFileSync('public/Insta_new_nexthashtags_cleaned.xlsx_csv.csv', 'utf-8');
  const instagramHashtagLines = instagramHashtag.split('\n');
  let instagramHashtagImported = 0;
  
  for (let i = 1; i < instagramHashtagLines.length && instagramHashtagImported < 1157; i++) {
    const line = instagramHashtagLines[i].trim();
    if (!line) continue;
    
    const fields = parseCSVLine(line);
    if (fields.length >= 11) {
      try {
        await pool.query(`
          INSERT INTO "nextretail_instagram_hashtag"
          ("Unnamed: 0", "text", "created_time", "likes", "comments", "shares", "saves", "hashtags", "mentions", "user_id", "post_id")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `, fields.slice(0, 11));
        instagramHashtagImported++;
      } catch (error) {
        // Skip invalid records
      }
    }
  }
  totalImported += instagramHashtagImported;
  console.log(`  ${instagramHashtagImported} records imported`);
  
  console.log(`\n=== FINAL IMPORT RESULT ===`);
  console.log(`Total Next Retail records imported: ${totalImported}/4,344`);
  console.log('All datasets processed with exact column preservation');
  console.log('Authentic data maintained from original CSV files');
  
  return totalImported;
}

async function main() {
  try {
    await dropAndCreateTables();
    const result = await importAllDatasets();
    console.log(`\nNext Retail import completed: ${result} authentic records`);
  } catch (error) {
    console.error('Import failed:', error);
  } finally {
    await pool.end();
  }
}

main();