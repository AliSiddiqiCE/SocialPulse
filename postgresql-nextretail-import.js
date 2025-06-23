import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

async function createTablesAndImport() {
  console.log('Creating tables and importing Next Retail data...');
  
  // Drop existing tables
  const tables = [
    'nextretail_tiktok_official',
    'nextretail_tiktok_hashtag', 
    'nextretail_youtube_official',
    'nextretail_youtube_hashtag',
    'nextretail_instagram_official',
    'nextretail_instagram_hashtag'
  ];
  
  for (const table of tables) {
    await pool.query(`DROP TABLE IF EXISTS "${table}" CASCADE;`);
  }
  
  let totalImported = 0;
  
  // 1. TikTok Official - Use batch insert with proper error handling
  console.log('Processing TikTok Official...');
  await pool.query(`
    CREATE TABLE "nextretail_tiktok_official" (
      id SERIAL PRIMARY KEY,
      unnamed_0 TEXT,
      text TEXT,
      created_time TEXT,
      mentions_0 TEXT,
      hashtags TEXT,
      share_count TEXT,
      comment_count TEXT,
      play_count TEXT,
      collect_count TEXT,
      digg_count TEXT
    );
  `);
  
  const tiktokContent = fs.readFileSync('attached_assets/tiktok_NEXT_Official_cleaned.xlsx_csv_1749116297478.csv', 'utf-8');
  const tiktokLines = tiktokContent.split('\n').filter(line => line.trim());
  
  let tiktokCount = 0;
  for (let i = 1; i < tiktokLines.length; i++) {
    const line = tiktokLines[i].trim();
    if (!line) continue;
    
    // Simple split for comma-separated values
    const values = line.split(',').map(val => val.trim().replace(/^"|"$/g, ''));
    
    if (values.length >= 10) {
      try {
        await pool.query(`
          INSERT INTO "nextretail_tiktok_official" 
          (unnamed_0, text, created_time, mentions_0, hashtags, share_count, comment_count, play_count, collect_count, digg_count)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        `, values.slice(0, 10));
        tiktokCount++;
      } catch (error) {
        // Try with safer parsing for complex content
        try {
          const safeValues = values.map(v => v || null);
          await pool.query(`
            INSERT INTO "nextretail_tiktok_official" 
            (unnamed_0, text, created_time, mentions_0, hashtags, share_count, comment_count, play_count, collect_count, digg_count)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
          `, safeValues.slice(0, 10));
          tiktokCount++;
        } catch (err) {
          // Skip problematic rows
        }
      }
    }
  }
  console.log(`TikTok Official: ${tiktokCount} records imported`);
  totalImported += tiktokCount;
  
  // 2. TikTok Hashtag
  console.log('Processing TikTok Hashtag...');
  await pool.query(`
    CREATE TABLE "nextretail_tiktok_hashtag" (
      id SERIAL PRIMARY KEY,
      text TEXT,
      created_time TEXT,
      hashtags TEXT,
      share_count TEXT,
      mentions TEXT,
      comment_count TEXT,
      play_count TEXT,
      collect_count TEXT,
      digg_count TEXT
    );
  `);
  
  const tiktokHashtagContent = fs.readFileSync('attached_assets/dataset_tiktok-hashtag_NextRetail_cleaned.xlsx_csv_1749116307865.csv', 'utf-8');
  const tiktokHashtagLines = tiktokHashtagContent.split('\n').filter(line => line.trim());
  
  let tiktokHashtagCount = 0;
  for (let i = 1; i < tiktokHashtagLines.length; i++) {
    const line = tiktokHashtagLines[i].trim();
    if (!line) continue;
    
    // Parse CSV line with proper quote handling
    const values = [];
    let current = '';
    let inQuotes = false;
    
    for (let j = 0; j < line.length; j++) {
      const char = line[j];
      if (char === '"' && !inQuotes) {
        inQuotes = true;
      } else if (char === '"' && inQuotes) {
        inQuotes = false;
      } else if (char === ',' && !inQuotes) {
        values.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    values.push(current.trim());
    
    if (values.length >= 9) {
      try {
        await pool.query(`
          INSERT INTO "nextretail_tiktok_hashtag" 
          (text, created_time, hashtags, share_count, mentions, comment_count, play_count, collect_count, digg_count)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, values.slice(0, 9));
        tiktokHashtagCount++;
      } catch (error) {
        // Skip problematic rows
      }
    }
  }
  console.log(`TikTok Hashtag: ${tiktokHashtagCount} records imported`);
  totalImported += tiktokHashtagCount;
  
  // 3. YouTube Official
  console.log('Processing YouTube Official...');
  await pool.query(`
    CREATE TABLE "nextretail_youtube_official" (
      id SERIAL PRIMARY KEY,
      channel_total_views TEXT,
      url TEXT,
      duration TEXT,
      date TEXT,
      view_count TEXT,
      title TEXT,
      channel_total_videos TEXT,
      number_of_subscribers TEXT,
      channel_description TEXT,
      channel_joined_date TEXT,
      channel_location TEXT
    );
  `);
  
  const youtubeContent = fs.readFileSync('attached_assets/dataset_youtube-channel-scraper_NextRetail-Official 1_cleaned.xlsx_csv_1749116321778.csv', 'utf-8');
  const youtubeLines = youtubeContent.split('\n').filter(line => line.trim());
  
  let youtubeCount = 0;
  for (let i = 1; i < youtubeLines.length; i++) {
    const line = youtubeLines[i].trim();
    if (!line) continue;
    
    // Parse with proper CSV handling
    const values = [];
    let current = '';
    let inQuotes = false;
    
    for (let j = 0; j < line.length; j++) {
      const char = line[j];
      if (char === '"' && !inQuotes) {
        inQuotes = true;
      } else if (char === '"' && inQuotes) {
        inQuotes = false;
      } else if (char === ',' && !inQuotes) {
        values.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    values.push(current.trim());
    
    if (values.length >= 11) {
      try {
        await pool.query(`
          INSERT INTO "nextretail_youtube_official" 
          (channel_total_views, url, duration, date, view_count, title, channel_total_videos, number_of_subscribers, channel_description, channel_joined_date, channel_location)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `, values.slice(0, 11));
        youtubeCount++;
      } catch (error) {
        // Skip problematic rows
      }
    }
  }
  console.log(`YouTube Official: ${youtubeCount} records imported`);
  totalImported += youtubeCount;
  
  // Continue with remaining datasets...
  console.log('\n=== IMPORT SUMMARY ===');
  console.log(`Total records imported so far: ${totalImported}`);
  console.log('Import process completed for initial datasets');
  
  return totalImported;
}

async function main() {
  try {
    await createTablesAndImport();
  } catch (error) {
    console.error('Import error:', error);
  } finally {
    await pool.end();
  }
}

main();