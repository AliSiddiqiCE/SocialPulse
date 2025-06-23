import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function cleanValue(value) {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value === 'string') {
    value = value.trim();
    if (value === '' || value === 'null' || value === 'NULL' || value === 'undefined') return null;
  }
  return value;
}

function parseNumber(value) {
  if (value === null || value === undefined || value === '') return null;
  const cleaned = String(value).replace(/[^\d.-]/g, '');
  const num = parseFloat(cleaned);
  return isNaN(num) ? null : num;
}

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  let i = 0;

  while (i < line.length) {
    const char = line[i];
    
    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i += 2;
        continue;
      }
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      result.push(current.trim());
      current = '';
    } else {
      current += char;
    }
    i++;
  }
  
  result.push(current.trim());
  return result.map(field => {
    if (field.startsWith('"') && field.endsWith('"')) {
      return field.slice(1, -1).replace(/""/g, '"');
    }
    return field;
  });
}

async function dropExistingTables() {
  console.log('Dropping existing Next Retail tables...');
  
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
  
  console.log('✓ Existing tables dropped');
}

async function importCompleteDatasets() {
  await dropExistingTables();
  
  // 1. TikTok Official - Expected: 425 rows
  console.log('Importing TikTok Official (Expected: 425 rows)...');
  const tiktokOfficial = fs.readFileSync('public/tiktok_NEXT_Official_cleaned.xlsx_csv.csv', 'utf-8');
  const tiktokOfficialLines = tiktokOfficial.split('\n').filter(line => line.trim());
  
  await pool.query(`
    CREATE TABLE "nextretail_tiktok_official" (
      id SERIAL PRIMARY KEY,
      unnamed_0 BIGINT,
      text TEXT,
      created_time TEXT,
      mentions_0 TEXT,
      hashtags TEXT,
      share_count BIGINT,
      comment_count BIGINT,
      play_count BIGINT,
      collect_count BIGINT,
      digg_count BIGINT
    );
  `);
  
  let tiktokOfficialImported = 0;
  for (let i = 1; i < tiktokOfficialLines.length; i++) {
    const line = tiktokOfficialLines[i].trim();
    if (!line) continue;
    
    const fields = parseCSVLine(line);
    if (fields.length >= 10) {
      try {
        await pool.query(`
          INSERT INTO "nextretail_tiktok_official" 
          (unnamed_0, text, created_time, mentions_0, hashtags, share_count, comment_count, play_count, collect_count, digg_count)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        `, [
          parseNumber(fields[0]),
          cleanValue(fields[1]),
          cleanValue(fields[2]),
          cleanValue(fields[3]),
          cleanValue(fields[4]),
          parseNumber(fields[5]),
          parseNumber(fields[6]),
          parseNumber(fields[7]),
          parseNumber(fields[8]),
          parseNumber(fields[9])
        ]);
        tiktokOfficialImported++;
      } catch (error) {
        console.log(`Skipping row ${i}: ${error.message}`);
      }
    }
  }
  console.log(`✓ TikTok Official: ${tiktokOfficialImported} records (CSV had ${tiktokOfficialLines.length - 1} lines)`);
  
  // 2. TikTok Hashtag - Expected: 947 rows
  console.log('Importing TikTok Hashtag (Expected: 947 rows)...');
  const tiktokHashtag = fs.readFileSync('public/dataset_tiktok-hashtag_NextRetail_cleaned.xlsx_csv.csv', 'utf-8');
  const tiktokHashtagLines = tiktokHashtag.split('\n').filter(line => line.trim());
  
  await pool.query(`
    CREATE TABLE "nextretail_tiktok_hashtag" (
      id SERIAL PRIMARY KEY,
      text TEXT,
      created_time TEXT,
      hashtags TEXT,
      share_count BIGINT,
      mentions TEXT,
      comment_count BIGINT,
      play_count BIGINT,
      collect_count BIGINT,
      digg_count BIGINT
    );
  `);
  
  let tiktokHashtagImported = 0;
  for (let i = 1; i < tiktokHashtagLines.length; i++) {
    const line = tiktokHashtagLines[i].trim();
    if (!line) continue;
    
    const fields = parseCSVLine(line);
    if (fields.length >= 9) {
      try {
        await pool.query(`
          INSERT INTO "nextretail_tiktok_hashtag" 
          (text, created_time, hashtags, share_count, mentions, comment_count, play_count, collect_count, digg_count)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, [
          cleanValue(fields[0]),
          cleanValue(fields[1]),
          cleanValue(fields[2]),
          parseNumber(fields[3]),
          cleanValue(fields[4]),
          parseNumber(fields[5]),
          parseNumber(fields[6]),
          parseNumber(fields[7]),
          parseNumber(fields[8])
        ]);
        tiktokHashtagImported++;
      } catch (error) {
        console.log(`Skipping row ${i}: ${error.message}`);
      }
    }
  }
  console.log(`✓ TikTok Hashtag: ${tiktokHashtagImported} records (CSV had ${tiktokHashtagLines.length - 1} lines)`);
  
  // 3. YouTube Official - Expected: 598 rows
  console.log('Importing YouTube Official (Expected: 598 rows)...');
  const youtubeOfficial = fs.readFileSync('public/dataset_youtube-channel-scraper_NextRetail-Official 1_cleaned.xlsx_csv.csv', 'utf-8');
  const youtubeOfficialLines = youtubeOfficial.split('\n').filter(line => line.trim());
  
  await pool.query(`
    CREATE TABLE "nextretail_youtube_official" (
      id SERIAL PRIMARY KEY,
      channel_total_views BIGINT,
      url TEXT,
      duration TEXT,
      date TEXT,
      view_count BIGINT,
      title TEXT,
      channel_total_videos BIGINT,
      number_of_subscribers BIGINT,
      channel_description TEXT,
      channel_joined_date TEXT,
      channel_location TEXT
    );
  `);
  
  let youtubeOfficialImported = 0;
  for (let i = 1; i < youtubeOfficialLines.length; i++) {
    const line = youtubeOfficialLines[i].trim();
    if (!line) continue;
    
    const fields = parseCSVLine(line);
    if (fields.length >= 11) {
      try {
        await pool.query(`
          INSERT INTO "nextretail_youtube_official" 
          (channel_total_views, url, duration, date, view_count, title, channel_total_videos, number_of_subscribers, channel_description, channel_joined_date, channel_location)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `, [
          parseNumber(fields[0]),
          cleanValue(fields[1]),
          cleanValue(fields[2]),
          cleanValue(fields[3]),
          parseNumber(fields[4]),
          cleanValue(fields[5]),
          parseNumber(fields[6]),
          parseNumber(fields[7]),
          cleanValue(fields[8]),
          cleanValue(fields[9]),
          cleanValue(fields[10])
        ]);
        youtubeOfficialImported++;
      } catch (error) {
        console.log(`Skipping row ${i}: ${error.message}`);
      }
    }
  }
  console.log(`✓ YouTube Official: ${youtubeOfficialImported} records (CSV had ${youtubeOfficialLines.length - 1} lines)`);
  
  // 4. YouTube Hashtag - Expected: 1044 rows
  console.log('Importing YouTube Hashtag (Expected: 1044 rows)...');
  const youtubeHashtag = fs.readFileSync('public/dataset_youtube-Hashtag_Next 1_cleaned.xlsx_csv.csv', 'utf-8');
  const youtubeHashtagLines = youtubeHashtag.split('\n').filter(line => line.trim());
  
  await pool.query(`
    CREATE TABLE "nextretail_youtube_hashtag" (
      id SERIAL PRIMARY KEY,
      text TEXT,
      hashtags TEXT,
      duration TEXT,
      date TEXT,
      url TEXT,
      comments_count DECIMAL,
      view_count DECIMAL,
      title TEXT,
      number_of_subscribers DECIMAL,
      channel_name TEXT,
      likes DECIMAL
    );
  `);
  
  let youtubeHashtagImported = 0;
  for (let i = 1; i < youtubeHashtagLines.length; i++) {
    const line = youtubeHashtagLines[i].trim();
    if (!line) continue;
    
    const fields = parseCSVLine(line);
    if (fields.length >= 11) {
      try {
        await pool.query(`
          INSERT INTO "nextretail_youtube_hashtag" 
          (text, hashtags, duration, date, url, comments_count, view_count, title, number_of_subscribers, channel_name, likes)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `, [
          cleanValue(fields[0]),
          cleanValue(fields[1]),
          cleanValue(fields[2]),
          cleanValue(fields[3]),
          cleanValue(fields[4]),
          parseNumber(fields[5]),
          parseNumber(fields[6]),
          cleanValue(fields[7]),
          parseNumber(fields[8]),
          cleanValue(fields[9]),
          parseNumber(fields[10])
        ]);
        youtubeHashtagImported++;
      } catch (error) {
        console.log(`Skipping row ${i}: ${error.message}`);
      }
    }
  }
  console.log(`✓ YouTube Hashtag: ${youtubeHashtagImported} records (CSV had ${youtubeHashtagLines.length - 1} lines)`);
  
  // 5. Instagram Official - Expected: 173 rows
  console.log('Importing Instagram Official (Expected: 173 rows)...');
  const instagramOfficial = fs.readFileSync('public/Insta_new_nextofficial_cleaned_2.xlsx_csv.csv', 'utf-8');
  const instagramOfficialLines = instagramOfficial.split('\n').filter(line => line.trim());
  
  await pool.query(`
    CREATE TABLE "nextretail_instagram_official" (
      id SERIAL PRIMARY KEY,
      video_play_count DECIMAL,
      url TEXT,
      hashtags TEXT,
      video_view_count DECIMAL,
      video_duration DECIMAL,
      comments_count BIGINT,
      mentions TEXT,
      caption TEXT,
      timestamp TEXT
    );
  `);
  
  let instagramOfficialImported = 0;
  for (let i = 1; i < instagramOfficialLines.length; i++) {
    const line = instagramOfficialLines[i].trim();
    if (!line) continue;
    
    const fields = parseCSVLine(line);
    if (fields.length >= 9) {
      try {
        await pool.query(`
          INSERT INTO "nextretail_instagram_official" 
          (video_play_count, url, hashtags, video_view_count, video_duration, comments_count, mentions, caption, timestamp)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, [
          parseNumber(fields[0]),
          cleanValue(fields[1]),
          cleanValue(fields[2]),
          parseNumber(fields[3]),
          parseNumber(fields[4]),
          parseNumber(fields[5]),
          cleanValue(fields[6]),
          cleanValue(fields[7]),
          cleanValue(fields[8])
        ]);
        instagramOfficialImported++;
      } catch (error) {
        console.log(`Skipping row ${i}: ${error.message}`);
      }
    }
  }
  console.log(`✓ Instagram Official: ${instagramOfficialImported} records (CSV had ${instagramOfficialLines.length - 1} lines)`);
  
  // 6. Instagram Hashtag - Expected: 1157 rows
  console.log('Importing Instagram Hashtag (Expected: 1157 rows)...');
  const instagramHashtag = fs.readFileSync('public/Insta_new_nexthashtags_cleaned.xlsx_csv.csv', 'utf-8');
  const instagramHashtagLines = instagramHashtag.split('\n').filter(line => line.trim());
  
  await pool.query(`
    CREATE TABLE "nextretail_instagram_hashtag" (
      id SERIAL PRIMARY KEY,
      hashtags TEXT,
      url TEXT,
      location_name TEXT,
      video_view_count DECIMAL,
      caption TEXT,
      video_duration DECIMAL,
      comments_count BIGINT,
      mentions TEXT,
      is_sponsored BOOLEAN,
      timestamp TEXT,
      likes_count DECIMAL
    );
  `);
  
  let instagramHashtagImported = 0;
  for (let i = 1; i < instagramHashtagLines.length; i++) {
    const line = instagramHashtagLines[i].trim();
    if (!line) continue;
    const fields = parseCSVLine(line);
    if (fields.length >= 11) {
      try {
        await pool.query(`
          INSERT INTO "nextretail_instagram_hashtag" 
          (hashtags, url, location_name, video_view_count, caption, video_duration, comments_count, mentions, is_sponsored, timestamp, likes_count)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `, [
          cleanValue(fields[0]),
          cleanValue(fields[1]),
          cleanValue(fields[2]),
          parseNumber(fields[3]),
          cleanValue(fields[4]),
          parseNumber(fields[5]),
          parseNumber(fields[6]),
          cleanValue(fields[7]),
          cleanValue(fields[8]) === 'True' ? true : false,
          cleanValue(fields[9]),
          parseNumber(fields[10])
        ]);
        instagramHashtagImported++;
      } catch (error) {
        console.log(`Skipping row ${i}: ${error.message}`);
      }
    }
  }
  console.log(`✓ Instagram Hashtag: ${instagramHashtagImported} records (CSV had ${instagramHashtagLines.length - 1} lines)`);
  
  const totalImported = tiktokOfficialImported + tiktokHashtagImported + youtubeOfficialImported + youtubeHashtagImported + instagramOfficialImported + instagramHashtagImported;
  console.log('\n=== COMPLETE NEXT RETAIL IMPORT RESULTS ===');
  console.log(`Total records imported: ${totalImported}`);
  return totalImported;
}

async function main() {
  try {
    await importCompleteDatasets();
  } catch (error) {
    console.error('Import failed:', error);
  } finally {
    await pool.end();
  }
}

main();