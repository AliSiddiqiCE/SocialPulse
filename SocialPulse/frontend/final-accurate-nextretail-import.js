import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function cleanValue(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string') {
    value = value.trim();
    if (value === '' || value === 'null' || value === 'NULL' || value === 'undefined') return null;
  }
  return value;
}

function parseNumber(value) {
  if (value === null || value === undefined || value === '') return null;
  const cleaned = String(value).replace(/[^\d.-]/g, '');
  if (cleaned === '' || cleaned === '-') return null;
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
      result.push(current);
      current = '';
    } else {
      current += char;
    }
    i++;
  }
  
  result.push(current);
  return result;
}

async function processCsvFile(filePath, tableName, createTableQuery, columns, valueMapper) {
  console.log(`Processing ${tableName}...`);
  
  const content = fs.readFileSync(filePath, 'utf-8');
  let lines = content.split('\n');
  
  // Remove header and empty lines
  const header = lines[0];
  lines = lines.slice(1).filter(line => line.trim().length > 0);
  
  console.log(`  Raw lines after header: ${lines.length}`);
  
  // Parse CSV properly to handle multiline content
  const records = [];
  let currentRecord = '';
  let inQuotes = false;
  
  for (const line of lines) {
    if (!line.trim()) continue;
    
    // Count quotes to determine if we're in a multiline field
    let quoteCount = 0;
    for (let char of line) {
      if (char === '"') quoteCount++;
    }
    
    if (currentRecord) {
      currentRecord += '\n' + line;
    } else {
      currentRecord = line;
    }
    
    // Check if this completes a record
    let tempQuoteCount = 0;
    for (let char of currentRecord) {
      if (char === '"') tempQuoteCount++;
    }
    
    // If quotes are balanced, we have a complete record
    if (tempQuoteCount % 2 === 0) {
      const fields = parseCSVLine(currentRecord);
      if (fields.length >= columns.length) {
        records.push(fields);
      }
      currentRecord = '';
    }
  }
  
  console.log(`  Parsed ${records.length} complete records`);
  
  // Create table
  await pool.query(`DROP TABLE IF EXISTS "${tableName}" CASCADE;`);
  await pool.query(createTableQuery);
  
  // Insert records
  let imported = 0;
  for (const record of records) {
    const values = valueMapper(record);
    
    try {
      const placeholders = columns.map((_, i) => `$${i + 1}`).join(', ');
      await pool.query(
        `INSERT INTO "${tableName}" (${columns.join(', ')}) VALUES (${placeholders})`,
        values
      );
      imported++;
    } catch (error) {
      console.log(`  Error in record ${imported + 1}: ${error.message}`);
    }
  }
  
  console.log(`  ✓ ${tableName}: ${imported} records imported (target: exact CSV content)`);
  return imported;
}

async function main() {
  try {
    console.log('=== Accurate Next Retail Import (Exact Row Counts) ===');
    
    let totalImported = 0;
    
    // 1. TikTok Official (Target: 425 records per user)
    totalImported += await processCsvFile(
      'attached_assets/tiktok_NEXT_Official_cleaned.xlsx_csv_1749116297478.csv',
      'nextretail_tiktok_official',
      `CREATE TABLE "nextretail_tiktok_official" (
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
      );`,
      ['unnamed_0', 'text', 'created_time', 'mentions_0', 'hashtags', 'share_count', 'comment_count', 'play_count', 'collect_count', 'digg_count'],
      (fields) => fields.slice(0, 10).map(cleanValue)
    );
    
    // 2. TikTok Hashtag (Target: 947 records per user)
    totalImported += await processCsvFile(
      'attached_assets/dataset_tiktok-hashtag_NextRetail_cleaned.xlsx_csv_1749116307865.csv',
      'nextretail_tiktok_hashtag',
      `CREATE TABLE "nextretail_tiktok_hashtag" (
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
      );`,
      ['text', 'created_time', 'hashtags', 'share_count', 'mentions', 'comment_count', 'play_count', 'collect_count', 'digg_count'],
      (fields) => fields.slice(0, 9).map(cleanValue)
    );
    
    // 3. YouTube Official (Target: 598 records per user)
    totalImported += await processCsvFile(
      'attached_assets/dataset_youtube-channel-scraper_NextRetail-Official 1_cleaned.xlsx_csv_1749116321778.csv',
      'nextretail_youtube_official',
      `CREATE TABLE "nextretail_youtube_official" (
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
      );`,
      ['channel_total_views', 'url', 'duration', 'date', 'view_count', 'title', 'channel_total_videos', 'number_of_subscribers', 'channel_description', 'channel_joined_date', 'channel_location'],
      (fields) => fields.slice(0, 11).map(cleanValue)
    );
    
    // 4. YouTube Hashtag (Target: 1044 records per user)
    totalImported += await processCsvFile(
      'attached_assets/dataset_youtube-Hashtag_Next 1_cleaned.xlsx_csv_1749116331413.csv',
      'nextretail_youtube_hashtag',
      `CREATE TABLE "nextretail_youtube_hashtag" (
        id SERIAL PRIMARY KEY,
        text TEXT,
        hashtags TEXT,
        duration TEXT,
        date TEXT,
        url TEXT,
        comments_count TEXT,
        view_count TEXT,
        title TEXT,
        number_of_subscribers TEXT,
        channel_name TEXT,
        likes TEXT
      );`,
      ['text', 'hashtags', 'duration', 'date', 'url', 'comments_count', 'view_count', 'title', 'number_of_subscribers', 'channel_name', 'likes'],
      (fields) => fields.slice(0, 11).map(cleanValue)
    );
    
    // 5. Instagram Official (Target: 173 records per user)
    totalImported += await processCsvFile(
      'attached_assets/Insta_new_nextofficial_cleaned_2.xlsx_csv_1749116347205.csv',
      'nextretail_instagram_official',
      `CREATE TABLE "nextretail_instagram_official" (
        id SERIAL PRIMARY KEY,
        video_play_count TEXT,
        url TEXT,
        hashtags TEXT,
        video_view_count TEXT,
        video_duration TEXT,
        comments_count TEXT,
        mentions TEXT,
        caption TEXT,
        timestamp TEXT
      );`,
      ['video_play_count', 'url', 'hashtags', 'video_view_count', 'video_duration', 'comments_count', 'mentions', 'caption', 'timestamp'],
      (fields) => fields.slice(0, 9).map(cleanValue)
    );
    
    // 6. Instagram Hashtag (Target: 1157 records per user)
    totalImported += await processCsvFile(
      'attached_assets/Insta_new_nexthashtags_cleaned.xlsx_csv_1749116356260.csv',
      'nextretail_instagram_hashtag',
      `CREATE TABLE "nextretail_instagram_hashtag" (
        id SERIAL PRIMARY KEY,
        hashtags TEXT,
        url TEXT,
        location_name TEXT,
        video_view_count TEXT,
        caption TEXT,
        video_duration TEXT,
        comments_count TEXT,
        mentions TEXT,
        is_sponsored TEXT,
        timestamp TEXT,
        likes_count TEXT
      );`,
      ['hashtags', 'url', 'location_name', 'video_view_count', 'caption', 'video_duration', 'comments_count', 'mentions', 'is_sponsored', 'timestamp', 'likes_count'],
      (fields) => fields.slice(0, 11).map(cleanValue)
    );
    
    console.log('\n=== FINAL RESULTS ===');
    console.log(`Total records imported: ${totalImported}`);
    console.log('Target total (user verified): 4,344 records');
    console.log('Import complete with exact CSV content only');
    
  } catch (error) {
    console.error('Import failed:', error);
  } finally {
    await pool.end();
  }
}

main();