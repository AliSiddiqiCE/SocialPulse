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

async function batchInsertOptimized(tableName, columns, records, batchSize = 500) {
  if (records.length === 0) return 0;
  
  let inserted = 0;
  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    const placeholders = [];
    const values = [];
    
    batch.forEach((record, idx) => {
      const startIdx = idx * columns.length + 1;
      const recordPlaceholders = columns.map((_, colIdx) => `$${startIdx + colIdx}`).join(', ');
      placeholders.push(`(${recordPlaceholders})`);
      values.push(...record);
    });
    
    const query = `INSERT INTO "${tableName}" (${columns.join(', ')}) VALUES ${placeholders.join(', ')}`;
    await pool.query(query, values);
    inserted += batch.length;
  }
  
  return inserted;
}

async function main() {
  try {
    console.log('=== Optimized Next Retail Complete Import ===');
    
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
    console.log('✓ Tables dropped');
    
    let totalImported = 0;
    
    // 1. TikTok Official
    console.log('Processing TikTok Official...');
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
    
    const tiktokOfficialContent = fs.readFileSync('attached_assets/tiktok_NEXT_Official_cleaned.xlsx_csv_1749116297478.csv', 'utf-8');
    const tiktokOfficialLines = tiktokOfficialContent.split('\n').filter(line => line.trim());
    const tiktokRecords = [];
    
    for (let i = 1; i < tiktokOfficialLines.length; i++) {
      const line = tiktokOfficialLines[i].trim();
      if (!line) continue;
      
      const fields = parseCSVLine(line);
      if (fields.length >= 10) {
        tiktokRecords.push([
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
      }
    }
    
    const tiktokOfficialCount = await batchInsertOptimized('nextretail_tiktok_official', 
      ['unnamed_0', 'text', 'created_time', 'mentions_0', 'hashtags', 'share_count', 'comment_count', 'play_count', 'collect_count', 'digg_count'],
      tiktokRecords
    );
    totalImported += tiktokOfficialCount;
    console.log(`✓ TikTok Official: ${tiktokOfficialCount} records`);
    
    // 2. TikTok Hashtag
    console.log('Processing TikTok Hashtag...');
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
    
    const tiktokHashtagContent = fs.readFileSync('attached_assets/dataset_tiktok-hashtag_NextRetail_cleaned.xlsx_csv_1749116307865.csv', 'utf-8');
    const tiktokHashtagLines = tiktokHashtagContent.split('\n').filter(line => line.trim());
    const tiktokHashtagRecords = [];
    
    for (let i = 1; i < tiktokHashtagLines.length; i++) {
      const line = tiktokHashtagLines[i].trim();
      if (!line) continue;
      
      const fields = parseCSVLine(line);
      if (fields.length >= 9) {
        tiktokHashtagRecords.push([
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
      }
    }
    
    const tiktokHashtagCount = await batchInsertOptimized('nextretail_tiktok_hashtag', 
      ['text', 'created_time', 'hashtags', 'share_count', 'mentions', 'comment_count', 'play_count', 'collect_count', 'digg_count'],
      tiktokHashtagRecords
    );
    totalImported += tiktokHashtagCount;
    console.log(`✓ TikTok Hashtag: ${tiktokHashtagCount} records`);
    
    // 3. YouTube Official
    console.log('Processing YouTube Official...');
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
    
    const youtubeOfficialContent = fs.readFileSync('attached_assets/dataset_youtube-channel-scraper_NextRetail-Official 1_cleaned.xlsx_csv_1749116321778.csv', 'utf-8');
    const youtubeOfficialLines = youtubeOfficialContent.split('\n').filter(line => line.trim());
    const youtubeOfficialRecords = [];
    
    for (let i = 1; i < youtubeOfficialLines.length; i++) {
      const line = youtubeOfficialLines[i].trim();
      if (!line) continue;
      
      const fields = parseCSVLine(line);
      if (fields.length >= 11) {
        youtubeOfficialRecords.push([
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
      }
    }
    
    const youtubeOfficialCount = await batchInsertOptimized('nextretail_youtube_official', 
      ['channel_total_views', 'url', 'duration', 'date', 'view_count', 'title', 'channel_total_videos', 'number_of_subscribers', 'channel_description', 'channel_joined_date', 'channel_location'],
      youtubeOfficialRecords
    );
    totalImported += youtubeOfficialCount;
    console.log(`✓ YouTube Official: ${youtubeOfficialCount} records`);
    
    // 4. YouTube Hashtag
    console.log('Processing YouTube Hashtag...');
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
    
    const youtubeHashtagContent = fs.readFileSync('attached_assets/dataset_youtube-Hashtag_Next 1_cleaned.xlsx_csv_1749116331413.csv', 'utf-8');
    const youtubeHashtagLines = youtubeHashtagContent.split('\n').filter(line => line.trim());
    const youtubeHashtagRecords = [];
    
    for (let i = 1; i < youtubeHashtagLines.length; i++) {
      const line = youtubeHashtagLines[i].trim();
      if (!line) continue;
      
      const fields = parseCSVLine(line);
      if (fields.length >= 11) {
        youtubeHashtagRecords.push([
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
      }
    }
    
    const youtubeHashtagCount = await batchInsertOptimized('nextretail_youtube_hashtag', 
      ['text', 'hashtags', 'duration', 'date', 'url', 'comments_count', 'view_count', 'title', 'number_of_subscribers', 'channel_name', 'likes'],
      youtubeHashtagRecords
    );
    totalImported += youtubeHashtagCount;
    console.log(`✓ YouTube Hashtag: ${youtubeHashtagCount} records`);
    
    // 5. Instagram Official
    console.log('Processing Instagram Official...');
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
    
    const instagramOfficialContent = fs.readFileSync('attached_assets/Insta_new_nextofficial_cleaned_2.xlsx_csv_1749116347205.csv', 'utf-8');
    const instagramOfficialLines = instagramOfficialContent.split('\n').filter(line => line.trim());
    const instagramOfficialRecords = [];
    
    for (let i = 1; i < instagramOfficialLines.length; i++) {
      const line = instagramOfficialLines[i].trim();
      if (!line) continue;
      
      const fields = parseCSVLine(line);
      if (fields.length >= 9) {
        instagramOfficialRecords.push([
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
      }
    }
    
    const instagramOfficialCount = await batchInsertOptimized('nextretail_instagram_official', 
      ['video_play_count', 'url', 'hashtags', 'video_view_count', 'video_duration', 'comments_count', 'mentions', 'caption', 'timestamp'],
      instagramOfficialRecords
    );
    totalImported += instagramOfficialCount;
    console.log(`✓ Instagram Official: ${instagramOfficialCount} records`);
    
    // 6. Instagram Hashtag
    console.log('Processing Instagram Hashtag...');
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
    
    const instagramHashtagContent = fs.readFileSync('attached_assets/Insta_new_nexthashtags_cleaned.xlsx_csv_1749116356260.csv', 'utf-8');
    const instagramHashtagLines = instagramHashtagContent.split('\n').filter(line => line.trim());
    const instagramHashtagRecords = [];
    
    for (let i = 1; i < instagramHashtagLines.length; i++) {
      const line = instagramHashtagLines[i].trim();
      if (!line) continue;
      
      const fields = parseCSVLine(line);
      if (fields.length >= 11) {
        instagramHashtagRecords.push([
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
      }
    }
    
    const instagramHashtagCount = await batchInsertOptimized('nextretail_instagram_hashtag', 
      ['hashtags', 'url', 'location_name', 'video_view_count', 'caption', 'video_duration', 'comments_count', 'mentions', 'is_sponsored', 'timestamp', 'likes_count'],
      instagramHashtagRecords
    );
    totalImported += instagramHashtagCount;
    console.log(`✓ Instagram Hashtag: ${instagramHashtagCount} records`);
    
    console.log('\n=== FINAL RESULTS ===');
    console.log(`Total imported: ${totalImported} (Target: 4344)`);
    console.log(`Success rate: ${((totalImported / 4344) * 100).toFixed(1)}%`);
    
    if (totalImported === 4344) {
      console.log('✅ COMPLETE SUCCESS - All records imported');
    }
    
  } catch (error) {
    console.error('Import error:', error);
  } finally {
    await pool.end();
  }
}

main();