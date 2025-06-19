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

async function importTikTokOfficial() {
  console.log('Importing Next TikTok Official dataset...');
  
  const filePath = 'attached_assets/tiktok_NEXT_Official_cleaned.xlsx_csv_1749116297478.csv';
  const tableName = 'nextretail_tiktok_official';
  
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter(line => line.trim());
  const headers = parseCSVLine(lines[0]);
  
  console.log(`Headers: ${headers.join(', ')}`);
  console.log(`Total lines: ${lines.length - 1}`);
  
  // Create table
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS "${tableName}" (
      id SERIAL PRIMARY KEY,
      "Unnamed: 0" BIGINT,
      text TEXT,
      created_time TEXT,
      "mentions/0" TEXT,
      hashtags TEXT,
      "shareCount" BIGINT,
      "commentCount" BIGINT,
      "playCount" BIGINT,
      "collectCount" BIGINT,
      "diggCount" BIGINT
    );
  `;
  
  await pool.query(createTableQuery);
  console.log(`Table "${tableName}" created`);
  
  // Reset ID sequence
  await pool.query(`ALTER SEQUENCE "${tableName}_id_seq" RESTART WITH 1;`);
  
  let imported = 0;
  const batchSize = 100;
  
  for (let i = 1; i < lines.length; i += batchSize) {
    const batch = [];
    const endIndex = Math.min(i + batchSize, lines.length);
    
    for (let j = i; j < endIndex; j++) {
      const line = lines[j].trim();
      if (!line) continue;
      
      const fields = parseCSVLine(line);
      if (fields.length >= 10) {
        batch.push([
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
    
    if (batch.length > 0) {
      const placeholders = batch.map((_, idx) => 
        `($${idx * 10 + 1}, $${idx * 10 + 2}, $${idx * 10 + 3}, $${idx * 10 + 4}, $${idx * 10 + 5}, $${idx * 10 + 6}, $${idx * 10 + 7}, $${idx * 10 + 8}, $${idx * 10 + 9}, $${idx * 10 + 10})`
      ).join(', ');
      
      const insertQuery = `
        INSERT INTO "${tableName}" ("Unnamed: 0", text, created_time, "mentions/0", hashtags, "shareCount", "commentCount", "playCount", "collectCount", "diggCount")
        VALUES ${placeholders}
      `;
      
      const flatValues = batch.flat();
      await pool.query(insertQuery, flatValues);
      imported += batch.length;
    }
  }
  
  console.log(`✓ TikTok Official: ${imported} records imported`);
  return imported;
}

async function importTikTokHashtag() {
  console.log('Importing Next TikTok Hashtag dataset...');
  
  const filePath = 'attached_assets/dataset_tiktok-hashtag_NextRetail_cleaned.xlsx_csv_1749116307865.csv';
  const tableName = 'nextretail_tiktok_hashtag';
  
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter(line => line.trim());
  const headers = parseCSVLine(lines[0]);
  
  console.log(`Headers: ${headers.join(', ')}`);
  console.log(`Total lines: ${lines.length - 1}`);
  
  // Create table
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS "${tableName}" (
      id SERIAL PRIMARY KEY,
      text TEXT,
      created_time TEXT,
      hashtags TEXT,
      "shareCount" BIGINT,
      mentions TEXT,
      "commentCount" BIGINT,
      "playCount" BIGINT,
      "collectCount" BIGINT,
      "diggCount" BIGINT
    );
  `;
  
  await pool.query(createTableQuery);
  console.log(`Table "${tableName}" created`);
  
  // Reset ID sequence
  await pool.query(`ALTER SEQUENCE "${tableName}_id_seq" RESTART WITH 1;`);
  
  let imported = 0;
  const batchSize = 100;
  
  for (let i = 1; i < lines.length; i += batchSize) {
    const batch = [];
    const endIndex = Math.min(i + batchSize, lines.length);
    
    for (let j = i; j < endIndex; j++) {
      const line = lines[j].trim();
      if (!line) continue;
      
      const fields = parseCSVLine(line);
      if (fields.length >= 9) {
        batch.push([
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
    
    if (batch.length > 0) {
      const placeholders = batch.map((_, idx) => 
        `($${idx * 9 + 1}, $${idx * 9 + 2}, $${idx * 9 + 3}, $${idx * 9 + 4}, $${idx * 9 + 5}, $${idx * 9 + 6}, $${idx * 9 + 7}, $${idx * 9 + 8}, $${idx * 9 + 9})`
      ).join(', ');
      
      const insertQuery = `
        INSERT INTO "${tableName}" (text, created_time, hashtags, "shareCount", mentions, "commentCount", "playCount", "collectCount", "diggCount")
        VALUES ${placeholders}
      `;
      
      const flatValues = batch.flat();
      await pool.query(insertQuery, flatValues);
      imported += batch.length;
    }
  }
  
  console.log(`✓ TikTok Hashtag: ${imported} records imported`);
  return imported;
}

async function importYouTubeOfficial() {
  console.log('Importing Next YouTube Official dataset...');
  
  const filePath = 'attached_assets/dataset_youtube-channel-scraper_NextRetail-Official 1_cleaned.xlsx_csv_1749116321778.csv';
  const tableName = 'nextretail_youtube_official';
  
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter(line => line.trim());
  const headers = parseCSVLine(lines[0]);
  
  console.log(`Headers: ${headers.join(', ')}`);
  console.log(`Total lines: ${lines.length - 1}`);
  
  // Create table
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS "${tableName}" (
      id SERIAL PRIMARY KEY,
      "channelTotalViews" BIGINT,
      url TEXT,
      duration TEXT,
      date TEXT,
      "viewCount" BIGINT,
      title TEXT,
      "channelTotalVideos" BIGINT,
      "numberOfSubscribers" BIGINT,
      "channelDescription" TEXT,
      "channelJoinedDate" TEXT,
      "channelLocation" TEXT
    );
  `;
  
  await pool.query(createTableQuery);
  console.log(`Table "${tableName}" created`);
  
  // Reset ID sequence
  await pool.query(`ALTER SEQUENCE "${tableName}_id_seq" RESTART WITH 1;`);
  
  let imported = 0;
  const batchSize = 100;
  
  for (let i = 1; i < lines.length; i += batchSize) {
    const batch = [];
    const endIndex = Math.min(i + batchSize, lines.length);
    
    for (let j = i; j < endIndex; j++) {
      const line = lines[j].trim();
      if (!line) continue;
      
      const fields = parseCSVLine(line);
      if (fields.length >= 11) {
        batch.push([
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
    
    if (batch.length > 0) {
      const placeholders = batch.map((_, idx) => 
        `($${idx * 11 + 1}, $${idx * 11 + 2}, $${idx * 11 + 3}, $${idx * 11 + 4}, $${idx * 11 + 5}, $${idx * 11 + 6}, $${idx * 11 + 7}, $${idx * 11 + 8}, $${idx * 11 + 9}, $${idx * 11 + 10}, $${idx * 11 + 11})`
      ).join(', ');
      
      const insertQuery = `
        INSERT INTO "${tableName}" ("channelTotalViews", url, duration, date, "viewCount", title, "channelTotalVideos", "numberOfSubscribers", "channelDescription", "channelJoinedDate", "channelLocation")
        VALUES ${placeholders}
      `;
      
      const flatValues = batch.flat();
      await pool.query(insertQuery, flatValues);
      imported += batch.length;
    }
  }
  
  console.log(`✓ YouTube Official: ${imported} records imported`);
  return imported;
}

async function importYouTubeHashtag() {
  console.log('Importing Next YouTube Hashtag dataset...');
  
  const filePath = 'attached_assets/dataset_youtube-Hashtag_Next 1_cleaned.xlsx_csv_1749116331413.csv';
  const tableName = 'nextretail_youtube_hashtag';
  
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter(line => line.trim());
  const headers = parseCSVLine(lines[0]);
  
  console.log(`Headers: ${headers.join(', ')}`);
  console.log(`Total lines: ${lines.length - 1}`);
  
  // Create table
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS "${tableName}" (
      id SERIAL PRIMARY KEY,
      text TEXT,
      hashtags TEXT,
      duration TEXT,
      date TEXT,
      url TEXT,
      "commentsCount" DECIMAL,
      "viewCount" DECIMAL,
      title TEXT,
      "numberOfSubscribers" DECIMAL,
      "channelName" TEXT,
      likes DECIMAL
    );
  `;
  
  await pool.query(createTableQuery);
  console.log(`Table "${tableName}" created`);
  
  // Reset ID sequence
  await pool.query(`ALTER SEQUENCE "${tableName}_id_seq" RESTART WITH 1;`);
  
  let imported = 0;
  const batchSize = 100;
  
  for (let i = 1; i < lines.length; i += batchSize) {
    const batch = [];
    const endIndex = Math.min(i + batchSize, lines.length);
    
    for (let j = i; j < endIndex; j++) {
      const line = lines[j].trim();
      if (!line) continue;
      
      const fields = parseCSVLine(line);
      if (fields.length >= 11) {
        batch.push([
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
    
    if (batch.length > 0) {
      const placeholders = batch.map((_, idx) => 
        `($${idx * 11 + 1}, $${idx * 11 + 2}, $${idx * 11 + 3}, $${idx * 11 + 4}, $${idx * 11 + 5}, $${idx * 11 + 6}, $${idx * 11 + 7}, $${idx * 11 + 8}, $${idx * 11 + 9}, $${idx * 11 + 10}, $${idx * 11 + 11})`
      ).join(', ');
      
      const insertQuery = `
        INSERT INTO "${tableName}" (text, hashtags, duration, date, url, "commentsCount", "viewCount", title, "numberOfSubscribers", "channelName", likes)
        VALUES ${placeholders}
      `;
      
      const flatValues = batch.flat();
      await pool.query(insertQuery, flatValues);
      imported += batch.length;
    }
  }
  
  console.log(`✓ YouTube Hashtag: ${imported} records imported`);
  return imported;
}

async function importInstagramOfficial() {
  console.log('Importing Next Instagram Official dataset...');
  
  const filePath = 'attached_assets/Insta_new_nextofficial_cleaned_2.xlsx_csv_1749116347205.csv';
  const tableName = 'nextretail_instagram_official';
  
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter(line => line.trim());
  const headers = parseCSVLine(lines[0]);
  
  console.log(`Headers: ${headers.join(', ')}`);
  console.log(`Total lines: ${lines.length - 1}`);
  
  // Create table
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS "${tableName}" (
      id SERIAL PRIMARY KEY,
      "videoPlayCount" DECIMAL,
      url TEXT,
      hashtags TEXT,
      "videoViewCount" DECIMAL,
      "videoDuration" DECIMAL,
      "commentsCount" BIGINT,
      mentions TEXT,
      caption TEXT,
      timestamp TEXT
    );
  `;
  
  await pool.query(createTableQuery);
  console.log(`Table "${tableName}" created`);
  
  // Reset ID sequence
  await pool.query(`ALTER SEQUENCE "${tableName}_id_seq" RESTART WITH 1;`);
  
  let imported = 0;
  const batchSize = 100;
  
  for (let i = 1; i < lines.length; i += batchSize) {
    const batch = [];
    const endIndex = Math.min(i + batchSize, lines.length);
    
    for (let j = i; j < endIndex; j++) {
      const line = lines[j].trim();
      if (!line) continue;
      
      const fields = parseCSVLine(line);
      if (fields.length >= 9) {
        batch.push([
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
    
    if (batch.length > 0) {
      const placeholders = batch.map((_, idx) => 
        `($${idx * 9 + 1}, $${idx * 9 + 2}, $${idx * 9 + 3}, $${idx * 9 + 4}, $${idx * 9 + 5}, $${idx * 9 + 6}, $${idx * 9 + 7}, $${idx * 9 + 8}, $${idx * 9 + 9})`
      ).join(', ');
      
      const insertQuery = `
        INSERT INTO "${tableName}" ("videoPlayCount", url, hashtags, "videoViewCount", "videoDuration", "commentsCount", mentions, caption, timestamp)
        VALUES ${placeholders}
      `;
      
      const flatValues = batch.flat();
      await pool.query(insertQuery, flatValues);
      imported += batch.length;
    }
  }
  
  console.log(`✓ Instagram Official: ${imported} records imported`);
  return imported;
}

async function importInstagramHashtag() {
  console.log('Importing Next Instagram Hashtag dataset...');
  
  const filePath = 'attached_assets/Insta_new_nexthashtags_cleaned.xlsx_csv_1749116356260.csv';
  const tableName = 'nextretail_instagram_hashtag';
  
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter(line => line.trim());
  const headers = parseCSVLine(lines[0]);
  
  console.log(`Headers: ${headers.join(', ')}`);
  console.log(`Total lines: ${lines.length - 1}`);
  
  // Create table
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS "${tableName}" (
      id SERIAL PRIMARY KEY,
      hashtags TEXT,
      url TEXT,
      "locationName" TEXT,
      "videoViewCount" DECIMAL,
      caption TEXT,
      "videoDuration" DECIMAL,
      "commentsCount" BIGINT,
      mentions TEXT,
      "isSponsored" BOOLEAN,
      timestamp TEXT,
      "likesCount" DECIMAL
    );
  `;
  
  await pool.query(createTableQuery);
  console.log(`Table "${tableName}" created`);
  
  // Reset ID sequence
  await pool.query(`ALTER SEQUENCE "${tableName}_id_seq" RESTART WITH 1;`);
  
  let imported = 0;
  const batchSize = 100;
  
  for (let i = 1; i < lines.length; i += batchSize) {
    const batch = [];
    const endIndex = Math.min(i + batchSize, lines.length);
    
    for (let j = i; j < endIndex; j++) {
      const line = lines[j].trim();
      if (!line) continue;
      
      const fields = parseCSVLine(line);
      if (fields.length >= 11) {
        batch.push([
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
    
    if (batch.length > 0) {
      const placeholders = batch.map((_, idx) => 
        `($${idx * 11 + 1}, $${idx * 11 + 2}, $${idx * 11 + 3}, $${idx * 11 + 4}, $${idx * 11 + 5}, $${idx * 11 + 6}, $${idx * 11 + 7}, $${idx * 11 + 8}, $${idx * 11 + 9}, $${idx * 11 + 10}, $${idx * 11 + 11})`
      ).join(', ');
      
      const insertQuery = `
        INSERT INTO "${tableName}" (hashtags, url, "locationName", "videoViewCount", caption, "videoDuration", "commentsCount", mentions, "isSponsored", timestamp, "likesCount")
        VALUES ${placeholders}
      `;
      
      const flatValues = batch.flat();
      await pool.query(insertQuery, flatValues);
      imported += batch.length;
    }
  }
  
  console.log(`✓ Instagram Hashtag: ${imported} records imported`);
  return imported;
}

async function main() {
  try {
    console.log('=== Next Retail Dataset Import ===');
    
    const results = [];
    
    results.push(await importTikTokOfficial());
    results.push(await importTikTokHashtag());
    results.push(await importYouTubeOfficial());
    results.push(await importYouTubeHashtag());
    results.push(await importInstagramOfficial());
    results.push(await importInstagramHashtag());
    
    const totalImported = results.reduce((sum, count) => sum + count, 0);
    
    console.log('\n=== IMPORT COMPLETE ===');
    console.log(`Total records imported: ${totalImported}`);
    console.log('✓ All Next Retail datasets imported successfully');
    
  } catch (error) {
    console.error('Import failed:', error);
  } finally {
    await pool.end();
  }
}

main();