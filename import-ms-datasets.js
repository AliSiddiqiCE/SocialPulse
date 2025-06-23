import fs from 'fs';
import path from 'path';
import pkg from 'pg';
const { Pool } = pkg;

// Database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function parseCSV(content) {
  const lines = content.split('\n');
  const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
  const rows = [];
  
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    
    // Parse CSV line with proper quote handling
    const values = [];
    let current = '';
    let inQuotes = false;
    
    for (let j = 0; j < line.length; j++) {
      const char = line[j];
      if (char === '"') {
        inQuotes = !inQuotes;
      } else if (char === ',' && !inQuotes) {
        values.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    values.push(current.trim());
    
    // Create row object
    const row = {};
    headers.forEach((header, index) => {
      let value = values[index] || '';
      // Remove quotes if present
      if (value.startsWith('"') && value.endsWith('"')) {
        value = value.slice(1, -1);
      }
      row[header] = value === '' ? null : value;
    });
    
    rows.push(row);
  }
  
  return { headers, rows };
}

async function importTikTokOfficial() {
  console.log('Importing TikTok Official M&S data...');
  
  const content = fs.readFileSync('attached_assets/dataset_tiktok_M&S_official_cleaned.xlsx_csv.csv', 'utf8');
  const { rows } = parseCSV(content);
  
  // Clear existing data
  await pool.query('DELETE FROM "dataset_tiktok_M&S_official_cleaned.xlsx_csv"');
  
  for (const row of rows) {
    await pool.query(`
      INSERT INTO "dataset_tiktok_M&S_official_cleaned.xlsx_csv" 
      (text, created_time, hashtags, "shareCount", mentions, "commentCount", "playCount", "diggCount", "collectCount")
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    `, [
      row.text,
      row.created_time,
      row.hashtags,
      row.shareCount ? parseInt(row.shareCount) : null,
      row.mentions,
      row.commentCount ? parseInt(row.commentCount) : null,
      row.playCount ? parseInt(row.playCount) : null,
      row.diggCount ? parseInt(row.diggCount) : null,
      row.collectCount ? parseInt(row.collectCount) : null
    ]);
  }
  
  console.log(`Imported ${rows.length} TikTok Official records`);
}

async function importTikTokHashtag() {
  console.log('Importing TikTok Hashtag M&S data...');
  
  const content = fs.readFileSync('attached_assets/dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv.csv', 'utf8');
  const { rows } = parseCSV(content);
  
  // Clear existing data
  await pool.query('DELETE FROM "dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv"');
  
  for (const row of rows) {
    await pool.query(`
      INSERT INTO "dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv" 
      (text, created_time, hashtags, "shareCount", mentions, "commentCount", "playCount", "collectCount", "diggCount")
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    `, [
      row.text,
      row.created_time,
      row.hashtags,
      row.shareCount ? parseInt(row.shareCount) : null,
      row.mentions,
      row.commentCount ? parseInt(row.commentCount) : null,
      row.playCount ? parseInt(row.playCount) : null,
      row.collectCount ? parseInt(row.collectCount) : null,
      row.diggCount ? parseInt(row.diggCount) : null
    ]);
  }
  
  console.log(`Imported ${rows.length} TikTok Hashtag records`);
}

async function importYouTubeOfficial() {
  console.log('Importing YouTube Official M&S data...');
  
  const content = fs.readFileSync('attached_assets/dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv.csv', 'utf8');
  const { rows } = parseCSV(content);
  
  // Clear existing data
  await pool.query('DELETE FROM "dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv"');
  
  for (const row of rows) {
    await pool.query(`
      INSERT INTO "dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv" 
      ("channelTotalViews", url, duration, date, title, "numberOfSubscribers", "channelDescription", "channelTotalVideos", "channelJoinedDate")
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    `, [
      row.channelTotalViews ? parseInt(row.channelTotalViews) : null,
      row.url,
      row.duration,
      row.date,
      row.title,
      row.numberOfSubscribers ? parseInt(row.numberOfSubscribers) : null,
      row.channelDescription,
      row.channelTotalVideos ? parseInt(row.channelTotalVideos) : null,
      row.channelJoinedDate
    ]);
  }
  
  console.log(`Imported ${rows.length} YouTube Official records`);
}

async function importYouTubeHashtag() {
  console.log('Importing YouTube Hashtag M&S data...');
  
  const content = fs.readFileSync('attached_assets/dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv.csv', 'utf8');
  const { rows } = parseCSV(content);
  
  // Clear existing data
  await pool.query('DELETE FROM "dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv"');
  
  for (const row of rows) {
    await pool.query(`
      INSERT INTO "dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv" 
      (text, hashtags, duration, date, url, "commentsCount", title, "numberOfSubscribers", "viewCount", "channelName", likes)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    `, [
      row.text,
      row.hashtags,
      row.duration,
      row.date,
      row.url,
      row.commentsCount ? parseInt(row.commentsCount) : null,
      row.title,
      row.numberOfSubscribers ? parseInt(row.numberOfSubscribers) : null,
      row.viewCount ? parseInt(row.viewCount) : null,
      row.channelName,
      row.likes ? parseInt(row.likes) : null
    ]);
  }
  
  console.log(`Imported ${rows.length} YouTube Hashtag records`);
}

async function importInstagramOfficial() {
  console.log('Importing Instagram Official M&S data...');
  
  const content = fs.readFileSync('attached_assets/Insta_new_marksandspencer_cleaned.xlsx_csv.csv', 'utf8');
  const { rows } = parseCSV(content);
  
  // Clear existing data
  await pool.query('DELETE FROM "Insta_new_marksandspencer_cleaned.xlsx_csv"');
  
  for (const row of rows) {
    await pool.query(`
      INSERT INTO "Insta_new_marksandspencer_cleaned.xlsx_csv" 
      ("videoPlayCount", hashtags, url, "locationName", "videoViewCount", "videoDuration", "commentsCount", mentions, caption, timestamp, "likesCount")
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    `, [
      row.videoPlayCount ? parseFloat(row.videoPlayCount) : null,
      row.hashtags,
      row.url,
      row.locationName,
      row.videoViewCount ? parseFloat(row.videoViewCount) : null,
      row.videoDuration ? parseFloat(row.videoDuration) : null,
      row.commentsCount ? parseInt(row.commentsCount) : null,
      row.mentions,
      row.caption,
      row.timestamp,
      row.likesCount ? parseInt(row.likesCount) : null
    ]);
  }
  
  console.log(`Imported ${rows.length} Instagram Official records`);
}

async function importInstagramHashtag() {
  console.log('Importing Instagram Hashtag M&S data...');
  
  const content = fs.readFileSync('attached_assets/Insta_new_mandshashtags_cleaned.xlsx_csv.csv', 'utf8');
  const { rows } = parseCSV(content);
  
  // Clear existing data
  await pool.query('DELETE FROM "Insta_new_mandshashtags_cleaned.xlsx_csv"');
  
  for (const row of rows) {
    await pool.query(`
      INSERT INTO "Insta_new_mandshashtags_cleaned.xlsx_csv" 
      (hashtags, url, "locationName", "paidPartnership", caption, "videoDuration", "commentsCount", mentions, "isSponsored", timestamp, "likesCount")
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    `, [
      row.hashtags,
      row.url,
      row.locationName,
      row.paidPartnership,
      row.caption,
      row.videoDuration ? parseFloat(row.videoDuration) : null,
      row.commentsCount ? parseFloat(row.commentsCount) : null,
      row.mentions,
      row.isSponsored ? parseFloat(row.isSponsored) : null,
      row.timestamp,
      row.likesCount ? parseFloat(row.likesCount) : null
    ]);
  }
  
  console.log(`Imported ${rows.length} Instagram Hashtag records`);
}

async function main() {
  try {
    console.log('Starting M&S authentic dataset import...');
    
    await importTikTokOfficial();
    await importTikTokHashtag();
    await importYouTubeOfficial();
    await importYouTubeHashtag();
    await importInstagramOfficial();
    await importInstagramHashtag();
    
    console.log('All M&S datasets imported successfully!');
    
    // Verify import counts
    const tables = [
      'dataset_tiktok_M&S_official_cleaned.xlsx_csv',
      'dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv',
      'dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv',
      'dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv',
      'Insta_new_marksandspencer_cleaned.xlsx_csv',
      'Insta_new_mandshashtags_cleaned.xlsx_csv'
    ];
    
    console.log('\nVerification counts:');
    for (const table of tables) {
      const result = await pool.query(`SELECT COUNT(*) FROM "${table}"`);
      console.log(`${table}: ${result.rows[0].count} records`);
    }
    
  } catch (error) {
    console.error('Error importing datasets:', error);
  } finally {
    await pool.end();
  }
}

main();