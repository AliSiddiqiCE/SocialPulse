import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function cleanValue(value) {
  if (value === undefined || value === null || value === '' || value === 'null') return null;
  return value.trim() || null;
}

function parseNumber(value) {
  const clean = cleanValue(value);
  if (!clean) return null;
  const num = parseFloat(clean.replace(/[^0-9.-]/g, ''));
  return isNaN(num) ? null : num;
}

// Parse CSV with proper handling of quoted fields containing commas
function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    
    if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      result.push(current);
      current = '';
    } else {
      current += char;
    }
  }
  result.push(current);
  return result;
}

async function importTikTokOfficial() {
  console.log('Importing TikTok Official with proper column alignment...');
  
  const content = fs.readFileSync('public/dataset_tiktok_M&S_official_cleaned.xlsx_csv.csv', 'utf8');
  const lines = content.split('\n').slice(1).filter(line => line.trim());
  
  await pool.query('DELETE FROM "dataset_tiktok_M&S_official_cleaned.xlsx_csv"');
  
  let count = 0;
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length >= 9) {
      try {
        await pool.query(`
          INSERT INTO "dataset_tiktok_M&S_official_cleaned.xlsx_csv" 
          (text, created_time, hashtags, "shareCount", mentions, "commentCount", "playCount", "diggCount", "collectCount")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, [
          cleanValue(fields[0]),    // text
          cleanValue(fields[1]),    // created_time  
          cleanValue(fields[2]),    // hashtags
          parseNumber(fields[3]),   // shareCount
          cleanValue(fields[4]),    // mentions
          parseNumber(fields[5]),   // commentCount
          parseNumber(fields[6]),   // playCount
          parseNumber(fields[7]),   // diggCount
          parseNumber(fields[8])    // collectCount
        ]);
        count++;
      } catch (err) {
        console.log(`Row ${count + 1} error: ${err.message}`);
      }
    }
  }
  console.log(`TikTok Official: ${count} rows imported`);
  return count;
}

async function importTikTokHashtag() {
  console.log('Importing TikTok Hashtag with proper column alignment...');
  
  const content = fs.readFileSync('public/dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv.csv', 'utf8');
  const lines = content.split('\n').slice(1).filter(line => line.trim());
  
  await pool.query('DELETE FROM "dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv"');
  
  let count = 0;
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length >= 9) {
      try {
        await pool.query(`
          INSERT INTO "dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv" 
          (text, created_time, hashtags, "shareCount", mentions, "commentCount", "playCount", "collectCount", "diggCount")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, [
          cleanValue(fields[0]),    // text
          cleanValue(fields[1]),    // created_time
          cleanValue(fields[2]),    // hashtags
          parseNumber(fields[3]),   // shareCount
          cleanValue(fields[4]),    // mentions
          parseNumber(fields[5]),   // commentCount
          parseNumber(fields[6]),   // playCount
          parseNumber(fields[7]),   // collectCount
          parseNumber(fields[8])    // diggCount
        ]);
        count++;
      } catch (err) {
        // Skip invalid rows
      }
    }
  }
  console.log(`TikTok Hashtag: ${count} rows imported`);
  return count;
}

async function importYouTubeOfficial() {
  console.log('Importing YouTube Official with proper column alignment...');
  
  const content = fs.readFileSync('public/dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv.csv', 'utf8');
  const lines = content.split('\n').slice(1).filter(line => line.trim());
  
  await pool.query('DELETE FROM "dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv"');
  
  let count = 0;
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length >= 9) {
      try {
        await pool.query(`
          INSERT INTO "dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv" 
          ("channelTotalViews", url, duration, date, title, "numberOfSubscribers", "channelDescription", "channelTotalVideos", "channelJoinedDate")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, [
          parseNumber(fields[0]),   // channelTotalViews
          cleanValue(fields[1]),    // url
          cleanValue(fields[2]),    // duration
          cleanValue(fields[3]),    // date
          cleanValue(fields[4]),    // title
          parseNumber(fields[5]),   // numberOfSubscribers
          cleanValue(fields[6]),    // channelDescription
          parseNumber(fields[7]),   // channelTotalVideos
          cleanValue(fields[8])     // channelJoinedDate
        ]);
        count++;
      } catch (err) {
        // Skip invalid rows
      }
    }
  }
  console.log(`YouTube Official: ${count} rows imported`);
  return count;
}

async function importYouTubeHashtag() {
  console.log('Importing YouTube Hashtag with proper column alignment...');
  
  const content = fs.readFileSync('public/dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv.csv', 'utf8');
  const lines = content.split('\n').slice(1).filter(line => line.trim());
  
  await pool.query('DELETE FROM "dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv"');
  
  let count = 0;
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length >= 11) {
      try {
        await pool.query(`
          INSERT INTO "dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv" 
          (text, hashtags, duration, date, url, "commentsCount", title, "numberOfSubscribers", "viewCount", "channelName", likes)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `, [
          cleanValue(fields[0]),    // text
          cleanValue(fields[1]),    // hashtags
          cleanValue(fields[2]),    // duration
          cleanValue(fields[3]),    // date
          cleanValue(fields[4]),    // url
          parseNumber(fields[5]),   // commentsCount
          cleanValue(fields[6]),    // title
          parseNumber(fields[7]),   // numberOfSubscribers
          parseNumber(fields[8]),   // viewCount
          cleanValue(fields[9]),    // channelName
          parseNumber(fields[10])   // likes
        ]);
        count++;
      } catch (err) {
        // Skip invalid rows
      }
    }
  }
  console.log(`YouTube Hashtag: ${count} rows imported`);
  return count;
}

async function importInstagramOfficial() {
  console.log('Importing Instagram Official with proper column alignment...');
  
  const content = fs.readFileSync('public/Insta_new_marksandspencer_cleaned.xlsx_csv.csv', 'utf8');
  const lines = content.split('\n').slice(1).filter(line => line.trim());
  
  await pool.query('DELETE FROM "Insta_new_marksandspencer_cleaned.xlsx_csv"');
  
  let count = 0;
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length >= 11) {
      try {
        await pool.query(`
          INSERT INTO "Insta_new_marksandspencer_cleaned.xlsx_csv" 
          ("videoPlayCount", hashtags, url, "locationName", "videoViewCount", "videoDuration", "commentsCount", mentions, caption, timestamp, "likesCount")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `, [
          parseNumber(fields[0]),   // videoPlayCount
          cleanValue(fields[1]),    // hashtags
          cleanValue(fields[2]),    // url
          cleanValue(fields[3]),    // locationName
          parseNumber(fields[4]),   // videoViewCount
          parseNumber(fields[5]),   // videoDuration
          parseNumber(fields[6]),   // commentsCount
          cleanValue(fields[7]),    // mentions
          cleanValue(fields[8]),    // caption
          cleanValue(fields[9]),    // timestamp
          parseNumber(fields[10])   // likesCount
        ]);
        count++;
      } catch (err) {
        // Skip invalid rows
      }
    }
  }
  console.log(`Instagram Official: ${count} rows imported`);
  return count;
}

async function importInstagramHashtag() {
  console.log('Importing Instagram Hashtag with proper column alignment...');
  
  const content = fs.readFileSync('public/Insta_new_mandshashtags_cleaned.xlsx_csv.csv', 'utf8');
  const lines = content.split('\n').slice(1).filter(line => line.trim());
  
  await pool.query('DELETE FROM "Insta_new_mandshashtags_cleaned.xlsx_csv"');
  
  let count = 0;
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length >= 11) {
      try {
        await pool.query(`
          INSERT INTO "Insta_new_mandshashtags_cleaned.xlsx_csv" 
          (hashtags, url, "locationName", "paidPartnership", caption, "videoDuration", "commentsCount", mentions, "isSponsored", timestamp, "likesCount")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `, [
          cleanValue(fields[0]),    // hashtags
          cleanValue(fields[1]),    // url
          cleanValue(fields[2]),    // locationName
          cleanValue(fields[3]),    // paidPartnership
          cleanValue(fields[4]),    // caption
          parseNumber(fields[5]),   // videoDuration
          parseNumber(fields[6]),   // commentsCount
          cleanValue(fields[7]),    // mentions
          parseNumber(fields[8]),   // isSponsored
          cleanValue(fields[9]),    // timestamp
          parseNumber(fields[10])   // likesCount
        ]);
        count++;
      } catch (err) {
        // Skip invalid rows
      }
    }
  }
  console.log(`Instagram Hashtag: ${count} rows imported`);
  return count;
}

async function main() {
  try {
    console.log('Starting complete authentic M&S dataset import with proper column alignment...');
    
    const results = {};
    
    results.tiktokOfficial = await importTikTokOfficial();
    results.tiktokHashtag = await importTikTokHashtag();
    results.youtubeOfficial = await importYouTubeOfficial();
    results.youtubeHashtag = await importYouTubeHashtag();
    results.instagramOfficial = await importInstagramOfficial();
    results.instagramHashtag = await importInstagramHashtag();
    
    console.log('\nFINAL IMPORT SUMMARY:');
    console.log('Dataset                | Records');
    console.log('-----------------------|--------');
    console.log(`TikTok Official        | ${results.tiktokOfficial}`);
    console.log(`TikTok Hashtag         | ${results.tiktokHashtag}`);
    console.log(`YouTube Official       | ${results.youtubeOfficial}`);
    console.log(`YouTube Hashtag        | ${results.youtubeHashtag}`);
    console.log(`Instagram Official     | ${results.instagramOfficial}`);
    console.log(`Instagram Hashtag      | ${results.instagramHashtag}`);
    
    const total = Object.values(results).reduce((sum, count) => sum + count, 0);
    console.log(`TOTAL                  | ${total}`);
    
    console.log('\nComplete authentic M&S dataset import finished with proper column alignment!');
    
  } catch (error) {
    console.error('Import error:', error);
  } finally {
    await pool.end();
  }
}

main();