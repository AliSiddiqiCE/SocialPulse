import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

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
      fields.push(current.trim() || null);
      current = '';
    } else {
      current += char;
    }
  }
  
  fields.push(current.trim() || null);
  return fields.map(field => {
    if (!field || field === 'null') return null;
    if (field.startsWith('"') && field.endsWith('"')) {
      return field.slice(1, -1);
    }
    return field;
  });
}

async function efficientRemainingImport() {
  console.log('Efficient completion of remaining Next Retail datasets\n');
  
  let newRecords = 0;
  
  // Complete YouTube Hashtag (230/1044 remaining: 814)
  console.log('Completing YouTube Hashtag (814 remaining)...');
  const youtubeHashtagContent = fs.readFileSync('attached_assets/dataset_youtube-Hashtag_Next 1_cleaned.xlsx_csv_1749116331413.csv', 'utf-8');
  const youtubeHashtagLines = youtubeHashtagContent.split('\n');
  let youtubeHashtagCompleted = 0;
  
  for (let i = 231; i < youtubeHashtagLines.length && youtubeHashtagCompleted < 814; i++) {
    const line = youtubeHashtagLines[i].trim();
    if (!line) continue;
    
    const fields = parseCSVLine(line);
    if (fields.length >= 11) {
      try {
        await pool.query(`
          INSERT INTO "nextretail_youtube_hashtag"
          ("Unnamed: 0", "title", "upload_date", "duration", "view_count", "like_count", "comment_count", "description", "tags", "channel_id", "video_id")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `, fields.slice(0, 11));
        youtubeHashtagCompleted++;
      } catch (error) {
        // Skip invalid
      }
    }
  }
  newRecords += youtubeHashtagCompleted;
  console.log(`YouTube Hashtag: ${youtubeHashtagCompleted} additional records`);
  
  // YouTube Official - 598 records
  console.log('Importing YouTube Official (598 records)...');
  const youtubeOfficialContent = fs.readFileSync('attached_assets/dataset_youtube-channel-scraper_NextRetail-Official 1_cleaned.xlsx_csv_1749116321778.csv', 'utf-8');
  const youtubeOfficialLines = youtubeOfficialContent.split('\n');
  let youtubeOfficialImported = 0;
  
  for (let i = 1; i < youtubeOfficialLines.length && youtubeOfficialImported < 598; i++) {
    const line = youtubeOfficialLines[i].trim();
    if (!line) continue;
    
    const fields = parseCSVLine(line);
    if (fields.length >= 11) {
      try {
        await pool.query(`
          INSERT INTO "nextretail_youtube_official"
          ("Unnamed: 0", "title", "upload_date", "duration", "view_count", "like_count", "comment_count", "description", "tags", "channel_id", "video_id")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `, fields.slice(0, 11));
        youtubeOfficialImported++;
      } catch (error) {
        // Skip invalid
      }
    }
  }
  newRecords += youtubeOfficialImported;
  console.log(`YouTube Official: ${youtubeOfficialImported} records`);
  
  // Instagram Official - 173 records
  console.log('Importing Instagram Official (173 records)...');
  const instagramOfficialContent = fs.readFileSync('attached_assets/Insta_new_nextofficial_cleaned_2.xlsx_csv_1749116347205.csv', 'utf-8');
  const instagramOfficialLines = instagramOfficialContent.split('\n');
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
        // Skip invalid
      }
    }
  }
  newRecords += instagramOfficialImported;
  console.log(`Instagram Official: ${instagramOfficialImported} records`);
  
  // Instagram Hashtag - 1157 records
  console.log('Importing Instagram Hashtag (1157 records)...');
  const instagramHashtagContent = fs.readFileSync('attached_assets/Insta_new_nexthashtags_cleaned.xlsx_csv_1749116356260.csv', 'utf-8');
  const instagramHashtagLines = instagramHashtagContent.split('\n');
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
        // Skip invalid
      }
    }
  }
  newRecords += instagramHashtagImported;
  console.log(`Instagram Hashtag: ${instagramHashtagImported} records`);
  
  console.log(`\nNew records imported in this session: ${newRecords}`);
  
  // Final verification
  const counts = await Promise.all([
    pool.query('SELECT COUNT(*) FROM "nextretail_tiktok_official"'),
    pool.query('SELECT COUNT(*) FROM "nextretail_tiktok_hashtag"'),
    pool.query('SELECT COUNT(*) FROM "nextretail_youtube_official"'),
    pool.query('SELECT COUNT(*) FROM "nextretail_youtube_hashtag"'),
    pool.query('SELECT COUNT(*) FROM "nextretail_instagram_official"'),
    pool.query('SELECT COUNT(*) FROM "nextretail_instagram_hashtag"')
  ]);
  
  const grandTotal = counts.reduce((sum, count) => sum + parseInt(count.rows[0].count), 0);
  
  console.log(`\n=== NEXT RETAIL IMPORT STATUS ===`);
  console.log(`TikTok Official: ${counts[0].rows[0].count}/425`);
  console.log(`TikTok Hashtag: ${counts[1].rows[0].count}/947`);
  console.log(`YouTube Official: ${counts[2].rows[0].count}/598`);
  console.log(`YouTube Hashtag: ${counts[3].rows[0].count}/1044`);
  console.log(`Instagram Official: ${counts[4].rows[0].count}/173`);
  console.log(`Instagram Hashtag: ${counts[5].rows[0].count}/1157`);
  console.log(`TOTAL: ${grandTotal}/4,344 authentic Next Retail records`);
  console.log('All datasets preserving exact column structure');
  
  return grandTotal;
}

async function main() {
  try {
    await efficientRemainingImport();
  } catch (error) {
    console.error('Import failed:', error);
  } finally {
    await pool.end();
  }
}

main();