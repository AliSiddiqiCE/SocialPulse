import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

async function processDataset(csvFile, tableName, targetCount) {
  console.log(`${tableName}: ${targetCount} records`);
  
  const content = fs.readFileSync(csvFile, 'utf-8');
  const lines = content.split('\n');
  const headers = lines[0].split(',').map(h => h.trim());
  
  // Create table with exact column names
  await pool.query(`DROP TABLE IF EXISTS "${tableName}"`);
  const cols = headers.map(h => `"${h}" TEXT`).join(', ');
  await pool.query(`CREATE TABLE "${tableName}" (${cols})`);
  
  let imported = 0;
  const quotedCols = headers.map(h => `"${h}"`).join(', ');
  
  for (let i = 1; i < lines.length && imported < targetCount; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    
    const fields = line.split(',').map(f => f.trim() || null);
    
    if (fields.length >= headers.length) {
      try {
        const values = fields.slice(0, headers.length);
        const placeholders = values.map((_, idx) => `$${idx + 1}`).join(', ');
        
        await pool.query(
          `INSERT INTO "${tableName}" (${quotedCols}) VALUES (${placeholders})`,
          values
        );
        imported++;
      } catch (e) {
        // Skip invalid
      }
    }
  }
  
  console.log(`  ${imported} imported`);
  return imported;
}

async function main() {
  let total = 0;
  
  // TikTok Official - "Unnamed: 0,text,created_time,mentions/0,hashtags,shareCount,commentCount,playCount,collectCount,diggCount"
  total += await processDataset('attached_assets/tiktok_NEXT_Official_cleaned.xlsx_csv_1749116297478.csv', 'nextretail_tiktok_official_new', 425);
  
  // TikTok Hashtag - "text,created_time,hashtags,shareCount,mentions,commentCount,playCount,collectCount,diggCount"
  total += await processDataset('attached_assets/dataset_tiktok-hashtag_NextRetail_cleaned.xlsx_csv_1749116307865.csv', 'nextretail_tiktok_hashtag_new', 947);
  
  // YouTube Official - "channelTotalViews,url,duration,date,viewCount,title,channelTotalVideos,numberOfSubscribers,channelDescription,channelJoinedDate,channelLocation"
  total += await processDataset('attached_assets/dataset_youtube-channel-scraper_NextRetail-Official 1_cleaned.xlsx_csv_1749116321778.csv', 'nextretail_youtube_official_new', 598);
  
  // YouTube Hashtag - "text,hashtags,duration,date,url,commentsCount,viewCount,title,numberOfSubscribers,channelName,likes"
  total += await processDataset('attached_assets/dataset_youtube-Hashtag_Next 1_cleaned.xlsx_csv_1749116331413.csv', 'nextretail_youtube_hashtag_new', 1044);
  
  // Instagram Official - "videoPlayCount,url,hashtags,videoViewCount,videoDuration,commentsCount,mentions,caption,timestamp"
  total += await processDataset('attached_assets/Insta_new_nextofficial_cleaned_2.xlsx_csv_1749116347205.csv', 'nextretail_instagram_official_new', 173);
  
  // Instagram Hashtag - "hashtags,url,locationName,videoViewCount,caption,videoDuration,commentsCount,mentions,isSponsored,timestamp,likesCount"
  total += await processDataset('attached_assets/Insta_new_nexthashtags_cleaned.xlsx_csv_1749116356260.csv', 'nextretail_instagram_hashtag_new', 1157);
  
  console.log(`Total: ${total}/4344`);
  await pool.end();
}

main();