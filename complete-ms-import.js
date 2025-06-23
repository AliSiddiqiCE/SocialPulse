import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function parseCSVRow(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  let i = 0;
  
  while (i < line.length) {
    const char = line[i];
    const nextChar = line[i + 1];
    
    if (char === '"') {
      if (inQuotes && nextChar === '"') {
        // Handle escaped quotes
        current += '"';
        i += 2;
        continue;
      } else {
        inQuotes = !inQuotes;
      }
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
    // Remove surrounding quotes if present
    if (field.startsWith('"') && field.endsWith('"')) {
      return field.slice(1, -1);
    }
    return field;
  });
}

async function importTikTokOfficial() {
  console.log('Importing complete TikTok Official M&S dataset...');
  
  const content = fs.readFileSync('attached_assets/dataset_tiktok_M&S_official_cleaned.xlsx_csv.csv', 'utf8');
  const lines = content.split('\n').slice(1); // Skip header
  
  await pool.query('DELETE FROM "dataset_tiktok_M&S_official_cleaned.xlsx_csv"');
  
  let imported = 0;
  for (const line of lines) {
    if (!line.trim()) continue;
    
    const fields = parseCSVRow(line);
    if (fields.length >= 9) {
      try {
        await pool.query(`
          INSERT INTO "dataset_tiktok_M&S_official_cleaned.xlsx_csv" 
          (text, created_time, hashtags, "shareCount", mentions, "commentCount", "playCount", "diggCount", "collectCount")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, [
          fields[0] || null,
          fields[1] || null, 
          fields[2] || null,
          fields[3] && fields[3] !== '' ? parseInt(fields[3]) : null,
          fields[4] || null,
          fields[5] && fields[5] !== '' ? parseInt(fields[5]) : null,
          fields[6] && fields[6] !== '' ? parseInt(fields[6]) : null,
          fields[7] && fields[7] !== '' ? parseInt(fields[7]) : null,
          fields[8] && fields[8] !== '' ? parseInt(fields[8]) : null
        ]);
        imported++;
      } catch (err) {
        console.log(`TikTok Official row ${imported + 1} error:`, err.message);
      }
    }
  }
  console.log(`✓ TikTok Official: ${imported} records imported`);
}

async function importTikTokHashtag() {
  console.log('Importing complete TikTok Hashtag M&S dataset...');
  
  const content = fs.readFileSync('attached_assets/dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv.csv', 'utf8');
  const lines = content.split('\n').slice(1); // Skip header
  
  await pool.query('DELETE FROM "dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv"');
  
  let imported = 0;
  for (const line of lines) {
    if (!line.trim()) continue;
    
    const fields = parseCSVRow(line);
    if (fields.length >= 10) {
      try {
        await pool.query(`
          INSERT INTO "dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv" 
          (text, created_time, hashtags, "shareCount", mentions, "commentCount", "playCount", "collectCount", "diggCount")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, [
          fields[0] || null,
          fields[1] || null,
          fields[2] || null,
          fields[3] && fields[3] !== '' ? parseInt(fields[3]) : null,
          fields[4] || null,
          fields[5] && fields[5] !== '' ? parseInt(fields[5]) : null,
          fields[6] && fields[6] !== '' ? parseInt(fields[6]) : null,
          fields[7] && fields[7] !== '' ? parseInt(fields[7]) : null,
          fields[8] && fields[8] !== '' ? parseInt(fields[8]) : null
        ]);
        imported++;
      } catch (err) {
        console.log(`TikTok Hashtag row ${imported + 1} error:`, err.message);
      }
    }
  }
  console.log(`✓ TikTok Hashtag: ${imported} records imported`);
}

async function importYouTubeOfficial() {
  console.log('Importing complete YouTube Official M&S dataset...');
  
  const content = fs.readFileSync('attached_assets/dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv.csv', 'utf8');
  const lines = content.split('\n').slice(1); // Skip header
  
  await pool.query('DELETE FROM "dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv"');
  
  let imported = 0;
  for (const line of lines) {
    if (!line.trim()) continue;
    
    const fields = parseCSVRow(line);
    if (fields.length >= 9) {
      try {
        await pool.query(`
          INSERT INTO "dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv" 
          ("channelTotalViews", url, duration, date, title, "numberOfSubscribers", "channelDescription", "channelTotalVideos", "channelJoinedDate")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, [
          fields[0] && fields[0] !== '' ? parseInt(fields[0]) : null,
          fields[1] || null,
          fields[2] || null,
          fields[3] || null,
          fields[4] || null,
          fields[5] && fields[5] !== '' ? parseInt(fields[5]) : null,
          fields[6] || null,
          fields[7] && fields[7] !== '' ? parseInt(fields[7]) : null,
          fields[8] || null
        ]);
        imported++;
      } catch (err) {
        console.log(`YouTube Official row ${imported + 1} error:`, err.message);
      }
    }
  }
  console.log(`✓ YouTube Official: ${imported} records imported`);
}

async function importYouTubeHashtag() {
  console.log('Importing complete YouTube Hashtag M&S dataset...');
  
  const content = fs.readFileSync('attached_assets/dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv.csv', 'utf8');
  const lines = content.split('\n').slice(1); // Skip header
  
  await pool.query('DELETE FROM "dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv"');
  
  let imported = 0;
  for (const line of lines) {
    if (!line.trim()) continue;
    
    const fields = parseCSVRow(line);
    if (fields.length >= 11) {
      try {
        await pool.query(`
          INSERT INTO "dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv" 
          (text, hashtags, duration, date, url, "commentsCount", title, "numberOfSubscribers", "viewCount", "channelName", likes)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `, [
          fields[0] || null,
          fields[1] || null,
          fields[2] || null,
          fields[3] || null,
          fields[4] || null,
          fields[5] && fields[5] !== '' ? parseInt(fields[5]) : null,
          fields[6] || null,
          fields[7] && fields[7] !== '' ? parseInt(fields[7]) : null,
          fields[8] && fields[8] !== '' ? parseInt(fields[8]) : null,
          fields[9] || null,
          fields[10] && fields[10] !== '' ? parseInt(fields[10]) : null
        ]);
        imported++;
      } catch (err) {
        console.log(`YouTube Hashtag row ${imported + 1} error:`, err.message);
      }
    }
  }
  console.log(`✓ YouTube Hashtag: ${imported} records imported`);
}

async function importInstagramOfficial() {
  console.log('Importing complete Instagram Official M&S dataset...');
  
  const content = fs.readFileSync('attached_assets/Insta_new_marksandspencer_cleaned.xlsx_csv.csv', 'utf8');
  const lines = content.split('\n').slice(1); // Skip header
  
  await pool.query('DELETE FROM "Insta_new_marksandspencer_cleaned.xlsx_csv"');
  
  let imported = 0;
  for (const line of lines) {
    if (!line.trim()) continue;
    
    const fields = parseCSVRow(line);
    if (fields.length >= 11) {
      try {
        await pool.query(`
          INSERT INTO "Insta_new_marksandspencer_cleaned.xlsx_csv" 
          ("videoPlayCount", hashtags, url, "locationName", "videoViewCount", "videoDuration", "commentsCount", mentions, caption, timestamp, "likesCount")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `, [
          fields[0] && fields[0] !== '' ? parseFloat(fields[0]) : null,
          fields[1] || null,
          fields[2] || null,
          fields[3] || null,
          fields[4] && fields[4] !== '' ? parseFloat(fields[4]) : null,
          fields[5] && fields[5] !== '' ? parseFloat(fields[5]) : null,
          fields[6] && fields[6] !== '' ? parseInt(fields[6]) : null,
          fields[7] || null,
          fields[8] || null,
          fields[9] || null,
          fields[10] && fields[10] !== '' ? parseInt(fields[10]) : null
        ]);
        imported++;
      } catch (err) {
        console.log(`Instagram Official row ${imported + 1} error:`, err.message);
      }
    }
  }
  console.log(`✓ Instagram Official: ${imported} records imported`);
}

async function importInstagramHashtag() {
  console.log('Importing complete Instagram Hashtag M&S dataset...');
  
  const content = fs.readFileSync('attached_assets/Insta_new_mandshashtags_cleaned.xlsx_csv.csv', 'utf8');
  const lines = content.split('\n').slice(1); // Skip header
  
  await pool.query('DELETE FROM "Insta_new_mandshashtags_cleaned.xlsx_csv"');
  
  let imported = 0;
  for (const line of lines) {
    if (!line.trim()) continue;
    
    const fields = parseCSVRow(line);
    if (fields.length >= 11) {
      try {
        await pool.query(`
          INSERT INTO "Insta_new_mandshashtags_cleaned.xlsx_csv" 
          (hashtags, url, "locationName", "paidPartnership", caption, "videoDuration", "commentsCount", mentions, "isSponsored", timestamp, "likesCount")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `, [
          fields[0] || null,
          fields[1] || null,
          fields[2] || null,
          fields[3] || null,
          fields[4] || null,
          fields[5] && fields[5] !== '' ? parseFloat(fields[5]) : null,
          fields[6] && fields[6] !== '' ? parseFloat(fields[6]) : null,
          fields[7] || null,
          fields[8] && fields[8] !== '' ? parseFloat(fields[8]) : null,
          fields[9] || null,
          fields[10] && fields[10] !== '' ? parseFloat(fields[10]) : null
        ]);
        imported++;
      } catch (err) {
        console.log(`Instagram Hashtag row ${imported + 1} error:`, err.message);
      }
    }
  }
  console.log(`✓ Instagram Hashtag: ${imported} records imported`);
}

async function main() {
  try {
    console.log('Starting complete M&S authentic dataset import...');
    console.log('Expected rows: TikTok Official(259), TikTok Hashtag(1017), YouTube Official(1000), YouTube Hashtag(12936), Instagram Official(5900), Instagram Hashtag(9727)');
    
    await importTikTokOfficial();
    await importTikTokHashtag();
    await importYouTubeOfficial();
    await importYouTubeHashtag();
    await importInstagramOfficial();
    await importInstagramHashtag();
    
    // Final verification
    const verification = await pool.query(`
      SELECT 
        'TikTok Official' as dataset,
        COUNT(*) as actual_count,
        259 as expected_count
      FROM "dataset_tiktok_M&S_official_cleaned.xlsx_csv"
      
      UNION ALL
      
      SELECT 
        'TikTok Hashtag' as dataset,
        COUNT(*) as actual_count,
        1017 as expected_count
      FROM "dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv"
      
      UNION ALL
      
      SELECT 
        'YouTube Official' as dataset,
        COUNT(*) as actual_count,
        1000 as expected_count
      FROM "dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv"
      
      UNION ALL
      
      SELECT 
        'YouTube Hashtag' as dataset,
        COUNT(*) as actual_count,
        12936 as expected_count
      FROM "dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv"
      
      UNION ALL
      
      SELECT 
        'Instagram Official' as dataset,
        COUNT(*) as actual_count,
        5900 as expected_count
      FROM "Insta_new_marksandspencer_cleaned.xlsx_csv"
      
      UNION ALL
      
      SELECT 
        'Instagram Hashtag' as dataset,
        COUNT(*) as actual_count,
        9727 as expected_count
      FROM "Insta_new_mandshashtags_cleaned.xlsx_csv"
      
      ORDER BY dataset
    `);
    
    console.log('\n📊 FINAL IMPORT VERIFICATION:');
    console.log('Dataset | Actual | Expected | Status');
    console.log('--------|--------|----------|--------');
    
    let totalActual = 0;
    let totalExpected = 0;
    
    verification.rows.forEach(row => {
      const status = row.actual_count == row.expected_count ? '✓' : '✗';
      console.log(`${row.dataset.padEnd(16)} | ${String(row.actual_count).padStart(6)} | ${String(row.expected_count).padStart(8)} | ${status}`);
      totalActual += parseInt(row.actual_count);
      totalExpected += parseInt(row.expected_count);
    });
    
    console.log('--------|--------|----------|--------');
    console.log(`${'TOTAL'.padEnd(16)} | ${String(totalActual).padStart(6)} | ${String(totalExpected).padStart(8)} | ${totalActual === totalExpected ? '✓' : '✗'}`);
    
    console.log('\n🎉 Complete M&S authentic dataset import finished!');
    
  } catch (error) {
    console.error('❌ Import error:', error);
  } finally {
    await pool.end();
  }
}

main();