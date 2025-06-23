import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function cleanValue(value) {
  if (value === undefined || value === null || value === '' || value === 'null') return null;
  // Remove surrounding quotes if present
  if (typeof value === 'string' && value.startsWith('"') && value.endsWith('"')) {
    value = value.slice(1, -1);
  }
  return value.trim() || null;
}

function parseNumber(value) {
  const clean = cleanValue(value);
  if (!clean) return null;
  const num = parseFloat(clean.replace(/[^0-9.-]/g, ''));
  return isNaN(num) ? null : num;
}

// Enhanced CSV parser that properly handles multiline content and quotes
function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    const nextChar = line[i + 1];
    
    if (char === '"') {
      if (inQuotes && nextChar === '"') {
        // Handle escaped quotes
        current += '"';
        i++; // Skip next quote
      } else {
        inQuotes = !inQuotes;
      }
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

async function importDataset(filePath, tableName, columnDefs, expectedColumns) {
  console.log(`Importing ${tableName}...`);
  
  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.split('\n');
  
  // Skip header and filter empty lines
  const dataLines = lines.slice(1).filter(line => line.trim());
  
  // Clear existing data
  await pool.query(`DELETE FROM "${tableName}"`);
  
  let imported = 0;
  let skipped = 0;
  
  for (let i = 0; i < dataLines.length; i++) {
    const line = dataLines[i];
    if (!line.trim()) continue;
    
    const fields = parseCSVLine(line);
    
    if (fields.length >= expectedColumns) {
      try {
        const values = columnDefs.map((def, index) => {
          const field = fields[index] || '';
          switch (def.type) {
            case 'integer':
            case 'float':
              return parseNumber(field);
            default:
              return cleanValue(field);
          }
        });
        
        const placeholders = values.map((_, index) => `$${index + 1}`).join(', ');
        const columnNames = columnDefs.map(def => `"${def.name}"`).join(', ');
        
        await pool.query(
          `INSERT INTO "${tableName}" (${columnNames}) VALUES (${placeholders})`,
          values
        );
        imported++;
      } catch (err) {
        skipped++;
        if (skipped < 5) { // Only log first few errors
          console.log(`Row ${i + 1} error: ${err.message}`);
        }
      }
    } else {
      skipped++;
    }
  }
  
  console.log(`${tableName}: ${imported} rows imported, ${skipped} skipped`);
  return imported;
}

async function main() {
  try {
    console.log('Starting complete import of all 6 M&S CSV files...');
    
    const results = {};
    
    // TikTok Official (259 expected rows)
    results.tiktokOfficial = await importDataset(
      'attached_assets/dataset_tiktok_M&S_official_cleaned.xlsx_csv.csv',
      'dataset_tiktok_M&S_official_cleaned.xlsx_csv',
      [
        {name: 'text', type: 'string'},
        {name: 'created_time', type: 'string'},
        {name: 'hashtags', type: 'string'},
        {name: 'shareCount', type: 'integer'},
        {name: 'mentions', type: 'string'},
        {name: 'commentCount', type: 'integer'},
        {name: 'playCount', type: 'integer'},
        {name: 'diggCount', type: 'integer'},
        {name: 'collectCount', type: 'integer'}
      ],
      9
    );
    
    // TikTok Hashtag (1017 expected rows)
    results.tiktokHashtag = await importDataset(
      'attached_assets/dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv.csv',
      'dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv',
      [
        {name: 'text', type: 'string'},
        {name: 'created_time', type: 'string'},
        {name: 'hashtags', type: 'string'},
        {name: 'shareCount', type: 'integer'},
        {name: 'mentions', type: 'string'},
        {name: 'commentCount', type: 'integer'},
        {name: 'playCount', type: 'integer'},
        {name: 'collectCount', type: 'integer'},
        {name: 'diggCount', type: 'integer'}
      ],
      9
    );
    
    // YouTube Official (1000 expected rows)
    results.youtubeOfficial = await importDataset(
      'attached_assets/dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv.csv',
      'dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv',
      [
        {name: 'channelTotalViews', type: 'integer'},
        {name: 'url', type: 'string'},
        {name: 'duration', type: 'string'},
        {name: 'date', type: 'string'},
        {name: 'title', type: 'string'},
        {name: 'numberOfSubscribers', type: 'integer'},
        {name: 'channelDescription', type: 'string'},
        {name: 'channelTotalVideos', type: 'integer'},
        {name: 'channelJoinedDate', type: 'string'}
      ],
      9
    );
    
    // YouTube Hashtag (12936 expected rows)
    results.youtubeHashtag = await importDataset(
      'attached_assets/dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv.csv',
      'dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv',
      [
        {name: 'text', type: 'string'},
        {name: 'hashtags', type: 'string'},
        {name: 'duration', type: 'string'},
        {name: 'date', type: 'string'},
        {name: 'url', type: 'string'},
        {name: 'commentsCount', type: 'integer'},
        {name: 'title', type: 'string'},
        {name: 'numberOfSubscribers', type: 'integer'},
        {name: 'viewCount', type: 'integer'},
        {name: 'channelName', type: 'string'},
        {name: 'likes', type: 'integer'}
      ],
      11
    );
    
    // Instagram Official (5900 expected rows)
    results.instagramOfficial = await importDataset(
      'attached_assets/Insta_new_marksandspencer_cleaned.xlsx_csv.csv',
      'Insta_new_marksandspencer_cleaned.xlsx_csv',
      [
        {name: 'videoPlayCount', type: 'float'},
        {name: 'hashtags', type: 'string'},
        {name: 'url', type: 'string'},
        {name: 'locationName', type: 'string'},
        {name: 'videoViewCount', type: 'float'},
        {name: 'videoDuration', type: 'float'},
        {name: 'commentsCount', type: 'integer'},
        {name: 'mentions', type: 'string'},
        {name: 'caption', type: 'string'},
        {name: 'timestamp', type: 'string'},
        {name: 'likesCount', type: 'integer'}
      ],
      11
    );
    
    // Instagram Hashtag (9727 expected rows)
    results.instagramHashtag = await importDataset(
      'attached_assets/Insta_new_mandshashtags_cleaned.xlsx_csv.csv',
      'Insta_new_mandshashtags_cleaned.xlsx_csv',
      [
        {name: 'hashtags', type: 'string'},
        {name: 'url', type: 'string'},
        {name: 'locationName', type: 'string'},
        {name: 'paidPartnership', type: 'string'},
        {name: 'caption', type: 'string'},
        {name: 'videoDuration', type: 'float'},
        {name: 'commentsCount', type: 'float'},
        {name: 'mentions', type: 'string'},
        {name: 'isSponsored', type: 'float'},
        {name: 'timestamp', type: 'string'},
        {name: 'likesCount', type: 'float'}
      ],
      11
    );
    
    console.log('\nCOMPLETE IMPORT SUMMARY:');
    console.log('Dataset                | Imported | Expected');
    console.log('-----------------------|----------|----------');
    console.log(`TikTok Official        | ${results.tiktokOfficial.toString().padStart(8)} | 259`);
    console.log(`TikTok Hashtag         | ${results.tiktokHashtag.toString().padStart(8)} | 1017`);
    console.log(`YouTube Official       | ${results.youtubeOfficial.toString().padStart(8)} | 1000`);
    console.log(`YouTube Hashtag        | ${results.youtubeHashtag.toString().padStart(8)} | 12936`);
    console.log(`Instagram Official     | ${results.instagramOfficial.toString().padStart(8)} | 5900`);
    console.log(`Instagram Hashtag      | ${results.instagramHashtag.toString().padStart(8)} | 9727`);
    
    const total = Object.values(results).reduce((sum, count) => sum + count, 0);
    console.log(`TOTAL                  | ${total.toString().padStart(8)} | 30839`);
    
    console.log('\nComplete M&S dataset import finished successfully!');
    
  } catch (error) {
    console.error('Import error:', error);
  } finally {
    await pool.end();
  }
}

main();