import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function parseCSVLine(line) {
  const result = [];
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
      result.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }
  
  result.push(current.trim());
  return result.map(field => {
    if (field === '' || field === 'null') return null;
    if (field.startsWith('"') && field.endsWith('"')) {
      return field.slice(1, -1).replace(/""/g, '"');
    }
    return field;
  });
}

async function importDataset(file, table, columns, targetCount) {
  console.log(`Importing ${table} (target: ${targetCount})...`);
  
  const content = fs.readFileSync(file, 'utf-8');
  const lines = content.split('\n').filter(line => line.trim());
  
  let imported = 0;
  let currentRecord = '';
  let processingMultiline = false;
  
  for (let i = 1; i < lines.length && imported < targetCount; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    
    if (currentRecord) {
      currentRecord += '\n' + line;
    } else {
      currentRecord = line;
    }
    
    // Count quotes to check if record is complete
    let quoteCount = 0;
    for (const char of currentRecord) {
      if (char === '"') quoteCount++;
    }
    
    // Record is complete when quotes are balanced
    if (quoteCount % 2 === 0) {
      const fields = parseCSVLine(currentRecord);
      
      if (fields.length >= columns.length) {
        const values = fields.slice(0, columns.length);
        
        try {
          const placeholders = columns.map((_, idx) => `$${idx + 1}`).join(', ');
          await pool.query(
            `INSERT INTO "${table}" (${columns.join(', ')}) VALUES (${placeholders})`,
            values
          );
          imported++;
          
          if (imported % 100 === 0) {
            console.log(`  Progress: ${imported}/${targetCount}`);
          }
        } catch (error) {
          console.log(`  Error at record ${imported + 1}: ${error.message.substring(0, 100)}`);
        }
      }
      
      currentRecord = '';
    }
  }
  
  console.log(`  ✓ ${table}: ${imported} records imported`);
  return imported;
}

async function main() {
  try {
    console.log('=== Authentic Next Retail CSV Import ===');
    
    const datasets = [
      {
        file: 'attached_assets/tiktok_NEXT_Official_cleaned.xlsx_csv_1749116297478.csv',
        table: 'nextretail_tiktok_official',
        columns: ['unnamed_0', 'text', 'created_time', 'mentions_0', 'hashtags', 'share_count', 'comment_count', 'play_count', 'collect_count', 'digg_count'],
        target: 425
      },
      {
        file: 'attached_assets/dataset_tiktok-hashtag_NextRetail_cleaned.xlsx_csv_1749116307865.csv',
        table: 'nextretail_tiktok_hashtag',
        columns: ['text', 'created_time', 'hashtags', 'share_count', 'mentions', 'comment_count', 'play_count', 'collect_count', 'digg_count'],
        target: 947
      },
      {
        file: 'attached_assets/dataset_youtube-channel-scraper_NextRetail-Official 1_cleaned.xlsx_csv_1749116321778.csv',
        table: 'nextretail_youtube_official',
        columns: ['channel_total_views', 'url', 'duration', 'date', 'view_count', 'title', 'channel_total_videos', 'number_of_subscribers', 'channel_description', 'channel_joined_date', 'channel_location'],
        target: 598
      },
      {
        file: 'attached_assets/dataset_youtube-Hashtag_Next 1_cleaned.xlsx_csv_1749116331413.csv',
        table: 'nextretail_youtube_hashtag',
        columns: ['text', 'hashtags', 'duration', 'date', 'url', 'comments_count', 'view_count', 'title', 'number_of_subscribers', 'channel_name', 'likes'],
        target: 1044
      },
      {
        file: 'attached_assets/Insta_new_nextofficial_cleaned_2.xlsx_csv_1749116347205.csv',
        table: 'nextretail_instagram_official',
        columns: ['video_play_count', 'url', 'hashtags', 'video_view_count', 'video_duration', 'comments_count', 'mentions', 'caption', 'timestamp'],
        target: 173
      },
      {
        file: 'attached_assets/Insta_new_nexthashtags_cleaned.xlsx_csv_1749116356260.csv',
        table: 'nextretail_instagram_hashtag',
        columns: ['hashtags', 'url', 'location_name', 'video_view_count', 'caption', 'video_duration', 'comments_count', 'mentions', 'is_sponsored', 'timestamp', 'likes_count'],
        target: 1157
      }
    ];
    
    let totalImported = 0;
    
    for (const dataset of datasets) {
      const imported = await importDataset(dataset.file, dataset.table, dataset.columns, dataset.target);
      totalImported += imported;
    }
    
    console.log('\n=== IMPORT COMPLETE ===');
    console.log(`Total records imported: ${totalImported}`);
    console.log('Expected total: 4,344 records');
    console.log('Import completed with authentic CSV data');
    
  } catch (error) {
    console.error('Import failed:', error);
  } finally {
    await pool.end();
  }
}

main();