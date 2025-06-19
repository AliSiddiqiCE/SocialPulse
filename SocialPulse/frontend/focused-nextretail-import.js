import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function parseSimpleCSV(content) {
  const lines = content.split('\n');
  const records = [];
  
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    
    const fields = line.split(',').map(field => {
      field = field.trim();
      if (field.startsWith('"') && field.endsWith('"')) {
        field = field.slice(1, -1);
      }
      return field || null;
    });
    
    if (fields.length > 3) {
      records.push(fields);
    }
  }
  
  return records;
}

async function importSingleDataset(csvFile, tableName, columns, targetCount) {
  console.log(`Importing ${tableName} (${targetCount} records)...`);
  
  try {
    const content = fs.readFileSync(csvFile, 'utf-8');
    const records = parseSimpleCSV(content);
    
    console.log(`  Found ${records.length} records in CSV`);
    
    await pool.query(`DELETE FROM "${tableName}"`);
    
    let inserted = 0;
    const cleanColumns = columns.map(c => c.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase());
    
    for (let i = 0; i < Math.min(records.length, targetCount); i++) {
      const record = records[i];
      if (record.length >= columns.length) {
        try {
          const values = record.slice(0, columns.length);
          const placeholders = values.map((_, idx) => `$${idx + 1}`).join(', ');
          
          await pool.query(
            `INSERT INTO "${tableName}" (${cleanColumns.map(c => `"${c}"`).join(', ')}) VALUES (${placeholders})`,
            values
          );
          inserted++;
        } catch (error) {
          // Skip invalid records
        }
      }
    }
    
    console.log(`  ✓ ${inserted} records imported`);
    return inserted;
    
  } catch (error) {
    console.error(`  Error: ${error.message}`);
    return 0;
  }
}

async function main() {
  console.log('=== Focused Next Retail Import ===\n');
  
  let totalImported = 0;
  
  // Import each dataset one by one
  const datasets = [
    {
      file: 'attached_assets/tiktok_NEXT_Official_cleaned.xlsx_csv_1749116297478.csv',
      table: 'tiktok_NEXT_Official_cleaned_xlsx_csv_1749116297478',
      columns: ['unnamed_0', 'text', 'created_time', 'mentions_0', 'hashtags', 'share_count', 'comment_count', 'play_count', 'collect_count', 'digg_count'],
      target: 425
    },
    {
      file: 'attached_assets/dataset_tiktok-hashtag_NextRetail_cleaned.xlsx_csv_1749116307865.csv',
      table: 'dataset_tiktok_hashtag_NextRetail_cleaned_xlsx_csv_174911630786',
      columns: ['text', 'created_time', 'hashtags', 'share_count', 'mentions', 'comment_count', 'play_count', 'collect_count', 'digg_count'],
      target: 947
    },
    {
      file: 'attached_assets/dataset_youtube-channel-scraper_NextRetail-Official 1_cleaned.xlsx_csv_1749116321778.csv',
      table: 'dataset_youtube_channel_scraper_NextRetail_Official_1_cleaned_x',
      columns: ['channel_total_views', 'url', 'duration', 'date', 'view_count', 'title', 'channel_total_videos', 'number_of_subscribers', 'channel_description', 'channel_joined_date', 'channel_location'],
      target: 598
    },
    {
      file: 'attached_assets/dataset_youtube-Hashtag_Next 1_cleaned.xlsx_csv_1749116331413.csv',
      table: 'dataset_youtube_Hashtag_Next_1_cleaned_xlsx_csv_1749116331413',
      columns: ['text', 'hashtags', 'duration', 'date', 'url', 'comments_count', 'view_count', 'title', 'number_of_subscribers', 'channel_name', 'likes'],
      target: 1044
    },
    {
      file: 'attached_assets/Insta_new_nextofficial_cleaned_2.xlsx_csv_1749116347205.csv',
      table: 'Insta_new_nextofficial_cleaned_2_xlsx_csv_1749116347205',
      columns: ['video_play_count', 'url', 'hashtags', 'video_view_count', 'video_duration', 'comments_count', 'mentions', 'caption', 'timestamp'],
      target: 173
    },
    {
      file: 'attached_assets/Insta_new_nexthashtags_cleaned.xlsx_csv_1749116356260.csv',
      table: 'Insta_new_nexthashtags_cleaned_xlsx_csv_1749116356260',
      columns: ['hashtags', 'url', 'location_name', 'video_view_count', 'caption', 'video_duration', 'comments_count', 'mentions', 'is_sponsored', 'timestamp', 'likes_count'],
      target: 1157
    }
  ];
  
  for (const dataset of datasets) {
    const imported = await importSingleDataset(dataset.file, dataset.table, dataset.columns, dataset.target);
    totalImported += imported;
  }
  
  console.log(`\nFinal total: ${totalImported} records imported`);
  console.log('Target: 4,344 authentic records');
  
  await pool.end();
}

main();