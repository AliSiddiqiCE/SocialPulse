import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function cleanCSVField(field) {
  if (!field) return null;
  field = field.toString().trim();
  if (field.startsWith('"') && field.endsWith('"')) {
    field = field.slice(1, -1).replace(/""/g, '"');
  }
  if (field === '' || field === 'null' || field === 'NULL') return null;
  return field;
}

function safeParseInt(value) {
  const cleaned = cleanCSVField(value);
  if (!cleaned) return null;
  const num = parseInt(cleaned.replace(/[^\d-]/g, ''));
  return isNaN(num) ? null : num;
}

function safeParseFloat(value) {
  const cleaned = cleanCSVField(value);
  if (!cleaned) return null;
  const num = parseFloat(cleaned.replace(/[^\d.-]/g, ''));
  return isNaN(num) ? null : num;
}

async function batchInsert(tableName, columns, data, batchSize = 500) {
  let inserted = 0;
  
  for (let i = 0; i < data.length; i += batchSize) {
    const batch = data.slice(i, i + batchSize);
    const values = [];
    const placeholders = [];
    
    batch.forEach((row, idx) => {
      const rowPlaceholders = columns.map((_, colIdx) => `$${values.length + colIdx + 1}`);
      placeholders.push(`(${rowPlaceholders.join(', ')})`);
      values.push(...row);
    });
    
    try {
      await pool.query(
        `INSERT INTO "${tableName}" (${columns.join(', ')}) VALUES ${placeholders.join(', ')}`,
        values
      );
      inserted += batch.length;
    } catch (error) {
      // Fallback to individual inserts
      for (const row of batch) {
        try {
          const rowPlaceholders = columns.map((_, idx) => `$${idx + 1}`);
          await pool.query(
            `INSERT INTO "${tableName}" (${columns.join(', ')}) VALUES (${rowPlaceholders.join(', ')})`,
            row
          );
          inserted++;
        } catch (rowError) {
          // Skip invalid rows
        }
      }
    }
  }
  
  return inserted;
}

async function processCSVFile(filePath, tableName, columnDefs) {
  console.log(`Processing ${tableName}...`);
  
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter(line => line.trim());
  
  // Skip header
  const dataLines = lines.slice(1);
  console.log(`  Found ${dataLines.length} data lines`);
  
  const processedData = [];
  
  for (const line of dataLines) {
    // Simple comma split for basic parsing
    const rawFields = line.split(',');
    
    if (rawFields.length >= columnDefs.length) {
      const processedRow = columnDefs.map((def, idx) => {
        const rawValue = rawFields[idx];
        
        switch (def.type) {
          case 'integer':
            return safeParseInt(rawValue);
          case 'decimal':
            return safeParseFloat(rawValue);
          case 'boolean':
            return cleanCSVField(rawValue) === 'True';
          default:
            return cleanCSVField(rawValue);
        }
      });
      
      processedData.push(processedRow);
    }
  }
  
  console.log(`  Processed ${processedData.length} valid records`);
  
  const columnNames = columnDefs.map(def => def.name);
  const inserted = await batchInsert(tableName, columnNames, processedData);
  
  console.log(`  ✓ ${tableName}: ${inserted} records imported`);
  return inserted;
}

async function main() {
  try {
    console.log('=== Fast Next Retail Import ===');
    
    let totalImported = 0;
    
    // Process all 6 datasets
    const datasets = [
      {
        file: 'attached_assets/tiktok_NEXT_Official_cleaned.xlsx_csv_1749116297478.csv',
        table: 'nextretail_tiktok_official',
        columns: [
          { name: 'unnamed_0', type: 'string' },
          { name: 'text', type: 'string' },
          { name: 'created_time', type: 'string' },
          { name: 'mentions_0', type: 'string' },
          { name: 'hashtags', type: 'string' },
          { name: 'share_count', type: 'string' },
          { name: 'comment_count', type: 'string' },
          { name: 'play_count', type: 'string' },
          { name: 'collect_count', type: 'string' },
          { name: 'digg_count', type: 'string' }
        ]
      },
      {
        file: 'attached_assets/dataset_tiktok-hashtag_NextRetail_cleaned.xlsx_csv_1749116307865.csv',
        table: 'nextretail_tiktok_hashtag',
        columns: [
          { name: 'text', type: 'string' },
          { name: 'created_time', type: 'string' },
          { name: 'hashtags', type: 'string' },
          { name: 'share_count', type: 'string' },
          { name: 'mentions', type: 'string' },
          { name: 'comment_count', type: 'string' },
          { name: 'play_count', type: 'string' },
          { name: 'collect_count', type: 'string' },
          { name: 'digg_count', type: 'string' }
        ]
      },
      {
        file: 'attached_assets/dataset_youtube-channel-scraper_NextRetail-Official 1_cleaned.xlsx_csv_1749116321778.csv',
        table: 'nextretail_youtube_official',
        columns: [
          { name: 'channel_total_views', type: 'string' },
          { name: 'url', type: 'string' },
          { name: 'duration', type: 'string' },
          { name: 'date', type: 'string' },
          { name: 'view_count', type: 'string' },
          { name: 'title', type: 'string' },
          { name: 'channel_total_videos', type: 'string' },
          { name: 'number_of_subscribers', type: 'string' },
          { name: 'channel_description', type: 'string' },
          { name: 'channel_joined_date', type: 'string' },
          { name: 'channel_location', type: 'string' }
        ]
      },
      {
        file: 'attached_assets/dataset_youtube-Hashtag_Next 1_cleaned.xlsx_csv_1749116331413.csv',
        table: 'nextretail_youtube_hashtag',
        columns: [
          { name: 'text', type: 'string' },
          { name: 'hashtags', type: 'string' },
          { name: 'duration', type: 'string' },
          { name: 'date', type: 'string' },
          { name: 'url', type: 'string' },
          { name: 'comments_count', type: 'string' },
          { name: 'view_count', type: 'string' },
          { name: 'title', type: 'string' },
          { name: 'number_of_subscribers', type: 'string' },
          { name: 'channel_name', type: 'string' },
          { name: 'likes', type: 'string' }
        ]
      },
      {
        file: 'attached_assets/Insta_new_nextofficial_cleaned_2.xlsx_csv_1749116347205.csv',
        table: 'nextretail_instagram_official',
        columns: [
          { name: 'video_play_count', type: 'string' },
          { name: 'url', type: 'string' },
          { name: 'hashtags', type: 'string' },
          { name: 'video_view_count', type: 'string' },
          { name: 'video_duration', type: 'string' },
          { name: 'comments_count', type: 'string' },
          { name: 'mentions', type: 'string' },
          { name: 'caption', type: 'string' },
          { name: 'timestamp', type: 'string' }
        ]
      },
      {
        file: 'attached_assets/Insta_new_nexthashtags_cleaned.xlsx_csv_1749116356260.csv',
        table: 'nextretail_instagram_hashtag',
        columns: [
          { name: 'hashtags', type: 'string' },
          { name: 'url', type: 'string' },
          { name: 'location_name', type: 'string' },
          { name: 'video_view_count', type: 'string' },
          { name: 'caption', type: 'string' },
          { name: 'video_duration', type: 'string' },
          { name: 'comments_count', type: 'string' },
          { name: 'mentions', type: 'string' },
          { name: 'is_sponsored', type: 'string' },
          { name: 'timestamp', type: 'string' },
          { name: 'likes_count', type: 'string' }
        ]
      }
    ];
    
    for (const dataset of datasets) {
      const imported = await processCSVFile(dataset.file, dataset.table, dataset.columns);
      totalImported += imported;
    }
    
    console.log('\n=== IMPORT COMPLETE ===');
    console.log(`Total imported: ${totalImported}`);
    console.log('All Next Retail CSV data imported');
    
  } catch (error) {
    console.error('Import error:', error);
  } finally {
    await pool.end();
  }
}

main();