import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function processCSVFile(filePath) {
  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.split('\n');
  const records = [];
  let currentRecord = '';
  let inQuotes = false;
  
  for (let i = 1; i < lines.length; i++) { // Skip header
    const line = lines[i];
    if (!line.trim()) continue;
    
    currentRecord += (currentRecord ? '\n' : '') + line;
    
    // Check if quotes are balanced
    const quotes = (currentRecord.match(/"/g) || []).length;
    inQuotes = quotes % 2 !== 0;
    
    if (!inQuotes) {
      const fields = parseRecord(currentRecord);
      if (fields.length > 0) {
        records.push(fields);
      }
      currentRecord = '';
    }
  }
  
  return records;
}

function parseRecord(record) {
  const fields = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < record.length; i++) {
    const char = record[i];
    
    if (char === '"') {
      if (inQuotes && record[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      fields.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }
  fields.push(current.trim());
  return fields;
}

function cleanValue(value) {
  if (!value || value === 'null' || value === '') return null;
  if (value.startsWith('"') && value.endsWith('"')) {
    value = value.slice(1, -1);
  }
  return value.replace(/""/g, '"') || null;
}

function parseNumber(value) {
  const clean = cleanValue(value);
  if (!clean) return null;
  const num = parseFloat(clean.replace(/[^0-9.-]/g, ''));
  return isNaN(num) ? null : num;
}

async function importLargeDataset(filePath, tableName, columns) {
  console.log(`Importing ${tableName} from ${filePath}...`);
  
  const records = processCSVFile(filePath);
  console.log(`Parsed ${records.length} records from CSV`);
  
  // Clear existing data
  await pool.query(`DELETE FROM "${tableName}"`);
  await pool.query(`ALTER SEQUENCE "${tableName}_id_seq" RESTART WITH 1`);
  
  let imported = 0;
  const batchSize = 50; // Smaller batches for large datasets
  
  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    const values = [];
    const placeholders = [];
    
    batch.forEach((record, batchIndex) => {
      if (record.length >= columns.length) {
        const rowPlaceholders = columns.map((_, colIndex) => 
          `$${batchIndex * columns.length + colIndex + 1}`
        ).join(', ');
        placeholders.push(`(${rowPlaceholders})`);
        
        const mappedValues = columns.map((col, idx) => {
          const field = record[idx] || '';
          return col.type === 'number' ? parseNumber(field) : cleanValue(field);
        });
        values.push(...mappedValues);
      }
    });
    
    if (placeholders.length > 0) {
      try {
        const columnNames = columns.map(col => `"${col.name}"`).join(', ');
        await pool.query(
          `INSERT INTO "${tableName}" (${columnNames}) VALUES ${placeholders.join(', ')}`,
          values
        );
        imported += placeholders.length;
      } catch (err) {
        console.log(`Batch ${Math.floor(i/batchSize) + 1} error: ${err.message}`);
      }
    }
  }
  
  console.log(`${tableName}: ${imported} records imported`);
  return imported;
}

async function main() {
  try {
    console.log('Importing remaining large M&S datasets...');
    
    // YouTube Hashtag (12,936 expected)
    const youtubeHashtagCount = await importLargeDataset(
      'attached_assets/dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv.csv',
      'dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv',
      [
        {name: 'text', type: 'string'},
        {name: 'hashtags', type: 'string'},
        {name: 'duration', type: 'string'},
        {name: 'date', type: 'string'},
        {name: 'url', type: 'string'},
        {name: 'commentsCount', type: 'number'},
        {name: 'title', type: 'string'},
        {name: 'numberOfSubscribers', type: 'number'},
        {name: 'viewCount', type: 'number'},
        {name: 'channelName', type: 'string'},
        {name: 'likes', type: 'number'}
      ]
    );
    
    // Instagram Official (5,900 expected) 
    const instagramOfficialCount = await importLargeDataset(
      'attached_assets/Insta_new_marksandspencer_cleaned.xlsx_csv.csv',
      'Insta_new_marksandspencer_cleaned.xlsx_csv',
      [
        {name: 'videoPlayCount', type: 'number'},
        {name: 'hashtags', type: 'string'},
        {name: 'url', type: 'string'},
        {name: 'locationName', type: 'string'},
        {name: 'videoViewCount', type: 'number'},
        {name: 'videoDuration', type: 'number'},
        {name: 'commentsCount', type: 'number'},
        {name: 'mentions', type: 'string'},
        {name: 'caption', type: 'string'},
        {name: 'timestamp', type: 'string'},
        {name: 'likesCount', type: 'number'}
      ]
    );
    
    // Instagram Hashtag (9,727 expected)
    const instagramHashtagCount = await importLargeDataset(
      'attached_assets/Insta_new_mandshashtags_cleaned.xlsx_csv.csv',
      'Insta_new_mandshashtags_cleaned.xlsx_csv',
      [
        {name: 'hashtags', type: 'string'},
        {name: 'url', type: 'string'},
        {name: 'locationName', type: 'string'},
        {name: 'paidPartnership', type: 'string'},
        {name: 'caption', type: 'string'},
        {name: 'videoDuration', type: 'number'},
        {name: 'commentsCount', type: 'number'},
        {name: 'mentions', type: 'string'},
        {name: 'isSponsored', type: 'number'},
        {name: 'timestamp', type: 'string'},
        {name: 'likesCount', type: 'number'}
      ]
    );
    
    // Reset IDs to sequential order
    console.log('\nResetting IDs to sequential order...');
    const tables = [
      'dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv',
      'Insta_new_marksandspencer_cleaned.xlsx_csv',
      'Insta_new_mandshashtags_cleaned.xlsx_csv'
    ];
    
    for (const table of tables) {
      try {
        await pool.query(`
          WITH numbered_rows AS (
            SELECT *, ROW_NUMBER() OVER (ORDER BY id) as new_id 
            FROM "${table}"
          )
          UPDATE "${table}" 
          SET id = numbered_rows.new_id
          FROM numbered_rows 
          WHERE "${table}".id = numbered_rows.id
        `);
      } catch (err) {
        console.log(`ID reset error for ${table}: ${err.message}`);
      }
    }
    
    console.log('\nLARGE DATASET IMPORT RESULTS:');
    console.log(`YouTube Hashtag: ${youtubeHashtagCount} records`);
    console.log(`Instagram Official: ${instagramOfficialCount} records`);
    console.log(`Instagram Hashtag: ${instagramHashtagCount} records`);
    
    const total = youtubeHashtagCount + instagramOfficialCount + instagramHashtagCount;
    console.log(`Additional records imported: ${total}`);
    
    console.log('\nLarge dataset import completed!');
    
  } catch (error) {
    console.error('Import error:', error);
  } finally {
    await pool.end();
  }
}

main();