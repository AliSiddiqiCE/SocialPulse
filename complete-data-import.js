import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function cleanValue(value) {
  if (value === undefined || value === null || value === '' || value === 'null') return null;
  if (typeof value === 'string') {
    if (value.startsWith('"') && value.endsWith('"')) {
      value = value.slice(1, -1);
    }
    value = value.replace(/""/g, '"');
  }
  return value.trim() || null;
}

function parseNumber(value) {
  const clean = cleanValue(value);
  if (!clean) return null;
  const num = parseFloat(clean.replace(/[^0-9.-]/g, ''));
  return isNaN(num) ? null : num;
}

// Enhanced CSV parser that handles complex multiline content
function parseCSVContent(content) {
  const lines = content.split('\n');
  const rows = [];
  let currentRow = '';
  let inQuotes = false;
  
  for (let i = 1; i < lines.length; i++) { // Skip header
    const line = lines[i];
    if (!line.trim()) continue;
    
    currentRow += (currentRow ? '\n' : '') + line;
    
    // Count quotes to determine if we're still inside a quoted field
    const quoteCount = (currentRow.match(/"/g) || []).length;
    inQuotes = quoteCount % 2 !== 0;
    
    if (!inQuotes) {
      // Complete row found, parse it
      const fields = parseCSVLine(currentRow);
      if (fields.length > 0) {
        rows.push(fields);
      }
      currentRow = '';
    }
  }
  
  return rows;
}

function parseCSVLine(line) {
  const fields = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    
    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++; // Skip escaped quote
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      fields.push(current);
      current = '';
    } else {
      current += char;
    }
  }
  fields.push(current);
  return fields;
}

async function batchInsert(tableName, columns, data, batchSize = 50) {
  let totalInserted = 0;
  
  for (let i = 0; i < data.length; i += batchSize) {
    const batch = data.slice(i, i + batchSize);
    const values = [];
    const placeholders = [];
    
    batch.forEach((row, batchIndex) => {
      const rowPlaceholders = columns.map((_, colIndex) => 
        `$${batchIndex * columns.length + colIndex + 1}`
      ).join(', ');
      placeholders.push(`(${rowPlaceholders})`);
      values.push(...row);
    });
    
    try {
      const columnNames = columns.map(col => `"${col}"`).join(', ');
      await pool.query(
        `INSERT INTO "${tableName}" (${columnNames}) VALUES ${placeholders.join(', ')}`,
        values
      );
      totalInserted += batch.length;
    } catch (err) {
      console.log(`Batch error: ${err.message}`);
    }
  }
  
  return totalInserted;
}

async function importAllDataComplete() {
  console.log('Starting complete import of all authentic M&S data...');
  
  // TikTok Official - Full Import
  console.log('Processing TikTok Official (all 259 rows)...');
  const tikTokOfficialContent = fs.readFileSync('public/tiktok_NEXT_Official_cleaned.xlsx_csv.csv', 'utf8');
  const tikTokOfficialRows = parseCSVContent(tikTokOfficialContent);
  
  await pool.query('DELETE FROM "dataset_tiktok_M&S_official_cleaned.xlsx_csv"');
  await pool.query('ALTER SEQUENCE "dataset_tiktok_M&S_official_cleaned.xlsx_csv_id_seq" RESTART WITH 1');
  
  const tikTokOfficialData = tikTokOfficialRows.map(fields => [
    cleanValue(fields[0]),    // text
    cleanValue(fields[1]),    // created_time
    cleanValue(fields[2]),    // hashtags
    parseNumber(fields[3]),   // shareCount
    cleanValue(fields[4]),    // mentions
    parseNumber(fields[5]),   // commentCount
    parseNumber(fields[6]),   // playCount
    parseNumber(fields[7]),   // diggCount
    parseNumber(fields[8])    // collectCount
  ]).filter(row => row.some(field => field !== null));
  
  const tikTokOfficialCount = await batchInsert(
    'dataset_tiktok_M&S_official_cleaned.xlsx_csv',
    ['text', 'created_time', 'hashtags', 'shareCount', 'mentions', 'commentCount', 'playCount', 'diggCount', 'collectCount'],
    tikTokOfficialData
  );
  
  // YouTube Hashtag - Full Import  
  console.log('Processing YouTube Hashtag (all 12,936 rows)...');
  const youTubeHashtagContent = fs.readFileSync('public/dataset_youtube-Hashtag_Next 1_cleaned.xlsx_csv.csv', 'utf8');
  const youTubeHashtagRows = parseCSVContent(youTubeHashtagContent);
  
  await pool.query('DELETE FROM "dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv"');
  await pool.query('ALTER SEQUENCE "dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv_id_seq" RESTART WITH 1');
  
  const youTubeHashtagData = youTubeHashtagRows.map(fields => [
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
  ]).filter(row => row.some(field => field !== null));
  
  const youTubeHashtagCount = await batchInsert(
    'dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv',
    ['text', 'hashtags', 'duration', 'date', 'url', 'commentsCount', 'title', 'numberOfSubscribers', 'viewCount', 'channelName', 'likes'],
    youTubeHashtagData
  );
  
  // Instagram Official - Full Import
  console.log('Processing Instagram Official (all 5,900 rows)...');
  const instagramOfficialContent = fs.readFileSync('public/Insta_new_nextofficial_cleaned_2.xlsx_csv.csv', 'utf8');
  const instagramOfficialRows = parseCSVContent(instagramOfficialContent);
  
  await pool.query('DELETE FROM "Insta_new_marksandspencer_cleaned.xlsx_csv"');
  await pool.query('ALTER SEQUENCE "Insta_new_marksandspencer_cleaned.xlsx_csv_id_seq" RESTART WITH 1');
  
  const instagramOfficialData = instagramOfficialRows.map(fields => [
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
  ]).filter(row => row.some(field => field !== null));
  
  const instagramOfficialCount = await batchInsert(
    'Insta_new_marksandspencer_cleaned.xlsx_csv',
    ['videoPlayCount', 'hashtags', 'url', 'locationName', 'videoViewCount', 'videoDuration', 'commentsCount', 'mentions', 'caption', 'timestamp', 'likesCount'],
    instagramOfficialData
  );
  
  // Instagram Hashtag - Full Import
  console.log('Processing Instagram Hashtag (all 9,727 rows)...');
  const instagramHashtagContent = fs.readFileSync('public/Insta_new_nexthashtags_cleaned.xlsx_csv.csv', 'utf8');
  const instagramHashtagRows = parseCSVContent(instagramHashtagContent);
  
  await pool.query('DELETE FROM "Insta_new_mandshashtags_cleaned.xlsx_csv"');
  await pool.query('ALTER SEQUENCE "Insta_new_mandshashtags_cleaned.xlsx_csv_id_seq" RESTART WITH 1');
  
  const instagramHashtagData = instagramHashtagRows.map(fields => [
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
  ]).filter(row => row.some(field => field !== null));
  
  const instagramHashtagCount = await batchInsert(
    'Insta_new_mandshashtags_cleaned.xlsx_csv',
    ['hashtags', 'url', 'locationName', 'paidPartnership', 'caption', 'videoDuration', 'commentsCount', 'mentions', 'isSponsored', 'timestamp', 'likesCount'],
    instagramHashtagData
  );
  
  console.log('\nCOMPLETE IMPORT RESULTS:');
  console.log(`TikTok Official: ${tikTokOfficialCount} rows (expected ~259)`);
  console.log(`YouTube Hashtag: ${youTubeHashtagCount} rows (expected ~12,936)`);
  console.log(`Instagram Official: ${instagramOfficialCount} rows (expected ~5,900)`);
  console.log(`Instagram Hashtag: ${instagramHashtagCount} rows (expected ~9,727)`);
  
  const total = tikTokOfficialCount + youTubeHashtagCount + instagramOfficialCount + instagramHashtagCount;
  console.log(`Total: ${total} authentic records imported`);
  
  return {
    tikTokOfficial: tikTokOfficialCount,
    youTubeHashtag: youTubeHashtagCount,
    instagramOfficial: instagramOfficialCount,
    instagramHashtag: instagramHashtagCount
  };
}

async function main() {
  try {
    const results = await importAllDataComplete();
    console.log('\nComplete authentic M&S dataset import finished!');
  } catch (error) {
    console.error('Import error:', error);
  } finally {
    await pool.end();
  }
}

main();