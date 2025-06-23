import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function cleanValue(value) {
  if (value === undefined || value === null || value === '' || value === 'null') return null;
  if (typeof value === 'string') {
    // Remove surrounding quotes
    if (value.startsWith('"') && value.endsWith('"')) {
      value = value.slice(1, -1);
    }
    // Handle escaped quotes
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

async function batchImport(tableName, columns, data) {
  const batchSize = 100;
  let totalImported = 0;
  
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
      totalImported += batch.length;
    } catch (err) {
      console.log(`Batch error in ${tableName}: ${err.message}`);
    }
  }
  
  return totalImported;
}

async function processTikTokOfficial() {
  console.log('Processing TikTok Official...');
  
  const content = fs.readFileSync('attached_assets/dataset_tiktok_M&S_official_cleaned.xlsx_csv.csv', 'utf8');
  let lines = content.split('\n');
  
  // Remove header
  lines = lines.slice(1);
  
  const processedData = [];
  
  for (let i = 0; i < lines.length; i++) {
    let line = lines[i];
    if (!line.trim()) continue;
    
    // Handle multiline quoted content
    while (line.includes('"') && (line.match(/"/g) || []).length % 2 !== 0) {
      if (i + 1 < lines.length) {
        i++;
        line += '\n' + lines[i];
      } else {
        break;
      }
    }
    
    // Split by comma, but respect quotes
    const fields = [];
    let current = '';
    let inQuotes = false;
    
    for (let j = 0; j < line.length; j++) {
      const char = line[j];
      if (char === '"') {
        inQuotes = !inQuotes;
        current += char;
      } else if (char === ',' && !inQuotes) {
        fields.push(current);
        current = '';
      } else {
        current += char;
      }
    }
    fields.push(current);
    
    if (fields.length >= 9) {
      processedData.push([
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
    }
  }
  
  await pool.query('DELETE FROM "dataset_tiktok_M&S_official_cleaned.xlsx_csv"');
  
  const imported = await batchImport(
    'dataset_tiktok_M&S_official_cleaned.xlsx_csv',
    ['text', 'created_time', 'hashtags', 'shareCount', 'mentions', 'commentCount', 'playCount', 'diggCount', 'collectCount'],
    processedData
  );
  
  console.log(`TikTok Official: ${imported} rows imported`);
  return imported;
}

async function processInstagramOfficial() {
  console.log('Processing Instagram Official...');
  
  const content = fs.readFileSync('attached_assets/Insta_new_marksandspencer_cleaned.xlsx_csv.csv', 'utf8');
  let lines = content.split('\n').slice(1);
  
  const processedData = [];
  
  for (let i = 0; i < lines.length; i++) {
    let line = lines[i];
    if (!line.trim()) continue;
    
    // Handle multiline content
    while (line.includes('"') && (line.match(/"/g) || []).length % 2 !== 0) {
      if (i + 1 < lines.length) {
        i++;
        line += '\n' + lines[i];
      } else {
        break;
      }
    }
    
    const fields = [];
    let current = '';
    let inQuotes = false;
    
    for (let j = 0; j < line.length; j++) {
      const char = line[j];
      if (char === '"') {
        inQuotes = !inQuotes;
        current += char;
      } else if (char === ',' && !inQuotes) {
        fields.push(current);
        current = '';
      } else {
        current += char;
      }
    }
    fields.push(current);
    
    if (fields.length >= 11) {
      processedData.push([
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
    }
  }
  
  await pool.query('DELETE FROM "Insta_new_marksandspencer_cleaned.xlsx_csv"');
  
  const imported = await batchImport(
    'Insta_new_marksandspencer_cleaned.xlsx_csv',
    ['videoPlayCount', 'hashtags', 'url', 'locationName', 'videoViewCount', 'videoDuration', 'commentsCount', 'mentions', 'caption', 'timestamp', 'likesCount'],
    processedData
  );
  
  console.log(`Instagram Official: ${imported} rows imported`);
  return imported;
}

async function processInstagramHashtag() {
  console.log('Processing Instagram Hashtag...');
  
  const content = fs.readFileSync('attached_assets/Insta_new_mandshashtags_cleaned.xlsx_csv.csv', 'utf8');
  let lines = content.split('\n').slice(1);
  
  const processedData = [];
  
  for (let i = 0; i < lines.length; i++) {
    let line = lines[i];
    if (!line.trim()) continue;
    
    // Handle multiline content
    while (line.includes('"') && (line.match(/"/g) || []).length % 2 !== 0) {
      if (i + 1 < lines.length) {
        i++;
        line += '\n' + lines[i];
      } else {
        break;
      }
    }
    
    const fields = [];
    let current = '';
    let inQuotes = false;
    
    for (let j = 0; j < line.length; j++) {
      const char = line[j];
      if (char === '"') {
        inQuotes = !inQuotes;
        current += char;
      } else if (char === ',' && !inQuotes) {
        fields.push(current);
        current = '';
      } else {
        current += char;
      }
    }
    fields.push(current);
    
    if (fields.length >= 11) {
      processedData.push([
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
    }
  }
  
  await pool.query('DELETE FROM "Insta_new_mandshashtags_cleaned.xlsx_csv"');
  
  const imported = await batchImport(
    'Insta_new_mandshashtags_cleaned.xlsx_csv',
    ['hashtags', 'url', 'locationName', 'paidPartnership', 'caption', 'videoDuration', 'commentsCount', 'mentions', 'isSponsored', 'timestamp', 'likesCount'],
    processedData
  );
  
  console.log(`Instagram Hashtag: ${imported} rows imported`);
  return imported;
}

async function main() {
  try {
    console.log('Starting complete batch import of all M&S CSV files...');
    
    const tiktokOfficial = await processTikTokOfficial();
    const instagramOfficial = await processInstagramOfficial();
    const instagramHashtag = await processInstagramHashtag();
    
    console.log('\nBATCH IMPORT RESULTS:');
    console.log(`TikTok Official: ${tiktokOfficial} records`);
    console.log(`Instagram Official: ${instagramOfficial} records`);
    console.log(`Instagram Hashtag: ${instagramHashtag} records`);
    console.log(`Total: ${tiktokOfficial + instagramOfficial + instagramHashtag} authentic records`);
    
  } catch (error) {
    console.error('Import error:', error);
  } finally {
    await pool.end();
  }
}

main();