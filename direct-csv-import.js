import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

async function importTikTokOfficial() {
  console.log('Importing TikTok Official dataset...');
  
  // Clear existing data
  await pool.query('DELETE FROM "dataset_tiktok_M&S_official_cleaned.xlsx_csv"');
  
  // Read file content
  const content = fs.readFileSync('attached_assets/dataset_tiktok_M&S_official_cleaned.xlsx_csv.csv', 'utf8');
  const lines = content.split('\n');
  
  let imported = 0;
  
  // Process each line manually to handle multiline content
  for (let i = 1; i < lines.length; i++) {
    let line = lines[i].trim();
    if (!line) continue;
    
    // Handle multiline quoted content
    while (line.includes('"') && (line.match(/"/g) || []).length % 2 !== 0 && i + 1 < lines.length) {
      i++;
      line += '\n' + lines[i];
    }
    
    // Simple split for basic cases, proper parsing for complex ones
    let fields;
    if (line.includes('"')) {
      // Use CSV parsing for complex lines
      fields = parseComplexCSVLine(line);
    } else {
      // Simple split for lines without quotes
      fields = line.split(',');
    }
    
    if (fields.length >= 9) {
      try {
        await pool.query(`
          INSERT INTO "dataset_tiktok_M&S_official_cleaned.xlsx_csv" 
          (text, created_time, hashtags, "shareCount", mentions, "commentCount", "playCount", "diggCount", "collectCount")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, [
          cleanField(fields[0]),
          cleanField(fields[1]),
          cleanField(fields[2]),
          parseNum(fields[3]),
          cleanField(fields[4]),
          parseNum(fields[5]),
          parseNum(fields[6]),
          parseNum(fields[7]),
          parseNum(fields[8])
        ]);
        imported++;
      } catch (err) {
        // Skip problematic rows
      }
    }
  }
  
  console.log(`TikTok Official: ${imported} rows imported`);
  return imported;
}

function parseComplexCSVLine(line) {
  const result = [];
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
      result.push(current);
      current = '';
    } else {
      current += char;
    }
  }
  result.push(current);
  return result;
}

function cleanField(value) {
  if (!value || value === 'null' || value === '') return null;
  return value.trim();
}

function parseNum(value) {
  const clean = cleanField(value);
  if (!clean) return null;
  const num = parseFloat(clean.replace(/[^0-9.-]/g, ''));
  return isNaN(num) ? null : num;
}

async function importInstagramOfficial() {
  console.log('Importing Instagram Official dataset...');
  
  await pool.query('DELETE FROM "Insta_new_marksandspencer_cleaned.xlsx_csv"');
  
  const content = fs.readFileSync('attached_assets/Insta_new_marksandspencer_cleaned.xlsx_csv.csv', 'utf8');
  const lines = content.split('\n');
  
  let imported = 0;
  
  for (let i = 1; i < lines.length; i++) {
    let line = lines[i].trim();
    if (!line) continue;
    
    // Handle multiline content
    while (line.includes('"') && (line.match(/"/g) || []).length % 2 !== 0 && i + 1 < lines.length) {
      i++;
      line += '\n' + lines[i];
    }
    
    const fields = line.includes('"') ? parseComplexCSVLine(line) : line.split(',');
    
    if (fields.length >= 11) {
      try {
        await pool.query(`
          INSERT INTO "Insta_new_marksandspencer_cleaned.xlsx_csv" 
          ("videoPlayCount", hashtags, url, "locationName", "videoViewCount", "videoDuration", "commentsCount", mentions, caption, timestamp, "likesCount")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `, [
          parseNum(fields[0]), cleanField(fields[1]), cleanField(fields[2]),
          cleanField(fields[3]), parseNum(fields[4]), parseNum(fields[5]),
          parseNum(fields[6]), cleanField(fields[7]), cleanField(fields[8]),
          cleanField(fields[9]), parseNum(fields[10])
        ]);
        imported++;
      } catch (err) {
        // Skip problematic rows
      }
    }
  }
  
  console.log(`Instagram Official: ${imported} rows imported`);
  return imported;
}

async function importInstagramHashtag() {
  console.log('Importing Instagram Hashtag dataset...');
  
  await pool.query('DELETE FROM "Insta_new_mandshashtags_cleaned.xlsx_csv"');
  
  const content = fs.readFileSync('attached_assets/Insta_new_mandshashtags_cleaned.xlsx_csv.csv', 'utf8');
  const lines = content.split('\n');
  
  let imported = 0;
  
  for (let i = 1; i < lines.length; i++) {
    let line = lines[i].trim();
    if (!line) continue;
    
    // Handle multiline content
    while (line.includes('"') && (line.match(/"/g) || []).length % 2 !== 0 && i + 1 < lines.length) {
      i++;
      line += '\n' + lines[i];
    }
    
    const fields = line.includes('"') ? parseComplexCSVLine(line) : line.split(',');
    
    if (fields.length >= 11) {
      try {
        await pool.query(`
          INSERT INTO "Insta_new_mandshashtags_cleaned.xlsx_csv" 
          (hashtags, url, "locationName", "paidPartnership", caption, "videoDuration", "commentsCount", mentions, "isSponsored", timestamp, "likesCount")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `, [
          cleanField(fields[0]), cleanField(fields[1]), cleanField(fields[2]),
          cleanField(fields[3]), cleanField(fields[4]), parseNum(fields[5]),
          parseNum(fields[6]), cleanField(fields[7]), parseNum(fields[8]),
          cleanField(fields[9]), parseNum(fields[10])
        ]);
        imported++;
      } catch (err) {
        // Skip problematic rows
      }
    }
  }
  
  console.log(`Instagram Hashtag: ${imported} rows imported`);
  return imported;
}

async function main() {
  try {
    console.log('Starting complete authentic dataset import...');
    
    const tikTokOfficial = await importTikTokOfficial();
    const instagramOfficial = await importInstagramOfficial();
    const instagramHashtag = await importInstagramHashtag();
    
    console.log('\nIMPORT RESULTS:');
    console.log(`TikTok Official: ${tikTokOfficial} records`);
    console.log(`Instagram Official: ${instagramOfficial} records`);
    console.log(`Instagram Hashtag: ${instagramHashtag} records`);
    console.log(`Total: ${tikTokOfficial + instagramOfficial + instagramHashtag} authentic records`);
    
  } catch (error) {
    console.error('Import error:', error);
  } finally {
    await pool.end();
  }
}

main();