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
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      result.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }
  result.push(current.trim());
  return result;
}

async function bulkImportTikTokOfficial() {
  console.log('Importing TikTok Official M&S data...');
  
  const content = fs.readFileSync('attached_assets/dataset_tiktok_M&S_official_cleaned.xlsx_csv.csv', 'utf8');
  const lines = content.split('\n').slice(1).filter(line => line.trim());
  
  await pool.query('DELETE FROM "dataset_tiktok_M&S_official_cleaned.xlsx_csv"');
  
  const batchSize = 50;
  let totalInserted = 0;
  
  for (let i = 0; i < lines.length; i += batchSize) {
    const batch = lines.slice(i, i + batchSize);
    const values = [];
    const placeholders = [];
    
    batch.forEach((line, index) => {
      const parts = parseCSVLine(line);
      if (parts.length >= 9) {
        const baseIndex = index * 9;
        placeholders.push(`($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${baseIndex + 4}, $${baseIndex + 5}, $${baseIndex + 6}, $${baseIndex + 7}, $${baseIndex + 8}, $${baseIndex + 9})`);
        
        values.push(
          parts[0]?.replace(/"/g, '') || null,
          parts[1]?.replace(/"/g, '') || null,
          parts[2]?.replace(/"/g, '') || null,
          parts[3] && parts[3] !== '' ? parseInt(parts[3]) : null,
          parts[4]?.replace(/"/g, '') || null,
          parts[5] && parts[5] !== '' ? parseInt(parts[5]) : null,
          parts[6] && parts[6] !== '' ? parseInt(parts[6]) : null,
          parts[7] && parts[7] !== '' ? parseInt(parts[7]) : null,
          parts[8] && parts[8] !== '' ? parseInt(parts[8]) : null
        );
      }
    });
    
    if (placeholders.length > 0) {
      try {
        await pool.query(`
          INSERT INTO "dataset_tiktok_M&S_official_cleaned.xlsx_csv" 
          (text, created_time, hashtags, "shareCount", mentions, "commentCount", "playCount", "diggCount", "collectCount")
          VALUES ${placeholders.join(', ')}
        `, values);
        totalInserted += placeholders.length;
      } catch (err) {
        console.log(`Batch error at position ${i}:`, err.message);
      }
    }
  }
  
  console.log(`Imported ${totalInserted} TikTok Official records`);
}

async function bulkImportTikTokHashtag() {
  console.log('Importing TikTok Hashtag M&S data...');
  
  const content = fs.readFileSync('attached_assets/dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv.csv', 'utf8');
  const lines = content.split('\n').slice(1).filter(line => line.trim());
  
  await pool.query('DELETE FROM "dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv"');
  
  let totalInserted = 0;
  
  for (const line of lines) {
    const parts = parseCSVLine(line);
    if (parts.length >= 10) {
      try {
        await pool.query(`
          INSERT INTO "dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv" 
          (text, created_time, hashtags, "shareCount", mentions, "commentCount", "playCount", "collectCount", "diggCount")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, [
          parts[0]?.replace(/"/g, '') || null,
          parts[1]?.replace(/"/g, '') || null,
          parts[2]?.replace(/"/g, '') || null,
          parts[3] && parts[3] !== '' ? parseInt(parts[3]) : null,
          parts[4]?.replace(/"/g, '') || null,
          parts[5] && parts[5] !== '' ? parseInt(parts[5]) : null,
          parts[6] && parts[6] !== '' ? parseInt(parts[6]) : null,
          parts[7] && parts[7] !== '' ? parseInt(parts[7]) : null,
          parts[8] && parts[8] !== '' ? parseInt(parts[8]) : null
        ]);
        totalInserted++;
      } catch (err) {
        // Skip malformed rows
      }
    }
  }
  
  console.log(`Imported ${totalInserted} TikTok Hashtag records`);
}

async function main() {
  try {
    await bulkImportTikTokOfficial();
    await bulkImportTikTokHashtag();
    
    // Verify counts
    const tiktokOfficial = await pool.query('SELECT COUNT(*) FROM "dataset_tiktok_M&S_official_cleaned.xlsx_csv"');
    const tiktokHashtag = await pool.query('SELECT COUNT(*) FROM "dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv"');
    
    console.log('\nImport verification:');
    console.log(`TikTok Official: ${tiktokOfficial.rows[0].count} records`);
    console.log(`TikTok Hashtag: ${tiktokHashtag.rows[0].count} records`);
    
  } catch (error) {
    console.error('Import error:', error);
  } finally {
    await pool.end();
  }
}

main();