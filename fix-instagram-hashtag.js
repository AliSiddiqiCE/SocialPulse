import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function cleanField(field) {
  if (!field || field === '' || field === 'null') return null;
  return field.replace(/^"(.*)"$/, '$1').trim();
}

function safeParseFloat(value) {
  if (!value || value === '' || value === 'null') return null;
  const cleaned = value.replace(/[^0-9.-]/g, '');
  const num = parseFloat(cleaned);
  return isNaN(num) ? null : num;
}

async function importInstagramHashtag() {
  console.log('Importing Instagram Hashtag dataset...');
  
  const content = fs.readFileSync('attached_assets/Insta_new_mandshashtags_cleaned.xlsx_csv.csv', 'utf8');
  const lines = content.split('\n').slice(1).filter(line => line.trim());
  
  await pool.query('DELETE FROM "Insta_new_mandshashtags_cleaned.xlsx_csv"');
  
  let imported = 0;
  for (const line of lines) {
    const fields = line.split(',');
    
    if (fields.length >= 11) {
      try {
        await pool.query(`
          INSERT INTO "Insta_new_mandshashtags_cleaned.xlsx_csv" 
          (hashtags, url, "locationName", "paidPartnership", caption, "videoDuration", "commentsCount", mentions, "isSponsored", timestamp, "likesCount")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `, [
          cleanField(fields[0]),
          cleanField(fields[1]),
          cleanField(fields[2]),
          cleanField(fields[3]),
          cleanField(fields[4]),
          safeParseFloat(fields[5]),
          safeParseFloat(fields[6]),
          cleanField(fields[7]),
          safeParseFloat(fields[8]),
          cleanField(fields[9]),
          safeParseFloat(fields[10])
        ]);
        imported++;
      } catch (err) {
        // Skip problematic rows
      }
    }
  }
  
  console.log(`Instagram Hashtag: ${imported} rows imported`);
}

async function main() {
  try {
    await importInstagramHashtag();
    
    // Verify final counts
    const verification = await pool.query(`
      SELECT COUNT(*) as count FROM "Insta_new_mandshashtags_cleaned.xlsx_csv"
    `);
    
    console.log(`Final count: ${verification.rows[0].count} records`);
    
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await pool.end();
  }
}

main();