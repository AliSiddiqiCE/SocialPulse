import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

async function resetTableIds(tableName) {
  console.log(`Resetting IDs for ${tableName}...`);
  
  try {
    // Create a temporary table with sequential IDs
    await pool.query(`CREATE TEMP TABLE temp_${tableName}_reset AS 
      SELECT *, row_number() OVER (ORDER BY id) as new_id 
      FROM "${tableName}"`);
    
    // Clear the original table
    await pool.query(`DELETE FROM "${tableName}"`);
    
    // Reset the sequence
    await pool.query(`ALTER SEQUENCE "${tableName}_id_seq" RESTART WITH 1`);
    
    // Insert data back with sequential IDs
    const columns = await getTableColumns(tableName);
    const dataColumns = columns.filter(col => col !== 'id').map(col => `"${col}"`).join(', ');
    
    await pool.query(`INSERT INTO "${tableName}" (${dataColumns}) 
      SELECT ${dataColumns} FROM temp_${tableName}_reset ORDER BY new_id`);
    
    // Get final count
    const result = await pool.query(`SELECT COUNT(*) as count FROM "${tableName}"`);
    const count = parseInt(result.rows[0].count);
    
    console.log(`${tableName}: ${count} rows with IDs 1-${count}`);
    return count;
    
  } catch (error) {
    console.log(`Error resetting ${tableName}: ${error.message}`);
    return 0;
  }
}

async function getTableColumns(tableName) {
  const result = await pool.query(`
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = $1 
    ORDER BY ordinal_position
  `, [tableName]);
  
  return result.rows.map(row => row.column_name);
}

async function main() {
  try {
    console.log('Resetting all table IDs to sequential order starting from 1...');
    
    const tables = [
      'dataset_tiktok_M&S_official_cleaned.xlsx_csv',
      'dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv',
      'dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv',
      'dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv',
      'Insta_new_marksandspencer_cleaned.xlsx_csv',
      'Insta_new_mandshashtags_cleaned.xlsx_csv'
    ];
    
    const results = {};
    
    for (const table of tables) {
      results[table] = await resetTableIds(table);
    }
    
    console.log('\nID RESET SUMMARY:');
    console.log('Dataset                           | Rows (ID 1-N)');
    console.log('----------------------------------|-------------');
    console.log(`TikTok Official                   | ${results['dataset_tiktok_M&S_official_cleaned.xlsx_csv']}`);
    console.log(`TikTok Hashtag                    | ${results['dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv']}`);
    console.log(`YouTube Official                  | ${results['dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv']}`);
    console.log(`YouTube Hashtag                   | ${results['dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv']}`);
    console.log(`Instagram Official                | ${results['Insta_new_marksandspencer_cleaned.xlsx_csv']}`);
    console.log(`Instagram Hashtag                 | ${results['Insta_new_mandshashtags_cleaned.xlsx_csv']}`);
    
    const total = Object.values(results).reduce((sum, count) => sum + count, 0);
    console.log(`TOTAL                             | ${total}`);
    
    console.log('\nAll table IDs reset to sequential order (1, 2, 3, ...)');
    
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await pool.end();
  }
}

main();