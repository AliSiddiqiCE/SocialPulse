import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// Target files for M&S data import
const TARGET_FILES = [
  'attached_assets/dataset_tiktok_M&S_official_cleaned.xlsx_csv.csv',
  'attached_assets/dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv.csv',
  'attached_assets/dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv.csv',
  'attached_assets/dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv.csv',
  'attached_assets/Insta_new_marksandspencer_cleaned.xlsx_csv.csv',
  'attached_assets/Insta_new_mandshashtags_cleaned.xlsx_csv.csv'
];

// Enhanced CSV parser that handles all edge cases
function parseCompleteCSV(filePath) {
  console.log(`Reading and parsing: ${filePath}`);
  
  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.split('\n');
  
  if (lines.length < 2) {
    throw new Error(`File ${filePath} has insufficient data`);
  }
  
  // Parse header
  const header = parseCSVLine(lines[0]);
  console.log(`Header columns (${header.length}): ${header.join(', ')}`);
  
  // Parse all data rows
  const records = [];
  let currentRecord = '';
  let lineIndex = 1; // Start from line 1 (skip header)
  let errors = [];
  
  while (lineIndex < lines.length) {
    const line = lines[lineIndex];
    
    if (!line.trim() && !currentRecord) {
      lineIndex++;
      continue;
    }
    
    currentRecord += (currentRecord ? '\n' : '') + line;
    
    // Check if record is complete (balanced quotes)
    const quotes = (currentRecord.match(/"/g) || []).length;
    const isComplete = quotes % 2 === 0;
    
    if (isComplete && currentRecord.trim()) {
      try {
        const fields = parseCSVLine(currentRecord);
        if (fields.length > 0) {
          records.push(fields);
        }
      } catch (err) {
        errors.push({
          line: lineIndex,
          content: currentRecord.substring(0, 100) + '...',
          error: err.message
        });
      }
      currentRecord = '';
    }
    
    lineIndex++;
  }
  
  // Handle any remaining content
  if (currentRecord.trim()) {
    try {
      const fields = parseCSVLine(currentRecord);
      if (fields.length > 0) {
        records.push(fields);
      }
    } catch (err) {
      errors.push({
        line: lineIndex,
        content: currentRecord.substring(0, 100) + '...',
        error: err.message
      });
    }
  }
  
  console.log(`Parsed ${records.length} records from ${filePath}`);
  if (errors.length > 0) {
    console.log(`Parsing errors (${errors.length}):`);
    errors.forEach(err => {
      console.log(`  Line ${err.line}: ${err.error} - Content: ${err.content}`);
    });
  }
  
  return { header, records, errors };
}

function parseCSVLine(line) {
  const fields = [];
  let current = '';
  let inQuotes = false;
  let i = 0;
  
  while (i < line.length) {
    const char = line[i];
    const nextChar = line[i + 1];
    
    if (char === '"') {
      if (!inQuotes) {
        inQuotes = true;
      } else if (nextChar === '"') {
        // Escaped quote
        current += '"';
        i++; // Skip next quote
      } else {
        inQuotes = false;
      }
    } else if (char === ',' && !inQuotes) {
      fields.push(current);
      current = '';
    } else {
      current += char;
    }
    i++;
  }
  
  fields.push(current);
  return fields.map(field => field.trim());
}

function analyzeColumnType(header, sampleValues) {
  const columnName = header.toLowerCase();
  
  // Get non-empty sample values
  const nonEmptyValues = sampleValues.filter(val => val && val !== '' && val !== 'null');
  
  if (nonEmptyValues.length === 0) {
    return 'TEXT';
  }
  
  // Check if values contain decimal points
  const hasDecimals = nonEmptyValues.some(val => val.includes('.'));
  
  // Check if all non-null values are numeric
  const numericValues = nonEmptyValues.filter(val => {
    const cleaned = val.replace(/[^0-9.-]/g, '');
    return !isNaN(parseFloat(cleaned)) && isFinite(cleaned);
  });
  
  const allNumeric = numericValues.length === nonEmptyValues.length;
  
  if (allNumeric) {
    if (hasDecimals || columnName.includes('duration') || columnName.includes('video')) {
      return 'DECIMAL(15,6)';
    } else if (columnName.includes('count') || columnName.includes('views') || 
               columnName.includes('subscribers') || columnName.includes('likes') ||
               columnName.includes('play') || columnName.includes('digg') ||
               columnName.includes('collect') || columnName.includes('share')) {
      return 'BIGINT';
    } else {
      return 'DECIMAL(15,6)'; // Safe default for numeric data
    }
  }
  
  return 'TEXT';
}

function cleanValue(value, columnType) {
  if (!value || value === '' || value === 'null') return null;
  
  // Remove surrounding quotes
  if (value.startsWith('"') && value.endsWith('"')) {
    value = value.slice(1, -1);
  }
  
  // Handle escaped quotes
  value = value.replace(/""/g, '"');
  
  if (columnType === 'BIGINT' || columnType.startsWith('DECIMAL')) {
    const cleaned = value.replace(/[^0-9.-]/g, '');
    if (cleaned === '' || cleaned === '-') return null;
    const num = parseFloat(cleaned);
    return isNaN(num) ? null : num;
  }
  
  return value || null;
}

async function createTableFromCSV(tableName, header, sampleData) {
  console.log(`Creating table: ${tableName}`);
  
  // Analyze column types from sample data
  const columnDefs = header.map((col, index) => {
    const sampleValues = sampleData.slice(0, 200).map(row => row[index] || '');
    const type = analyzeColumnType(col, sampleValues);
    console.log(`  Column "${col}": ${type} (sample: ${sampleValues.slice(0, 3).join(', ')})`);
    return `"${col}" ${type}`;
  });
  
  // Drop existing table if it exists
  await pool.query(`DROP TABLE IF EXISTS "${tableName}"`);
  
  // Create new table
  const createSQL = `
    CREATE TABLE "${tableName}" (
      id SERIAL PRIMARY KEY,
      ${columnDefs.join(',\n      ')}
    )
  `;
  
  await pool.query(createSQL);
  console.log(`Table "${tableName}" created with ${header.length} columns`);
  
  return header.map((col, index) => {
    const sampleValues = sampleData.slice(0, 200).map(row => row[index] || '');
    return {
      name: col,
      type: analyzeColumnType(col, sampleValues)
    };
  });
}

async function batchInsertData(tableName, columns, records) {
  console.log(`Inserting ${records.length} records into ${tableName}...`);
  
  const batchSize = 100; // Smaller batches for better error handling
  let totalInserted = 0;
  let errors = [];
  
  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    
    try {
      const values = [];
      const placeholders = [];
      
      batch.forEach((record, batchIndex) => {
        const recordValues = columns.map((col, colIndex) => {
          const rawValue = record[colIndex] || '';
          return cleanValue(rawValue, col.type);
        });
        
        const recordPlaceholders = columns.map((_, colIndex) => 
          `$${batchIndex * columns.length + colIndex + 1}`
        ).join(', ');
        
        placeholders.push(`(${recordPlaceholders})`);
        values.push(...recordValues);
      });
      
      const columnNames = columns.map(col => `"${col.name}"`).join(', ');
      const insertSQL = `
        INSERT INTO "${tableName}" (${columnNames}) 
        VALUES ${placeholders.join(', ')}
      `;
      
      await pool.query(insertSQL, values);
      totalInserted += batch.length;
      
      if (i % (batchSize * 5) === 0) {
        console.log(`  Inserted ${totalInserted}/${records.length} records...`);
      }
      
    } catch (err) {
      const batchNumber = Math.floor(i / batchSize) + 1;
      console.log(`  Batch ${batchNumber} error: ${err.message}`);
      
      // Try inserting records one by one for this batch
      let recoveredInBatch = 0;
      for (let j = 0; j < batch.length; j++) {
        try {
          const record = batch[j];
          const recordValues = columns.map((col, colIndex) => {
            const rawValue = record[colIndex] || '';
            return cleanValue(rawValue, col.type);
          });
          
          const singlePlaceholders = columns.map((_, colIndex) => `$${colIndex + 1}`).join(', ');
          const columnNames = columns.map(col => `"${col.name}"`).join(', ');
          
          await pool.query(
            `INSERT INTO "${tableName}" (${columnNames}) VALUES (${singlePlaceholders})`,
            recordValues
          );
          recoveredInBatch++;
          totalInserted++;
        } catch (singleErr) {
          errors.push({
            row: i + j + 1,
            error: singleErr.message,
            data: batch[j].slice(0, 3).join(', ') + '...'
          });
        }
      }
      
      if (recoveredInBatch > 0) {
        console.log(`  Recovered ${recoveredInBatch}/${batch.length} records from failed batch`);
      }
    }
  }
  
  console.log(`Completed: ${totalInserted} records inserted into ${tableName}`);
  
  if (errors.length > 0) {
    console.log(`Individual record errors (${errors.length}):`);
    errors.slice(0, 10).forEach(err => {
      console.log(`  Row ${err.row}: ${err.error} - Data: ${err.data}`);
    });
    if (errors.length > 10) {
      console.log(`  ... and ${errors.length - 10} more errors`);
    }
  }
  
  return { inserted: totalInserted, errors };
}

async function verifyImport(tableName, expectedCount) {
  const result = await pool.query(`SELECT COUNT(*) as count FROM "${tableName}"`);
  const actualCount = parseInt(result.rows[0].count);
  
  console.log(`Verification - ${tableName}:`);
  console.log(`  Expected: ${expectedCount} rows`);
  console.log(`  Actual: ${actualCount} rows`);
  console.log(`  Success: ${actualCount === expectedCount ? 'YES' : 'NO'}`);
  
  if (actualCount !== expectedCount) {
    console.log(`  Missing: ${expectedCount - actualCount} rows`);
  }
  
  return actualCount;
}

async function importCompleteDataset() {
  console.log('=== COMPLETE M&S DATASET IMPORT (FIXED) ===');
  console.log(`Importing ${TARGET_FILES.length} CSV files with improved type detection...`);
  console.log('');
  
  const results = {};
  let totalExpected = 0;
  let totalImported = 0;
  
  for (const filePath of TARGET_FILES) {
    try {
      // Extract table name from file path
      const fileName = filePath.split('/').pop();
      const tableName = fileName.replace('.csv', '').replace(/[^a-zA-Z0-9_]/g, '_');
      
      console.log(`\n--- Processing: ${fileName} ---`);
      
      // Parse CSV file
      const { header, records, errors } = parseCompleteCSV(filePath);
      
      // Create table with improved column type detection
      const columns = await createTableFromCSV(tableName, header, records);
      
      // Insert all data in batches with error recovery
      const insertResult = await batchInsertData(tableName, columns, records);
      
      // Verify import
      const actualCount = await verifyImport(tableName, records.length);
      
      results[fileName] = {
        expectedRows: records.length,
        importedRows: actualCount,
        parseErrors: errors.length,
        insertErrors: insertResult.errors.length
      };
      
      totalExpected += records.length;
      totalImported += actualCount;
      
    } catch (err) {
      console.error(`Failed to process ${filePath}: ${err.message}`);
      results[filePath] = {
        error: err.message
      };
    }
  }
  
  console.log('\n=== FINAL IMPORT SUMMARY ===');
  console.log('File                                           | Expected | Imported | Status');
  console.log('-----------------------------------------------|----------|----------|--------');
  
  for (const [fileName, result] of Object.entries(results)) {
    if (result.error) {
      console.log(`${fileName.padEnd(47)} | ERROR: ${result.error}`);
    } else {
      const status = result.expectedRows === result.importedRows ? 'SUCCESS' : 'PARTIAL';
      console.log(`${fileName.padEnd(47)} | ${result.expectedRows.toString().padStart(8)} | ${result.importedRows.toString().padStart(8)} | ${status}`);
    }
  }
  
  console.log('-----------------------------------------------|----------|----------|--------');
  console.log(`${'TOTAL'.padEnd(47)} | ${totalExpected.toString().padStart(8)} | ${totalImported.toString().padStart(8)} | ${totalExpected === totalImported ? 'SUCCESS' : 'PARTIAL'}`);
  
  console.log(`\nImport completed: ${totalImported}/${totalExpected} rows imported`);
  console.log(`Success rate: ${((totalImported/totalExpected)*100).toFixed(2)}%`);
  
  return results;
}

async function main() {
  try {
    await importCompleteDataset();
    console.log('\nAll M&S CSV files have been processed with improved type detection.');
  } catch (error) {
    console.error('Import failed:', error);
  } finally {
    await pool.end();
  }
}

main();