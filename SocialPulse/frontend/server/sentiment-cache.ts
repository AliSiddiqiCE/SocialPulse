import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { SentimentRecord } from './types';

// Get the directory path in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const SENTIMENT_CACHE_FILE = path.join(__dirname, '..', 'data', 'sentiment-cache.json');

/**
 * Save sentiment data to a cache file
 */
export function saveSentimentCache(sentimentData: SentimentRecord[]): void {
  try {
    // Ensure the data directory exists
    const dataDir = path.join(__dirname, '..', 'data');
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true });
    }

    // Convert Date objects to ISO strings for JSON serialization
    const serializedData = sentimentData.map(item => ({
      ...item,
      date: item.date.toISOString(),
      createdAt: item.createdAt.toISOString()
    }));

    // Write to file
    fs.writeFileSync(
      SENTIMENT_CACHE_FILE, 
      JSON.stringify(serializedData, null, 2)
    );
    
    console.log(`Sentiment cache saved to ${SENTIMENT_CACHE_FILE} (${sentimentData.length} records)`);
  } catch (error) {
    console.error('Error saving sentiment cache:', error);
  }
}

/**
 * Load sentiment data from cache file
 * @returns Array of sentiment records or null if cache doesn't exist
 */
export function loadSentimentCache(): SentimentRecord[] | null {
  try {
    if (!fs.existsSync(SENTIMENT_CACHE_FILE)) {
      console.log('Sentiment cache file not found');
      return null;
    }

    const fileContent = fs.readFileSync(SENTIMENT_CACHE_FILE, 'utf-8');
    const parsedData = JSON.parse(fileContent);
    
    // Convert ISO strings back to Date objects
    const deserializedData = parsedData.map((item: any) => ({
      ...item,
      date: new Date(item.date),
      createdAt: new Date(item.createdAt)
    }));

    console.log(`Loaded ${deserializedData.length} sentiment records from cache`);
    return deserializedData;
  } catch (error) {
    console.error('Error loading sentiment cache:', error);
    return null;
  }
}

/**
 * Check if sentiment cache is outdated by comparing with CSV modification times
 */
export function isSentimentCacheOutdated(): boolean {
  try {
    if (!fs.existsSync(SENTIMENT_CACHE_FILE)) {
      return true;
    }

    const cacheStats = fs.statSync(SENTIMENT_CACHE_FILE);
    const cacheModified = cacheStats.mtime;
    
    // Check if any CSV files are newer than the cache
    const publicDir = path.join(__dirname, '..', 'public');
    const csvFiles = fs.readdirSync(publicDir)
      .filter(file => file.endsWith('.csv'));
    
    for (const csvFile of csvFiles) {
      const csvPath = path.join(publicDir, csvFile);
      const csvStats = fs.statSync(csvPath);
      
      if (csvStats.mtime > cacheModified) {
        console.log(`CSV file ${csvFile} is newer than sentiment cache`);
        return true;
      }
    }
    
    return false;
  } catch (error) {
    console.error('Error checking if sentiment cache is outdated:', error);
    return true; // If there's an error, assume cache is outdated
  }
}
