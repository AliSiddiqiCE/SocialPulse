import { spawn } from 'child_process';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Cache directories
const CACHE_DIR = path.join(__dirname, 'cache');
const SENTIMENT_CACHE_FILE = path.join(__dirname, 'data', 'sentiment-cache.json');

// Ensure cache directory exists
if (!fs.existsSync(CACHE_DIR)) {
  fs.mkdirSync(CACHE_DIR, { recursive: true });
}

// Check if sentiment cache exists and is up to date
function isSentimentCacheValid() {
  try {
    if (!fs.existsSync(SENTIMENT_CACHE_FILE)) {
      console.log('❌ Sentiment cache not found');
      return false;
    }

    const cacheStats = fs.statSync(SENTIMENT_CACHE_FILE);
    const cacheModified = cacheStats.mtime;
    
    // Check if any CSV files are newer than the cache
    const publicDir = path.join(__dirname, 'public');
    const csvFiles = fs.readdirSync(publicDir).filter(file => file.endsWith('.csv'));
    
    for (const csvFile of csvFiles) {
      const csvPath = path.join(publicDir, csvFile);
      const csvStats = fs.statSync(csvPath);
      
      if (csvStats.mtime > cacheModified) {
        console.log(`📝 CSV file ${csvFile} is newer than sentiment cache`);
        return false;
      }
    }
    
    console.log('✅ Sentiment cache is up to date');
    return true;
  } catch (error) {
    console.error('❌ Error checking sentiment cache:', error);
    return false;
  }
}

// Run sentiment analysis extraction
async function runSentimentAnalysis() {
  return new Promise((resolve, reject) => {
    console.log('🧠 Running sentiment analysis extraction...');
    
    // Import and run the sentiment analyzer
    import('./server/sentiment-analyzer.js').then(async (module) => {
      try {
        const result = await module.extractAndAnalyzeSentiments();
        console.log(`✅ Sentiment analysis completed: ${result.data.length} records processed`);
        resolve(result);
      } catch (error) {
        console.error('❌ Sentiment analysis failed:', error);
        reject(error);
      }
    }).catch(error => {
      console.error('❌ Failed to import sentiment analyzer:', error);
      reject(error);
    });
  });
}

// Start the application
function startApplication() {
  console.log('🚀 Starting the application...');
  
  const isDev = process.env.NODE_ENV === 'development';
  const command = isDev ? 'npm' : 'npm';
  const args = isDev ? ['run', 'dev'] : ['run', 'start'];
  
  const app = spawn(command, args, {
    stdio: 'inherit',
    shell: true,
    cwd: __dirname
  });
  
  app.on('error', (error) => {
    console.error('❌ Failed to start application:', error);
    process.exit(1);
  });
  
  app.on('exit', (code) => {
    console.log(`Application exited with code ${code}`);
    process.exit(code);
  });
  
  // Handle process termination
  process.on('SIGINT', () => {
    console.log('\n🛑 Shutting down application...');
    app.kill('SIGINT');
  });
  
  process.on('SIGTERM', () => {
    console.log('\n🛑 Shutting down application...');
    app.kill('SIGTERM');
  });
}

// Main startup function
async function startWithCache() {
  console.log('🎯 Starting RetailSocialPulse with data caching...');
  
  try {
    // Check if sentiment cache is valid
    if (!isSentimentCacheValid()) {
      console.log('🔄 Cache is outdated or missing, running sentiment analysis...');
      await runSentimentAnalysis();
    } else {
      console.log('✅ Using existing sentiment cache');
    }
    
    // Start the application
    startApplication();
    
  } catch (error) {
    console.error('❌ Failed to start with cache:', error);
    process.exit(1);
  }
}

// Run the startup function
startWithCache(); 