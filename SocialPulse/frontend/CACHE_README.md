# Data Caching System

This application now includes a comprehensive data caching system that pre-processes and caches all data before starting the application, significantly improving startup time and performance.

## 🚀 Quick Start

### Option 1: Using npm scripts (Recommended)

```bash
# For development with caching
npm run dev-with-cache

# For production with caching
npm run start-with-cache
```

### Option 2: Using Windows scripts

```bash
# Using PowerShell (recommended for Windows)
.\start-cached.ps1

# Using Command Prompt
start-cached.bat
```

### Option 3: Manual caching

```bash
# First, cache the data
node start-with-cache.js

# Then start normally
npm run dev
# or
npm run start
```

## 📊 What Gets Cached

The caching system processes and stores:

1. **CSV Data**: All CSV files from the `public/` directory
2. **Sentiment Analysis**: Pre-processed sentiment data for all social media posts
3. **Engagement Metrics**: Pre-calculated engagement rates and statistics
4. **Aggregated Posts**: Normalized and aggregated post data across all platforms

## 📁 Cache Locations

- **Sentiment Cache**: `data/sentiment-cache.json`
- **General Cache**: `cache/` directory (created automatically)

## 🔄 Cache Invalidation

The cache is automatically invalidated when:
- CSV files are modified
- Cache files are deleted
- Cache files are corrupted

## ⚡ Performance Benefits

- **Faster Startup**: No need to process CSV files on every startup
- **Reduced Memory Usage**: Pre-processed data is more efficient
- **Better User Experience**: Application starts immediately with cached data
- **Consistent Performance**: No variance in startup time based on data size

## 🛠️ Development

### Cache Structure

```
cache/
├── data-cache.json          # Raw CSV data
├── metrics-cache.json       # Engagement metrics
└── aggregated-cache.json    # Aggregated posts data

data/
└── sentiment-cache.json     # Sentiment analysis results
```

### Manual Cache Management

```bash
# Clear all caches
rm -rf cache/ data/sentiment-cache.json

# Force cache refresh
npm run dev-with-cache
```

## 🔧 Troubleshooting

### Cache Issues

If you encounter cache-related issues:

1. **Clear the cache**:
   ```bash
   rm -rf cache/ data/sentiment-cache.json
   ```

2. **Restart with fresh cache**:
   ```bash
   npm run dev-with-cache
   ```

### Common Problems

- **"Cache file not found"**: Run the caching script first
- **"CSV file is newer than cache"**: Cache will be automatically refreshed
- **"Failed to import sentiment analyzer"**: Check that all dependencies are installed

## 📈 Monitoring

The caching process provides detailed logging:

```
🎯 Starting RetailSocialPulse with data caching...
✅ Sentiment cache is up to date
🚀 Starting the application...
```

Or when cache needs refresh:

```
🎯 Starting RetailSocialPulse with data caching...
🔄 Cache is outdated or missing, running sentiment analysis...
🧠 Running sentiment analysis extraction...
✅ Sentiment analysis completed: 1234 records processed
🚀 Starting the application...
```

## 🔄 Integration with Existing Code

The caching system is designed to work seamlessly with the existing codebase:

- **Backward Compatible**: Existing scripts still work
- **Automatic Fallback**: If cache fails, falls back to original behavior
- **Transparent**: No changes needed to existing API endpoints

## 📝 Notes

- Cache files are automatically created in the `cache/` directory
- The system checks file modification times to determine if cache is stale
- Sentiment analysis cache is shared between development and production
- Cache files are JSON format for easy debugging and inspection 