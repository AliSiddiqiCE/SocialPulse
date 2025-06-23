import { storage } from "./storage";
import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse/sync';
// @ts-ignore
import fetch from 'node-fetch';
import { SentimentRecord } from './types';
import { saveSentimentCache, loadSentimentCache, isSentimentCacheOutdated } from './sentiment-cache';

// Helper to read and parse a CSV file from the public folder
function readCSV(filename: string): any[] {
  const filePath = path.join(process.cwd(), 'public', filename);
  if (!fs.existsSync(filePath)) {
    console.error(`CSV file not found: ${filename}`);
    return [];
  }
  const content = fs.readFileSync(filePath, 'utf-8');
  return parse(content, { columns: true, skip_empty_lines: true });
}

async function analyzeSentimentWithTextBlob(text: string) {
  if (!text || typeof text !== 'string') return { sentiment: 'neutral', score: 0.5, subjectivity: 0.5 };
  try {
    const response = await fetch('http://localhost:5001/analyze', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text })
    });
    if (!response.ok) {
      console.error(`TextBlob API error: ${response.status} ${response.statusText}`);
      return { sentiment: 'neutral', score: 0.5, subjectivity: 0.5 };
    }
    return await response.json();
  } catch (error) {
    console.error('TextBlob API error:', error);
    return { sentiment: 'neutral', score: 0.5, subjectivity: 0.5 };
  }
}

// Process CSV data and analyze sentiment for each platform
async function processPlatformData(filename: string, platform: string, brandId: number, textFields: string | string[], dateFields: string | string[]): Promise<SentimentRecord[]> {
  console.log(`Processing ${platform} data from ${filename}...`);
  const results: SentimentRecord[] = [];
  const data = readCSV(filename);
  
  console.log(`Found ${data.length} ${platform} entries`);
  
  for (const row of data) {
    // Extract text content based on provided fields
    let textContent = '';
    const textFieldsArray = Array.isArray(textFields) ? textFields : [textFields];
    
    for (const field of textFieldsArray) {
      if (row[field]) {
        textContent = row[field];
        break;
      }
    }
    
    if (!textContent) continue;
    
    // Analyze sentiment using TextBlob
    const analysis = await analyzeSentimentWithTextBlob(textContent);
    
    // Determine date from row data based on provided date fields
    let date = new Date();
    const dateFieldsArray = Array.isArray(dateFields) ? dateFields : [dateFields];
    
    for (const field of dateFieldsArray) {
      if (row[field]) {
        date = new Date(row[field]);
        break;
      }
    }
    
    // Create sentiment record
    results.push({
      brandId,
      platform,
      sentiment: analysis.sentiment,
      score: analysis.score,
      subjectivity: analysis.subjectivity || 0.5,
      mentionCount: 1,
      text: textContent.substring(0, 100), // Store a snippet of the text
      date,
      createdAt: date
    });
  }
  
  return results;
}

export async function extractAndAnalyzeSentiments() {
  try {
    // Try to load from cache first
    if (!isSentimentCacheOutdated()) {
      const cachedData = loadSentimentCache();
      if (cachedData && cachedData.length > 0) {
        console.log(`Using cached sentiment data (${cachedData.length} records)`);
        global.sentimentData = cachedData;
        return {
          success: true,
          message: 'Loaded sentiment analysis from cache',
          data: cachedData
        };
      }
    }
    
    console.log('Starting sentiment analysis extraction from CSV files...');
    
    // Process data for each platform and brand
    const brandIds = [1, 2]; // M&S and Next
    const allSentimentData: SentimentRecord[] = [];
    
    for (const brandId of brandIds) {
      // Instagram
      const instagramFile = brandId === 1 ? 'mands_insta_off.csv' : 'next_insta_off.csv';
      const instagramData = await processPlatformData(instagramFile, 'instagram', brandId, 'caption', 'timestamp');
      allSentimentData.push(...instagramData);
      
      // TikTok
      const tiktokFile = brandId === 1 ? 'mands_tik_off.csv' : 'next_tik_off.csv';
      const tiktokData = await processPlatformData(tiktokFile, 'tiktok', brandId, ['description', 'text'], 'created_time');
      allSentimentData.push(...tiktokData);
      
      // YouTube
      const youtubeFile = brandId === 1 ? 'mands_yt_off.csv' : 'next_yt_off.csv';
      const youtubeData = await processPlatformData(youtubeFile, 'youtube', brandId, ['title', 'video_description'], ['publishedAt', 'date']);
      allSentimentData.push(...youtubeData);
    }
    
    // Calculate sentiment statistics
    const platforms = ['instagram', 'tiktok', 'youtube'];
    const sentimentTypes = ['positive', 'neutral', 'negative'];
    const stats = [];
    
    for (const platform of platforms) {
      const platformData = allSentimentData.filter(item => item.platform === platform);
      
      for (const sentiment of sentimentTypes) {
        const count = platformData.filter(item => item.sentiment === sentiment).length;
        stats.push({ platform, sentiment, count });
      }
    }
    
    // Store the sentiment data in memory for API access
    global.sentimentData = allSentimentData;
    
    // Save to cache file for future use
    saveSentimentCache(allSentimentData);
    
    console.log(`Sentiment analysis extraction completed with ${allSentimentData.length} total records!`);
    
    return {
      success: true,
      message: `Sentiment analysis completed from CSV files (${allSentimentData.length} records)`,
      stats,
      data: allSentimentData
    };
    
  } catch (error) {
    console.error('Error in sentiment analysis:', error);
    throw error;
  }
}