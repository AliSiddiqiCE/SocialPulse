import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse/sync';
// @ts-ignore
import fetch from 'node-fetch';
import { SentimentRecord } from './types';
import { loadSentimentCache, saveSentimentCache, isSentimentCacheOutdated } from './sentiment-cache';

// Initialize global sentiment data if not exists
if (!global.sentimentData) {
  global.sentimentData = [];
}

// Add these types at the top of the file
interface SocialMetrics {
  platform: string;
  mentions: number;
  mentionCount: number;
  totalPosts: number;
  reach: number;
  likes: number;
  comments: number;
  shares: number;
  engagementScore: string;
}

interface Post {
  platform: string;
  date: string;
  likes?: number;
  comments?: number;
  shares?: number;
  reach?: number;
}

// Reference date for all date range calculations
const FIXED_REFERENCE_DATE = new Date('2025-05-29T23:59:59.999Z');

// Helper to read and parse a CSV file from the public folder
// CSV-only mode: all types are 'any' for demo simplicity
function readCSV(filename: string): any[] {
  try {
  const filePath = path.join(process.cwd(), 'public', filename);
    const fileContent = fs.readFileSync(filePath, 'utf-8');
    const rows = parse(fileContent, { columns: true, skip_empty_lines: true });
    
    // For YouTube data, normalize the date field
    if (filename.includes('yt_')) {
      return rows.map((row: any) => {
        // If date is empty, try to extract from title or use a fallback
        if (!row.date && row.title) {
          const dateMatch = row.title.match(/\b\d{4}\b/);
          row.date = dateMatch ? `${dateMatch[0]}-01-01` : new Date().toISOString().split('T')[0];
        }
        return row;
      });
    }
    return rows;
  } catch (error) {
    console.error(`Error reading CSV file ${filename}:`, error);
    return [];
  }
}

function isWithinDateRange(dateStr: string, startDate?: string, endDate?: string): boolean {
  if (!dateStr) return false; // If no date, exclude the row
  if (!startDate && !endDate) return true; // If no date range specified, include all
  
  try {
    // Parse the date, handling different formats
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) return false; // If date is invalid, exclude the row
    
    // Set to start of day in UTC
    date.setUTCHours(0, 0, 0, 0);
    
    if (startDate) {
      const start = new Date(startDate);
      start.setUTCHours(0, 0, 0, 0);
      if (isNaN(start.getTime())) return false; // If start date is invalid, exclude the row
      if (date < start) return false;
    }
    
    if (endDate) {
      const end = new Date(endDate);
      end.setUTCHours(23, 59, 59, 999);
      if (isNaN(end.getTime())) return false; // If end date is invalid, exclude the row
      if (date > end) return false;
    }
    
  return true;
  } catch (error) {
    console.error('Error parsing date:', error);
    return false; // If there's an error, exclude the row
  }
}

// Helper function to calculate engagement score based on platform and data
function calculateEngagementScore(platform: string, data: any[]): string {
  if (!data || data.length === 0) return '0%';
  
  let totalEngagement = 0;
  let totalReach = 0;
  
  switch (platform) {
    case 'tiktok':
      totalEngagement = data.reduce((sum, row) => 
        sum + Number(row.diggCount || 0) + Number(row.shareCount || 0) + Number(row.commentCount || 0), 0);
      totalReach = data.reduce((sum, row) => sum + Number(row.playCount || 0), 0);
      break;
    case 'instagram':
      totalEngagement = data.reduce((sum, row) => 
        sum + Number(row.likesCount || 0) + Number(row.commentsCount || 0), 0);
      totalReach = data.reduce((sum, row) => sum + Number(row.videoViewCount || 0), 0);
      break;
    case 'youtube':
      totalEngagement = data.reduce((sum, row) => 
        sum + Number(row.likes || 0) + Number(row.commentsCount || 0), 0);
      totalReach = data.reduce((sum, row) => sum + Number(row.viewCount || 0), 0);
      break;
    default:
      return '0%';
  }
  
  // Calculate engagement rate: (Total Engagement / Total Reach) * 100
  const engagementRate = totalReach > 0 ? (totalEngagement / totalReach) * 100 : 0;
  return `${engagementRate.toFixed(2)}%`;
}

// Add this helper function at the top
function normalizeDate(dateStr: string): string {
  if (!dateStr) return '';
  
  try {
    // Handle different date formats
    let date: Date;
    
    // Check if date is in DD/MM/YYYY format
    if (dateStr.includes('/')) {
      const [day, month, year] = dateStr.split('/');
      date = new Date(`${year}-${month}-${day}`);
    } else {
      date = new Date(dateStr);
    }
    
    // Return ISO string if valid date, empty string if invalid
    return !isNaN(date.getTime()) ? date.toISOString() : '';
  } catch {
    return '';
  }
}

// Update the aggregatePosts function
function aggregatePosts(brandId: number) {
  const posts: any[] = [];

  // --- TIKTOK (Both brands, both sources) ---
  const tiktokFiles = brandId === 1
    ? ['mands_tik_off.csv', 'mands_tik_hash.csv']
    : ['next_tik_off.csv', 'next_tik_hash.csv'];
  for (const file of tiktokFiles) {
    for (const row of readCSV(file)) {
      const likes = Number(row.diggCount || 0);
      const comments = Number(row.commentCount || 0);
      const reach = Number(row.playCount || 0);
      // Only include in engagement rate if all three are present and greater than zero
      const validForEngagementRate = likes > 0 && comments > 0 && reach > 0;
      if (likes || comments) {
        posts.push({
          brandId,
          platform: 'tiktok',
          likes,
          comments,
          reach,
          totalEngagement: likes + comments,
          engagementRate: validForEngagementRate ? (likes + comments) / reach : null,
          date: normalizeDate(row.created_time),
          hashtags: row.hashtags || '',
          mentions: row.mentions || '',
        });
      }
    }
  }

  // --- INSTAGRAM ---
  const instaHash = brandId === 1 ? 'mands_insta_hash.csv' : 'next_insta_hash.csv';
  const instaOff = brandId === 1 ? 'mands_insta_off.csv' : 'next_insta_off.csv';
  // OFFICIAL
  for (const row of readCSV(instaOff)) {
    const likes = Number(row.likesCount || 0); // Both brands: likes from OFF
    const comments = Number(row.commentsCount || 0);
    const reach = Number(row.videoPlayCount || row.videoViewCount || 0);
    const validForEngagementRate = likes > 0 && comments > 0 && reach > 0;
    if ((likes || comments)) {
      posts.push({
        brandId,
        platform: 'instagram',
        likes,
        comments,
        reach,
        totalEngagement: likes + comments,
        engagementRate: validForEngagementRate ? (likes + comments) / reach : null,
        date: normalizeDate(row.timestamp),
        hashtags: row.hashtags || '',
        mentions: row.mentions || '',
      });
    }
  }
  // HASH
  for (const row of readCSV(instaHash)) {
    const likes = Number(row.likesCount || 0); // Both brands: likes from HASH
    const comments = Number(row.commentsCount || 0);
    // Reach only from OFFICIAL, so 0 here
    if ((likes || comments)) {
      posts.push({
        brandId,
        platform: 'instagram',
        likes,
        comments,
        reach: 0,
        totalEngagement: likes + comments,
        engagementRate: null,
        date: normalizeDate(row.timestamp),
        hashtags: row.hashtags || '',
        mentions: row.mentions || '',
      });
    }
  }

  // --- YOUTUBE (Both official and hashtag files) ---
  const ytFiles = brandId === 1 
    ? ['mands_yt_off.csv', 'mands_yt_hash.csv']
    : ['next_yt_off.csv', 'next_yt_hash.csv'];

  for (const file of ytFiles) {
    for (const row of readCSV(file)) {
      // YouTube CSV has these column names: likes, commentsCount, viewCount, date
      const likes = Number(row.likes || 0);
      const comments = Number(row.commentsCount || 0);
      const reach = Number(row.viewCount || 0);
      const validForEngagementRate = likes > 0 && comments > 0 && reach > 0;
      
      // Only include posts that have some engagement data
      if (likes > 0 || comments > 0 || reach > 0) {
        posts.push({
          brandId,
          platform: 'youtube',
          likes,
          comments,
          reach,
          totalEngagement: likes + comments,
          engagementRate: validForEngagementRate ? (likes + comments) / reach : null,
          date: normalizeDate(row.date),
          hashtags: row.hashtags || '',
          mentions: '', // always zero for YouTube
        });
      }
    }
  }

  // Filter out posts with invalid dates
  return posts.filter(post => post.date);
}

function extractTopics(text: string): string[] {
  // Extract hashtags and split text into words (improve as needed)
  const hashtags = (text.match(/#\w+/g) || []).map(tag => tag.toLowerCase());
  const words = text
    .replace(/[^a-zA-Z0-9# ]/g, '')
    .toLowerCase()
    .split(' ')
    .filter(word => word.length > 4 && !word.startsWith('#'));
  return [...hashtags, ...words];
}

async function analyzeSentimentWithTextBlob(text: string) {
  if (!text || typeof text !== 'string') return { sentiment: 'neutral', score: 0.5 };
  try {
    const response = await fetch('http://localhost:5001/analyze', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text })
    });
    if (!response.ok) return { sentiment: 'neutral', score: 0.5 };
    return await response.json();
  } catch (error) {
    console.error('TextBlob API error:', error);
    return { sentiment: 'neutral', score: 0.5 };
  }
}

export async function getKeyTopicsBySentiment(brandId: number, platform?: string, startDate?: string, endDate?: string) {
  console.log(`Getting key topics for brandId: ${brandId}, platform: ${platform || 'all'}, startDate: ${startDate}, endDate: ${endDate}`);
  const files = [
    brandId === 1 ? 'mands_insta_off.csv' : 'next_insta_off.csv',
    brandId === 1 ? 'mands_tik_off.csv' : 'next_tik_off.csv',
    brandId === 1 ? 'mands_yt_off.csv' : 'next_yt_off.csv'
  ];
  // Filter files based on platform if specified
  const filteredFiles = platform ? 
    files.filter(file => {
      if (platform === 'instagram') return file.includes('insta');
      if (platform === 'tiktok') return file.includes('tik');
      if (platform === 'youtube') return file.includes('yt');
      return true;
    }) : 
    files;
  // Store topic data with sentiment counts and total mentions
  const topicData: Record<string, { 
    positive: number; 
    neutral: number; 
    negative: number; 
    total: number;
  }> = {};
  // Process each file
  for (const file of filteredFiles) {
    const rows = readCSV(file);
    for (const row of rows) {
      // Skip if no topic
      if (!row.topic) continue;
      // Filter by date if provided
      let rowDate = row.timestamp || row.created_time || row.date;
      if ((startDate || endDate) && !isWithinDateRange(rowDate, startDate, endDate)) continue;
      // Clean up the topic string (remove quotes)
      const topic = row.topic.replace(/"/g, '');
      // Get text content for sentiment analysis
      let text = '';
      if (file.includes('insta')) {
        text = row.caption || '';
      } else if (file.includes('tik')) {
        text = row.description || row.video_description || row.text || '';
      } else if (file.includes('yt')) {
        text = row.title || row.video_description || '';
      }
      // Analyze sentiment
      const { sentiment } = await analyzeSentimentWithTextBlob(text);
      // Initialize topic data if not exists
      if (!topicData[topic]) {
        topicData[topic] = { 
          positive: 0, 
          neutral: 0, 
          negative: 0, 
          total: 0
        };
      }
      // Increment sentiment count
      if (sentiment === 'positive' || sentiment === 'neutral' || sentiment === 'negative') {
        topicData[topic][sentiment as 'positive' | 'neutral' | 'negative']++;
        topicData[topic].total++;
      }
    }
  }
  // Convert to array format with calculated sentiment scores
  const result = Object.entries(topicData).map(([topic, data]) => {
    // Calculate sentiment score as percentage (0-100)
    const positiveWeight = 100;
    const neutralWeight = 50;
    const negativeWeight = 0;
    let sentimentScore = 0;
    if (data.total > 0) {
      sentimentScore = (
        (data.positive * positiveWeight) + 
        (data.neutral * neutralWeight) + 
        (data.negative * negativeWeight)
      ) / data.total;
    }
    return {
      topic,
      sentiment_score: sentimentScore
    };
  });
  // Shuffle the result array (Fisher-Yates shuffle)
  for (let i = result.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [result[i], result[j]] = [result[j], result[i]];
  }
  return result;
}

export async function getPostFrequency(brandId: number, platform?: string, startDate?: string, endDate?: string) {
  const posts = aggregatePosts(brandId);
  
  // Filter by platform if specified
  let filteredPosts = posts;
  if (platform) {
    filteredPosts = posts.filter(post => post.platform === platform.toLowerCase());
  }
  
  // Filter by date range if specified
  if (startDate || endDate) {
    filteredPosts = filteredPosts.filter(post => isWithinDateRange(post.date, startDate, endDate));
  }
  
  // Group posts by date
  const frequencyMap: Record<string, number> = {};
  filteredPosts.forEach(post => {
    const date = post.date.split('T')[0]; // Get just the date part
    frequencyMap[date] = (frequencyMap[date] || 0) + 1;
  });
  
  // Convert to array format
  return Object.entries(frequencyMap).map(([date, count]) => ({
    date,
    count
  })).sort((a, b) => a.date.localeCompare(b.date));
}

// Function to get engagement data over time from CSV files
export async function getEngagementOverTime(brandId: number, platform?: string, dateRangeParam?: string): Promise<any> {
  // Get all posts for the brand
  const posts = aggregatePosts(brandId);
  
  // Filter posts by platform if specified
  let filteredPosts = posts;
  if (platform && platform !== 'all') {
    filteredPosts = posts.filter(post => post.platform === platform.toLowerCase());
  }
  
  // Find the latest date in all posts to use as reference
  let latestDateInAllData = FIXED_REFERENCE_DATE; // Always use fixed reference date
  
  // Try to find the latest date in the posts
  const datesFromPosts = posts
    .filter(post => post.date)
    .map(post => new Date(post.date));
  
  if (datesFromPosts.length > 0) {
    // Find the most recent date - but we'll still use FIXED_REFERENCE_DATE for calculations
    const actualLatestDate = new Date(Math.max(...datesFromPosts.map(date => date.getTime())));
    console.log('Actual latest date in data:', actualLatestDate.toISOString().split('T')[0]);
    console.log('Using fixed reference date:', FIXED_REFERENCE_DATE.toISOString().split('T')[0]);
  }
  
  // Calculate date range based on the fixed reference date
  let startDate: Date | undefined;
  if (dateRangeParam && dateRangeParam !== 'all') {
    // Use the fixed reference date for all calculations
    const referenceDate = new Date(FIXED_REFERENCE_DATE.getTime());
    
    switch (dateRangeParam) {
      case '7days':
        startDate = new Date(referenceDate.setDate(referenceDate.getDate() - 6)); // 7 days inclusive
        break;
      case '30days':
        startDate = new Date(referenceDate.setDate(referenceDate.getDate() - 29)); // 30 days inclusive
        break;
      case '90days':
        startDate = new Date(referenceDate.setDate(referenceDate.getDate() - 89)); // 90 days inclusive
        break;
      default:
        startDate = new Date(referenceDate.setDate(referenceDate.getDate() - 29)); // Default to 30 days inclusive
    }
    
    if (startDate) {
      // Use FIXED_REFERENCE_DATE as the end date for filtering
      const endDate = FIXED_REFERENCE_DATE;
      
      filteredPosts = filteredPosts.filter(post => {
        if (!post.date) return false;
        const postDate = new Date(post.date);
        return postDate >= startDate! && postDate <= endDate;
      });
    }
  }
  
  // Determine the time buckets based on the date range
  let timeBuckets: { label: string, startDate: Date, endDate: Date }[] = [];
  
  // Create a constant reference to latestDateInAllData to prevent TypeScript null errors
  const referenceLatestDate = FIXED_REFERENCE_DATE;
  
  if (dateRangeParam === '7days' || dateRangeParam === '7d') {
    // For 7 days, create daily buckets
    // Use a different approach to ensure we get data for all days
    
    // Start from 6 days back from the latest date to ensure we have a full week of data
    // This will give us a better chance of having data for all days including Monday and Tuesday
    let bestStartDate = new Date(referenceLatestDate.getTime());
    bestStartDate.setDate(bestStartDate.getDate() - 6); // Start 6 days back from the latest date
    
    // Create the 7 daily buckets starting from our best start date
    for (let i = 0; i < 7; i++) {
      const bucketEndDate = new Date(bestStartDate.getTime());
      bucketEndDate.setDate(bucketEndDate.getDate() + i); // Move forward through the week
      
      const bucketStartDate = new Date(bucketEndDate.getTime());
      bucketStartDate.setHours(0, 0, 0, 0);
      
      bucketEndDate.setHours(23, 59, 59, 999);
      
      // Format the day as Mon, Tue, etc.
      // Shift the day label by one day back (so Thursday's data shows as Wednesday, etc.)
      const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
      
      // Get the previous day's index for the label
      let dayIndex = bucketStartDate.getDay() - 1;
      if (dayIndex < 0) dayIndex = 6; // If it's Sunday (0), wrap around to Saturday (6)
      
      const label = dayNames[dayIndex];
      
      timeBuckets.unshift({ label, startDate: bucketStartDate, endDate: bucketEndDate });
    }
  } else if (dateRangeParam === '30days' || dateRangeParam === '30d') {
    // For 30 days, create weekly buckets
    for (let i = 0; i < 4; i++) {
      const bucketEndDate = new Date(referenceLatestDate.getTime());
      bucketEndDate.setDate(bucketEndDate.getDate() - (i * 7));
      
      const bucketStartDate = new Date(bucketEndDate.getTime());
      bucketStartDate.setDate(bucketStartDate.getDate() - 6);
      bucketStartDate.setHours(0, 0, 0, 0);
      
      bucketEndDate.setHours(23, 59, 59, 999);
      
      const label = `Week ${4 - i}`;
      timeBuckets.unshift({ label, startDate: bucketStartDate, endDate: bucketEndDate });
    }
  } else if (dateRangeParam === '90days' || dateRangeParam === '90d') {
    // For 90 days, create monthly buckets
    for (let i = 0; i < 3; i++) {
      const bucketEndDate = new Date(referenceLatestDate.getTime());
      bucketEndDate.setMonth(bucketEndDate.getMonth() - i);
      
      const bucketStartDate = new Date(bucketEndDate.getTime());
      bucketStartDate.setDate(1);
      bucketStartDate.setHours(0, 0, 0, 0);
      
      // Fix: get the last day of the month, then set time
      const lastDay = new Date(bucketEndDate.getFullYear(), bucketEndDate.getMonth() + 1, 0).getDate();
      bucketEndDate.setDate(lastDay);
      bucketEndDate.setHours(23, 59, 59, 999);
      
      const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
      const label = `${monthNames[bucketStartDate.getMonth()]}`;
      
      timeBuckets.unshift({ label, startDate: bucketStartDate, endDate: bucketEndDate });
    }
  } else {
    // Default or 'all' - create 6 monthly buckets
    for (let i = 0; i < 6; i++) {
      const bucketEndDate = new Date(referenceLatestDate.getTime());
      bucketEndDate.setMonth(bucketEndDate.getMonth() - i);
      
      const bucketStartDate = new Date(bucketEndDate.getTime());
      bucketStartDate.setDate(1);
      bucketStartDate.setHours(0, 0, 0, 0);
      
      // Fix: get the last day of the month, then set time
      const lastDay = new Date(bucketEndDate.getFullYear(), bucketEndDate.getMonth() + 1, 0).getDate();
      bucketEndDate.setDate(lastDay);
      bucketEndDate.setHours(23, 59, 59, 999);
      
      const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
      const label = `${monthNames[bucketStartDate.getMonth()]}`;
      
      timeBuckets.unshift({ label, startDate: bucketStartDate, endDate: bucketEndDate });
    }
  }
  
  // Aggregate engagement metrics for each time bucket
  const engagementData = timeBuckets.map(bucket => {
    // Filter posts for this time bucket
    const postsInBucket = filteredPosts.filter(post => {
      if (!post.date) return false;
      const postDate = new Date(post.date);
      return postDate >= bucket.startDate && postDate <= bucket.endDate;
    });
    
    // Calculate engagement metrics
    let likes = 0;
    let comments = 0;
    let shares = 0;
    
    postsInBucket.forEach(post => {
      // First try to parse the numbers directly
      const postLikes = typeof post.likes === 'number' ? post.likes : parseInt(post.likes || '0', 10) || 0;
      const postComments = typeof post.comments === 'number' ? post.comments : parseInt(post.comments || '0', 10) || 0;
      const postShares = typeof post.shares === 'number' ? post.shares : parseInt(post.shares || '0', 10) || 0;
      
      // Add to our running totals
      likes += postLikes;
      comments += postComments;
      shares += postShares;
      
      // Debug output to help diagnose issues
      if (postLikes > 0 || postComments > 0 || postShares > 0) {
        console.log(`Post engagement: date=${post.date}, likes=${postLikes}, comments=${postComments}, shares=${postShares}`);
      }
    });
    
    return {
      week: bucket.label,
      likes,
      comments,
      shares,
      posts: postsInBucket.length
    };
  });
  
  return engagementData;
}

export const storage = {
  // Example: getBrands returns static brands based on available CSVs
  async getBrands(): Promise<any[]> {
    // You can hardcode or infer from CSV filenames
    return [
      { id: 1, name: 'Marks & Spencer', slug: 'marks-spencer', industry: 'Retail', description: 'Marks & Spencer', createdAt: new Date() },
      { id: 2, name: 'Next Retail', slug: 'next-retail', industry: 'Retail', description: 'Next Retail', createdAt: new Date() },
    ];
  },

  async getBrandBySlug(slug: string): Promise<any> {
    const brands = await this.getBrands();
    return brands.find(b => b.slug === slug);
  },

  // Get social media metrics for KPI cards based on real data
  async getSocialMetrics(brandId: number, startDate?: string, endDate?: string, platform?: string): Promise<SocialMetrics[]> {
    console.log(`Getting social metrics for brand ${brandId}, platform: ${platform}, date range: ${startDate} to ${endDate}`);
    
    try {
      // Get all posts for the brand using aggregatePosts for better data processing
      let allPosts = aggregatePosts(brandId);
      
      // Filter by platform if specified
      if (platform && platform !== 'all') {
        allPosts = allPosts.filter(post => post.platform === platform);
      }

      // Handle date range filtering
      if (startDate || endDate) {
        // Filter posts by date range
        allPosts = allPosts.filter(post => {
          if (!post.date) return false;
          
          const postDate = new Date(post.date);
          postDate.setUTCHours(0, 0, 0, 0); // Normalize to start of day in UTC
          
          if (startDate) {
            const startDateObj = new Date(startDate);
            startDateObj.setUTCHours(0, 0, 0, 0);
            if (postDate < startDateObj) return false;
          }
          
          if (endDate) {
            const endDateObj = new Date(endDate);
            endDateObj.setUTCHours(23, 59, 59, 999);
            if (postDate > endDateObj) return false;
          }
          
          return true;
        });
      }
      
      // Count total posts directly
      const totalPosts = allPosts.length;
      
      // Calculate totals across all platforms
      let totalLikes = 0;
      let totalComments = 0;
      let totalShares = 0;
      let totalReach = 0;
      
      allPosts.forEach(post => {
        totalLikes += Number(post.likes || 0);
        totalComments += Number(post.comments || 0);
        totalShares += Number(post.shares || 0);
        totalReach += Number(post.reach || 0);
      });
      
      // Calculate engagement rate as percentage
      let engagementRate = 0;
      if (totalReach > 0) {
        engagementRate = ((totalLikes + totalComments + totalShares) / totalReach) * 100;
      }
      
      console.log('Filtered metrics:', {
          totalPosts,
        dateRange: { startDate, endDate },
        platform: platform || 'all',
        engagementRate: `${engagementRate.toFixed(2)}%`
      });
      
      return [{
        platform: platform || 'all',
        mentions: totalPosts,
        mentionCount: totalPosts,
        totalPosts: totalPosts,
        reach: totalReach,
        likes: totalLikes,
        comments: totalComments,
        shares: totalShares,
        engagementScore: `${engagementRate.toFixed(2)}%`
      }];
    } catch (error) {
      console.error('Error getting social metrics:', error);
    return [];
    }
  },

  async getTopContent(brandId: number, limit: number = 10, platform?: string, startDate?: string, endDate?: string): Promise<any[]> {
    let allContent: any[] = [];
    if (brandId === 1) { // Marks & Spencer
      if (!platform || platform === 'tiktok') {
        const tiktokOfficial = readCSV('mands_tik_off.csv');
        const tiktokHashtag = readCSV('mands_tik_hash.csv');
        let allTiktok = [...tiktokOfficial, ...tiktokHashtag];
        if (startDate || endDate) {
          allTiktok = allTiktok.filter(row => isWithinDateRange(row.created_time, startDate, endDate));
        }
        allContent = allContent.concat(allTiktok.map(row => ({
          brandId,
          platform: 'tiktok',
          content: row.text || '',
          postType: 'video',
          publishedAt: row.created_time || new Date().toISOString(),
          views: Number(row.playCount || 0),
          likes: Number(row.diggCount || 0),
          comments: Number(row.commentCount || 0),
          shares: Number(row.shareCount || 0),
          engagementRate: 0,
          createdAt: new Date(),
        })));
      }
      if (!platform || platform === 'instagram') {
        const instagramOfficial = readCSV('mands_insta_off.csv');
        const instagramHashtag = readCSV('mands_insta_hash.csv');
        let allInstagram = [...instagramOfficial, ...instagramHashtag];
        if (startDate || endDate) {
          allInstagram = allInstagram.filter(row => isWithinDateRange(row.timestamp, startDate, endDate));
        }
        allContent = allContent.concat(allInstagram.map(row => ({
          brandId,
          platform: 'instagram',
          content: row.caption || '',
          postType: 'image',
          publishedAt: row.timestamp || new Date().toISOString(),
          views: Number(row.videoViewCount || 0),
          likes: Number(row.likesCount || 0),
          comments: Number(row.commentsCount || 0),
          shares: 0,
          engagementRate: 0,
          createdAt: new Date(),
        })));
      }
      if (!platform || platform === 'youtube') {
        const youtubeOfficial = readCSV('mands_yt_off.csv');
        const youtubeHashtag = readCSV('mands_yt_hash.csv');
        let allYoutube = [...youtubeOfficial, ...youtubeHashtag];
        if (startDate || endDate) {
          allYoutube = allYoutube.filter(row => isWithinDateRange(row.date, startDate, endDate));
        }
        allContent = allContent.concat(allYoutube.map(row => ({
          brandId,
          platform: 'youtube',
          content: row.video_description || '',
          postType: 'video',
          publishedAt: row.date || new Date().toISOString(),
          views: Number(row.view_count || 0),
          likes: Number(row.likes || 0),
          comments: Number(row.commentsCount || 0),
          shares: 0,
          engagementRate: 0,
          createdAt: new Date(),
        })));
      }
    } else if (brandId === 2) { // Next Retail
      if (!platform || platform === 'tiktok') {
        const tiktokOfficial = readCSV('next_tik_off.csv');
        const tiktokHashtag = readCSV('next_tik_hash.csv');
        let allTiktok = [...tiktokOfficial, ...tiktokHashtag];
        if (startDate || endDate) {
          allTiktok = allTiktok.filter(row => isWithinDateRange(row.created_time, startDate, endDate));
        }
        allContent = allContent.concat(allTiktok.map(row => ({
          brandId,
          platform: 'tiktok',
          content: row.text || '',
          postType: 'video',
          publishedAt: row.created_time || new Date().toISOString(),
          views: Number(row.playCount || 0),
          likes: Number(row.diggCount || 0),
          comments: Number(row.commentCount || 0),
          shares: Number(row.shareCount || 0),
          engagementRate: 0,
          createdAt: new Date(),
        })));
      }
      if (!platform || platform === 'instagram') {
        const instagramOfficial = readCSV('next_insta_off.csv');
        const instagramHashtag = readCSV('next_insta_hash.csv');
        let allInstagram = [...instagramOfficial, ...instagramHashtag];
        if (startDate || endDate) {
          allInstagram = allInstagram.filter(row => isWithinDateRange(row.timestamp, startDate, endDate));
        }
        allContent = allContent.concat(allInstagram.map(row => ({
          brandId,
          platform: 'instagram',
          content: row.caption || '',
          postType: 'image',
          publishedAt: row.timestamp || new Date().toISOString(),
          views: Number(row.videoViewCount || 0),
          likes: Number(row.likesCount || 0),
          comments: Number(row.commentsCount || 0),
          shares: 0,
          engagementRate: 0,
          createdAt: new Date(),
        })));
      }
      if (!platform || platform === 'youtube') {
        let youtubeOfficial = readCSV('next_yt_off.csv');
        let youtubeHashtag = readCSV('next_yt_hash.csv');
        let allYoutube = [...youtubeOfficial, ...youtubeHashtag];
        if (startDate || endDate) {
          allYoutube = allYoutube.filter(row => isWithinDateRange(row.date, startDate, endDate));
        }
        allContent = allContent.concat(allYoutube.map(row => ({
          brandId,
          platform: 'youtube',
          content: row.video_description || '',
          postType: 'video',
          publishedAt: row.date || new Date().toISOString(),
          views: Number(row.viewCount || 0),
          likes: Number(row.likes || 0),
          comments: Number(row.commentsCount || 0),
          shares: 0,
          engagementRate: 0,
          createdAt: new Date(),
        })));
      }
    }
    // Sort by (views + likes + comments + shares) descending
    allContent = allContent.sort((a, b) => ((b.views + b.likes + b.comments + b.shares) - (a.views + a.likes + a.comments + a.shares)));
    // Add id field after sorting
    allContent = allContent.map((row, i) => ({ ...row, id: i + 1 }));
    return allContent.slice(0, limit);
  },

  async getBrandHashtags(brandId: number, limit: number = 10, platform?: string, startDate?: string, endDate?: string): Promise<any[]> {
    const posts = await aggregatePosts(brandId);
    let filteredPosts = posts;
    // Filter by platform if specified
    if (platform && platform !== 'all') {
      filteredPosts = filteredPosts.filter(post => post.platform === platform);
    }
    // Filter by date range if specified
    if (startDate || endDate) {
      filteredPosts = filteredPosts.filter(post => isWithinDateRange(post.date, startDate, endDate));
    }
    // Extract and count hashtags
    const hashtagCounts = new Map<string, number>();
    filteredPosts.forEach((post: { hashtags?: string }) => {
      if (post.hashtags) {
        const hashtags = post.hashtags.split(',').map((h: string) => h.trim());
        hashtags.forEach((hashtag: string) => {
          if (hashtag) {
            hashtagCounts.set(hashtag, (hashtagCounts.get(hashtag) || 0) + 1);
          }
        });
      }
    });
    // Convert to array and sort by usage count
    return Array.from(hashtagCounts.entries())
      .map(([hashtag, usageCount], i) => ({
        id: i + 1,
        brandId,
        platform: platform || 'all',
        hashtag,
        usageCount
      }))
      .sort((a, b) => b.usageCount - a.usageCount)
      .slice(0, limit);
  },

  async getIndustryHashtags(): Promise<any[]> {
    // Aggregate hashtags from all hashtag CSVs
    const hashtagFiles = [
      'mands_tik_hash.csv', 'mands_insta_hash.csv',
      'next_tik_hash.csv', 'next_insta_hash.csv'
    ];
    const hashtagCounts: { [key: string]: number } = {};
    for (const file of hashtagFiles) {
      const rows = readCSV(file);
      rows.forEach(row => {
        if (row.hashtags) {
          const hashtags = row.hashtags.split(',').map((h: string) => h.trim());
          hashtags.forEach((hashtag: string) => {
            if (hashtag) {
              hashtagCounts[hashtag] = (hashtagCounts[hashtag] || 0) + 1;
            }
          });
        }
      });
    }
    return Object.entries(hashtagCounts)
      .map(([hashtag, count], i) => ({
        id: i + 1,
        hashtag,
        usageCount: count,
        engagementRate: 0,
        createdAt: new Date(),
      }))
      .sort((a, b) => b.usageCount - a.usageCount)
      .slice(0, 20);
  },

  async getAudienceDemographics(brandId: number, startDate?: string, endDate?: string, platform?: string): Promise<any[]> {
    const posts = await aggregatePosts(brandId);
    let filteredPosts = posts;
    if (platform && platform !== 'all') {
      filteredPosts = filteredPosts.filter(post => post.platform === platform);
    }
    if (startDate || endDate) {
      filteredPosts = filteredPosts.filter(post => isWithinDateRange(post.date, startDate, endDate));
    }
    // Dummy demographics calculation (replace with real logic as needed)
    const demographics = [
      { age: '18-24', count: Math.floor(filteredPosts.length * 0.2) },
      { age: '25-34', count: Math.floor(filteredPosts.length * 0.3) },
      { age: '35-44', count: Math.floor(filteredPosts.length * 0.25) },
      { age: '45-54', count: Math.floor(filteredPosts.length * 0.15) },
      { age: '55+', count: Math.floor(filteredPosts.length * 0.1) },
    ];
    return demographics;
  },

  async getSentimentData(brandId: number, startDate?: string, endDate?: string, platform?: string): Promise<SentimentRecord[]> {
    try {
      // Always try to load fresh data from cache file first
      const cachedData = loadSentimentCache();
      let sentimentData = cachedData || [];
      
      // If no cached data or cache is outdated, analyze on-the-fly
      if (!cachedData || isSentimentCacheOutdated()) {
        console.log('Cache missing or outdated, analyzing on-the-fly');
        sentimentData = await this.analyzeSentimentFromCSV(brandId);
        saveSentimentCache(sentimentData);
      }
        
        // Filter by brand ID
      let filteredData = sentimentData.filter((item: SentimentRecord) => item.brandId === brandId);
        
        // Filter by platform if specified
        if (platform && platform !== 'all') {
        filteredData = filteredData.filter((item: SentimentRecord) => item.platform === platform);
        }
        
        // Filter by date range if specified
        if (startDate || endDate) {
        filteredData = filteredData.filter((item: SentimentRecord) => {
          // Convert to YYYY-MM-DD format to ignore time
          const itemDate = item.date.toISOString().split('T')[0];
          return isWithinDateRange(itemDate, startDate, endDate);
        });
        }
        
        console.log(`Returning ${filteredData.length} filtered sentiment records`);
        return filteredData;
    } catch (error) {
      console.error('Error fetching sentiment data:', error);
      return [];
      }
  },
      
  async analyzeSentimentFromCSV(brandId: number): Promise<SentimentRecord[]> {
    const sentimentData: SentimentRecord[] = [];
      const instagramFile = brandId === 1 ? 'mands_insta_off.csv' : 'next_insta_off.csv';
      const tiktokFile = brandId === 1 ? 'mands_tik_off.csv' : 'next_tik_off.csv';
      const youtubeFile = brandId === 1 ? 'mands_yt_off.csv' : 'next_yt_off.csv';
      
    // Process Instagram data
        for (const post of readCSV(instagramFile)) {
          const text = post.caption || '';
          if (!text) continue;
          
          const { sentiment, score, subjectivity } = await analyzeSentimentWithTextBlob(text);
          const date = new Date(post.timestamp || Date.now());
          
          sentimentData.push({ 
            brandId, 
            platform: 'instagram', 
            sentiment, 
            score, 
            subjectivity: subjectivity || 0.5,
            mentionCount: 1, 
        text: text.substring(0, 100),
            date, 
            createdAt: date 
          });
      }
      
    // Process TikTok data
        for (const video of readCSV(tiktokFile)) {
          const text = video.description || video.text || '';
          if (!text) continue;
          
          const { sentiment, score, subjectivity } = await analyzeSentimentWithTextBlob(text);
          const date = new Date(video.created_time || Date.now());
          
          sentimentData.push({ 
            brandId, 
            platform: 'tiktok', 
            sentiment, 
            score, 
            subjectivity: subjectivity || 0.5,
            mentionCount: 1, 
            text: text.substring(0, 100),
            date, 
            createdAt: date 
          });
      }
      
    // Process YouTube data
        for (const video of readCSV(youtubeFile)) {
          const text = video.title || video.video_description || '';
          if (!text) continue;
          
          const { sentiment, score, subjectivity } = await analyzeSentimentWithTextBlob(text);
          const date = new Date(video.publishedAt || video.date || Date.now());
          
          sentimentData.push({ 
            brandId, 
            platform: 'youtube', 
            sentiment, 
            score, 
            subjectivity: subjectivity || 0.5,
            mentionCount: 1, 
            text: text.substring(0, 100),
            date, 
            createdAt: date 
          });
      }
      
    console.log(`Generated ${sentimentData.length} sentiment records from CSV files`);
      return sentimentData;
  },

  async getContentStrategy(brandId: number, platform?: string, dateRangeParam?: string): Promise<any> {
    const posts = aggregatePosts(brandId);
    
    // Filter by platform if specified
    let filteredPosts = posts;
    if (platform && platform !== 'all') {
      filteredPosts = posts.filter(post => post.platform === platform.toLowerCase());
    }
    
    // Calculate date range based on the dateRangeParam
    let startDate: Date | undefined;
    let endDate = FIXED_REFERENCE_DATE; // Always use fixed reference date
    
    if (dateRangeParam && dateRangeParam !== 'all') {
      switch (dateRangeParam) {
        case '7days':
          startDate = new Date(endDate);
          startDate.setDate(endDate.getDate() - 6); // 7 days inclusive
          break;
        case '30days':
          startDate = new Date(endDate);
          startDate.setDate(endDate.getDate() - 29); // 30 days inclusive
          break;
        case '90days':
          startDate = new Date(endDate);
          startDate.setDate(endDate.getDate() - 89); // 90 days inclusive
          break;
        default:
          startDate = new Date(endDate);
          startDate.setDate(endDate.getDate() - 29); // Default to 30 days inclusive
      }
      
      // Filter posts by date range
        filteredPosts = filteredPosts.filter(post => {
          const postDate = new Date(post.date);
        return postDate >= startDate! && postDate <= endDate;
        });
    }
    
    const totalPosts = filteredPosts.length;
    
    // Calculate the number of days in the range
    let daysInRange = 1; // Default to 1 to avoid division by zero
    
    if (startDate) {
      // Calculate days between start and end date
      daysInRange = Math.ceil((endDate.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24));
    } else {
      // If no date range specified, use actual date range from the data
    if (filteredPosts.length > 0) {
        const dates = filteredPosts.map(post => new Date(post.date).getTime());
        const earliestDate = new Date(Math.min(...dates));
        daysInRange = Math.ceil((endDate.getTime() - earliestDate.getTime()) / (1000 * 60 * 60 * 24));
      }
    }
    
    // Ensure minimum of 1 day
    daysInRange = Math.max(1, daysInRange);
    
    // Calculate posts per day
    const postsPerDay = totalPosts / daysInRange;
    
    // Calculate content type distribution
    const contentTypes = filteredPosts.reduce((acc: any, post) => {
      const type = post.platform === 'youtube' ? 'video' : 
                  post.platform === 'instagram' ? 'image' : 'video';
      
      if (!acc[type]) {
        acc[type] = { count: 0, type };
      }
      acc[type].count++;
      return acc;
    }, {});
    
    // Convert to array and calculate percentages
    const contentTypesArray = Object.values(contentTypes).map((type: any) => ({
      type: type.type,
      count: type.count,
      percentage: Math.round((type.count / totalPosts) * 100)
    }));
    
    return {
      postsPerDay: parseFloat(postsPerDay.toFixed(2)),
      totalPosts,
      contentTypes: contentTypesArray,
      dateRange: {
        start: startDate?.toISOString() || null,
        end: endDate.toISOString()
      }
    };
  },

  async getAudienceOverlap(
    brand1Id: number, 
    brand2Id: number, 
    platform?: string, 
    dateRange?: string
  ): Promise<{ overlapPercentage: number; commonHashtags: string[] }> {
    // Get all posts for both brands
    const brand1Posts = aggregatePosts(brand1Id);
    const brand2Posts = aggregatePosts(brand2Id);

    // Filter by platform if specified
    let filteredBrand1Posts = brand1Posts;
    let filteredBrand2Posts = brand2Posts;

    if (platform && platform !== 'all') {
      filteredBrand1Posts = brand1Posts.filter(post => post.platform === platform.toLowerCase());
      filteredBrand2Posts = brand2Posts.filter(post => post.platform === platform.toLowerCase());
    }

    // Filter by date range if specified
    if (dateRange && dateRange !== 'all') {
      const now = FIXED_REFERENCE_DATE;
      let startDate: Date;
      
      switch (dateRange) {
        case '7days':
          startDate = new Date(now.setDate(now.getDate() - 6)); // 7 days inclusive
          break;
        case '30days':
          startDate = new Date(now.setDate(now.getDate() - 29)); // 30 days inclusive
          break;
        case '90days':
          startDate = new Date(now.setDate(now.getDate() - 89)); // 90 days inclusive
          break;
        default:
          startDate = new Date(now.setDate(now.getDate() - 29)); // Default to 30 days inclusive
      }

      filteredBrand1Posts = filteredBrand1Posts.filter(post => new Date(post.date) >= startDate);
      filteredBrand2Posts = filteredBrand2Posts.filter(post => new Date(post.date) >= startDate);
    }

    // Extract all unique keywords from both brands
    const brand1Keywords = new Set<string>();
    const brand2Keywords = new Set<string>();

    // Process brand 1 posts
    filteredBrand1Posts.forEach(post => {
      const text = `${post.hashtags || ''} ${post.mentions || ''}`;
      const keywords = text.toLowerCase().split(/\s+/).filter(word => word.length > 0);
      keywords.forEach(keyword => brand1Keywords.add(keyword));
    });

    // Process brand 2 posts
    filteredBrand2Posts.forEach(post => {
      const text = `${post.hashtags || ''} ${post.mentions || ''}`;
      const keywords = text.toLowerCase().split(/\s+/).filter(word => word.length > 0);
      keywords.forEach(keyword => brand2Keywords.add(keyword));
    });

    // Find common keywords
    const commonKeywords = new Set<string>();
    brand1Keywords.forEach(keyword => {
      if (brand2Keywords.has(keyword)) {
        commonKeywords.add(keyword);
      }
    });

    // Calculate overlap percentage
    const totalUniqueKeywords = brand1Keywords.size + brand2Keywords.size - commonKeywords.size;
    const overlapPercentage = totalUniqueKeywords > 0 
      ? Math.round((commonKeywords.size / totalUniqueKeywords) * 100)
      : 0;

    // Get top 5 most common keywords
    const commonHashtags = Array.from(commonKeywords)
      .filter(keyword => keyword.startsWith('#'))
      .slice(0, 5);

    return {
      overlapPercentage,
      commonHashtags
    };
  },
  
  // Get engagement data over time from CSV files
  async getEngagementOverTime(brandId: number, platform?: string, dateRangeParam?: string): Promise<any> {
    return getEngagementOverTime(brandId, platform, dateRangeParam);
  },

  // Add this method before getSocialMetrics
  async getAllPosts(brandId: number): Promise<Post[]> {
    // Use aggregatePosts for consistent data processing
    const posts = aggregatePosts(brandId);
    return posts.map(post => ({
      platform: post.platform.toLowerCase(),
      date: post.date,
      likes: Number(post.likes || 0),
      comments: Number(post.comments || 0),
      shares: Number(post.shares || 0),
      reach: Number(post.reach || 0)
    }));
  },

  getKeyTopicsBySentiment: getKeyTopicsBySentiment,
};