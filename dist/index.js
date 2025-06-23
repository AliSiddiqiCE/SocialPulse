var __defProp = Object.defineProperty;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __esm = (fn, res) => function __init() {
  return fn && (res = (0, fn[__getOwnPropNames(fn)[0]])(fn = 0)), res;
};
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};

// server/sentiment-cache.ts
import fs2 from "fs";
import path2 from "path";
import { fileURLToPath } from "url";
function saveSentimentCache(sentimentData) {
  try {
    const dataDir = path2.join(__dirname, "..", "data");
    if (!fs2.existsSync(dataDir)) {
      fs2.mkdirSync(dataDir, { recursive: true });
    }
    const serializedData = sentimentData.map((item) => ({
      ...item,
      date: item.date.toISOString(),
      createdAt: item.createdAt.toISOString()
    }));
    fs2.writeFileSync(
      SENTIMENT_CACHE_FILE,
      JSON.stringify(serializedData, null, 2)
    );
    console.log(`Sentiment cache saved to ${SENTIMENT_CACHE_FILE} (${sentimentData.length} records)`);
  } catch (error) {
    console.error("Error saving sentiment cache:", error);
  }
}
function loadSentimentCache() {
  try {
    if (!fs2.existsSync(SENTIMENT_CACHE_FILE)) {
      console.log("Sentiment cache file not found");
      return null;
    }
    const fileContent = fs2.readFileSync(SENTIMENT_CACHE_FILE, "utf-8");
    const parsedData = JSON.parse(fileContent);
    const deserializedData = parsedData.map((item) => ({
      ...item,
      date: new Date(item.date),
      createdAt: new Date(item.createdAt)
    }));
    console.log(`Loaded ${deserializedData.length} sentiment records from cache`);
    return deserializedData;
  } catch (error) {
    console.error("Error loading sentiment cache:", error);
    return null;
  }
}
function isSentimentCacheOutdated() {
  try {
    if (!fs2.existsSync(SENTIMENT_CACHE_FILE)) {
      return true;
    }
    const cacheStats = fs2.statSync(SENTIMENT_CACHE_FILE);
    const cacheModified = cacheStats.mtime;
    const publicDir = path2.join(__dirname, "..", "public");
    const csvFiles = fs2.readdirSync(publicDir).filter((file) => file.endsWith(".csv"));
    for (const csvFile of csvFiles) {
      const csvPath = path2.join(publicDir, csvFile);
      const csvStats = fs2.statSync(csvPath);
      if (csvStats.mtime > cacheModified) {
        console.log(`CSV file ${csvFile} is newer than sentiment cache`);
        return true;
      }
    }
    return false;
  } catch (error) {
    console.error("Error checking if sentiment cache is outdated:", error);
    return true;
  }
}
var __filename, __dirname, SENTIMENT_CACHE_FILE;
var init_sentiment_cache = __esm({
  "server/sentiment-cache.ts"() {
    "use strict";
    __filename = fileURLToPath(import.meta.url);
    __dirname = path2.dirname(__filename);
    SENTIMENT_CACHE_FILE = path2.join(__dirname, "..", "data", "sentiment-cache.json");
  }
});

// server/sentiment-analyzer.ts
var sentiment_analyzer_exports = {};
__export(sentiment_analyzer_exports, {
  extractAndAnalyzeSentiments: () => extractAndAnalyzeSentiments
});
import fs3 from "fs";
import path3 from "path";
import { parse as parse2 } from "csv-parse/sync";
import fetch2 from "node-fetch";
function readCSV2(filename) {
  const filePath = path3.join(process.cwd(), "public", filename);
  if (!fs3.existsSync(filePath)) {
    console.error(`CSV file not found: ${filename}`);
    return [];
  }
  const content = fs3.readFileSync(filePath, "utf-8");
  return parse2(content, { columns: true, skip_empty_lines: true });
}
async function analyzeSentimentWithTextBlob2(text) {
  if (!text || typeof text !== "string") return { sentiment: "neutral", score: 0.5, subjectivity: 0.5 };
  try {
    const response = await fetch2("http://localhost:5001/analyze", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text })
    });
    if (!response.ok) {
      console.error(`TextBlob API error: ${response.status} ${response.statusText}`);
      return { sentiment: "neutral", score: 0.5, subjectivity: 0.5 };
    }
    return await response.json();
  } catch (error) {
    console.error("TextBlob API error:", error);
    return { sentiment: "neutral", score: 0.5, subjectivity: 0.5 };
  }
}
async function processPlatformData(filename, platform, brandId, textFields, dateFields) {
  console.log(`Processing ${platform} data from ${filename}...`);
  const results = [];
  const data = readCSV2(filename);
  console.log(`Found ${data.length} ${platform} entries`);
  for (const row of data) {
    let textContent = "";
    const textFieldsArray = Array.isArray(textFields) ? textFields : [textFields];
    for (const field of textFieldsArray) {
      if (row[field]) {
        textContent = row[field];
        break;
      }
    }
    if (!textContent) continue;
    const analysis = await analyzeSentimentWithTextBlob2(textContent);
    let date = /* @__PURE__ */ new Date();
    const dateFieldsArray = Array.isArray(dateFields) ? dateFields : [dateFields];
    for (const field of dateFieldsArray) {
      if (row[field]) {
        date = new Date(row[field]);
        break;
      }
    }
    results.push({
      brandId,
      platform,
      sentiment: analysis.sentiment,
      score: analysis.score,
      subjectivity: analysis.subjectivity || 0.5,
      mentionCount: 1,
      text: textContent.substring(0, 100),
      // Store a snippet of the text
      date,
      createdAt: date
    });
  }
  return results;
}
async function extractAndAnalyzeSentiments() {
  try {
    if (!isSentimentCacheOutdated()) {
      const cachedData = loadSentimentCache();
      if (cachedData && cachedData.length > 0) {
        console.log(`Using cached sentiment data (${cachedData.length} records)`);
        global.sentimentData = cachedData;
        return {
          success: true,
          message: "Loaded sentiment analysis from cache",
          data: cachedData
        };
      }
    }
    console.log("Starting sentiment analysis extraction from CSV files...");
    const brandIds = [1, 2];
    const allSentimentData = [];
    for (const brandId of brandIds) {
      const instagramFile = brandId === 1 ? "mands_insta_off.csv" : "next_insta_off.csv";
      const instagramData = await processPlatformData(instagramFile, "instagram", brandId, "caption", "timestamp");
      allSentimentData.push(...instagramData);
      const tiktokFile = brandId === 1 ? "mands_tik_off.csv" : "next_tik_off.csv";
      const tiktokData = await processPlatformData(tiktokFile, "tiktok", brandId, ["description", "text"], "created_time");
      allSentimentData.push(...tiktokData);
      const youtubeFile = brandId === 1 ? "mands_yt_off.csv" : "next_yt_off.csv";
      const youtubeData = await processPlatformData(youtubeFile, "youtube", brandId, ["title", "video_description"], ["publishedAt", "date"]);
      allSentimentData.push(...youtubeData);
    }
    const platforms = ["instagram", "tiktok", "youtube"];
    const sentimentTypes = ["positive", "neutral", "negative"];
    const stats = [];
    for (const platform of platforms) {
      const platformData = allSentimentData.filter((item) => item.platform === platform);
      for (const sentiment of sentimentTypes) {
        const count = platformData.filter((item) => item.sentiment === sentiment).length;
        stats.push({ platform, sentiment, count });
      }
    }
    global.sentimentData = allSentimentData;
    saveSentimentCache(allSentimentData);
    console.log(`Sentiment analysis extraction completed with ${allSentimentData.length} total records!`);
    return {
      success: true,
      message: `Sentiment analysis completed from CSV files (${allSentimentData.length} records)`,
      stats,
      data: allSentimentData
    };
  } catch (error) {
    console.error("Error in sentiment analysis:", error);
    throw error;
  }
}
var init_sentiment_analyzer = __esm({
  "server/sentiment-analyzer.ts"() {
    "use strict";
    init_sentiment_cache();
  }
});

// server/index.ts
import express2 from "express";

// server/routes.ts
import { createServer } from "http";

// server/storage.ts
import fs from "fs";
import path from "path";
import { parse } from "csv-parse/sync";
import fetch from "node-fetch";
if (!global.sentimentData) {
  global.sentimentData = [];
}
function readCSV(filename) {
  try {
    const filePath = path.join(process.cwd(), "public", filename);
    const fileContent = fs.readFileSync(filePath, "utf-8");
    const rows = parse(fileContent, { columns: true, skip_empty_lines: true });
    if (filename.includes("yt_")) {
      return rows.map((row) => {
        if (!row.date && row.title) {
          const dateMatch = row.title.match(/\b\d{4}\b/);
          row.date = dateMatch ? `${dateMatch[0]}-01-01` : (/* @__PURE__ */ new Date()).toISOString().split("T")[0];
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
function isWithinDateRange(dateStr, startDate, endDate) {
  if (!dateStr || !startDate && !endDate) return true;
  try {
    let date;
    if (dateStr.includes("T")) {
      date = new Date(dateStr);
    } else {
      date = /* @__PURE__ */ new Date(dateStr + "T00:00:00");
    }
    if (isNaN(date.getTime())) return true;
    if (startDate) {
      const start = /* @__PURE__ */ new Date(startDate + "T00:00:00");
      if (date < start) return false;
    }
    if (endDate) {
      const end = /* @__PURE__ */ new Date(endDate + "T23:59:59");
      if (date > end) return false;
    }
    return true;
  } catch (error) {
    console.error("Error parsing date:", error);
    return true;
  }
}
function calculateEngagementScore(platform, data) {
  if (!data || data.length === 0) return "0%";
  let totalEngagement = 0;
  let totalReach = 0;
  switch (platform) {
    case "tiktok":
      totalEngagement = data.reduce((sum, row) => sum + Number(row.diggCount || 0) + Number(row.shareCount || 0) + Number(row.commentCount || 0), 0);
      totalReach = data.reduce((sum, row) => sum + Number(row.playCount || 0), 0);
      break;
    case "instagram":
      totalEngagement = data.reduce((sum, row) => sum + Number(row.likesCount || 0) + Number(row.commentsCount || 0), 0);
      totalReach = data.reduce((sum, row) => sum + Number(row.videoViewCount || 0), 0);
      break;
    case "youtube":
      totalEngagement = data.reduce((sum, row) => sum + Number(row.likes || 0) + Number(row.commentsCount || 0), 0);
      totalReach = data.reduce((sum, row) => sum + Number(row.viewCount || 0), 0);
      break;
    default:
      return "0%";
  }
  const engagementRate = totalReach > 0 ? totalEngagement / totalReach * 100 : 0;
  return `${engagementRate.toFixed(1)}%`;
}
function aggregatePosts(brandId) {
  const posts = [];
  const tiktokFiles = brandId === 1 ? ["mands_tik_off.csv", "mands_tik_hash.csv"] : ["next_tik_off.csv", "next_tik_hash.csv"];
  for (const file of tiktokFiles) {
    for (const row of readCSV(file)) {
      const likes = Number(row.diggCount || 0);
      const comments = Number(row.commentCount || 0);
      const reach = Number(row.playCount || 0);
      const validForEngagementRate = likes > 0 && comments > 0 && reach > 0;
      if (likes || comments) {
        posts.push({
          brandId,
          platform: "tiktok",
          likes,
          comments,
          reach,
          totalEngagement: likes + comments,
          engagementRate: validForEngagementRate ? (likes + comments) / reach : null,
          date: row.created_time,
          hashtags: row.hashtags || "",
          mentions: row.mentions || ""
        });
      }
    }
  }
  const instaHash = brandId === 1 ? "mands_insta_hash.csv" : "next_insta_hash.csv";
  const instaOff = brandId === 1 ? "mands_insta_off.csv" : "next_insta_off.csv";
  for (const row of readCSV(instaOff)) {
    let likes = 0;
    if (brandId === 1) likes = Number(row.likesCount || 0);
    const comments = Number(row.commentsCount || 0);
    const reach = Number(row.videoPlayCount || row.videoViewCount || 0);
    const validForEngagementRate = likes > 0 && comments > 0 && reach > 0;
    if (likes || comments) {
      posts.push({
        brandId,
        platform: "instagram",
        likes,
        comments,
        reach,
        totalEngagement: likes + comments,
        engagementRate: validForEngagementRate ? (likes + comments) / reach : null,
        date: row.timestamp,
        hashtags: row.hashtags || "",
        mentions: row.mentions || ""
      });
    }
  }
  for (const row of readCSV(instaHash)) {
    let likes = 0;
    if (brandId === 1 || brandId === 2) likes = Number(row.likesCount || 0);
    const comments = Number(row.commentsCount || 0);
    if (likes || comments) {
      posts.push({
        brandId,
        platform: "instagram",
        likes,
        comments,
        reach: 0,
        totalEngagement: likes + comments,
        engagementRate: null,
        date: row.timestamp,
        hashtags: row.hashtags || "",
        mentions: row.mentions || ""
      });
    }
  }
  const ytFiles = brandId === 1 ? ["mands_yt_off.csv", "mands_yt_hash.csv"] : ["next_yt_off.csv", "next_yt_hash.csv"];
  for (const file of ytFiles) {
    for (const row of readCSV(file)) {
      const likes = Number(row.likes || 0);
      const comments = Number(row.commentsCount || 0);
      const reach = Number(row.viewCount || 0);
      const validForEngagementRate = likes > 0 && comments > 0 && reach > 0;
      if (likes || comments) {
        posts.push({
          brandId,
          platform: "youtube",
          likes,
          comments,
          reach,
          totalEngagement: likes + comments,
          engagementRate: validForEngagementRate ? (likes + comments) / reach : null,
          date: row.date,
          hashtags: row.hashtags || "",
          mentions: ""
          // always zero for YouTube
        });
      }
    }
  }
  return posts;
}
async function analyzeSentimentWithTextBlob(text) {
  if (!text || typeof text !== "string") return { sentiment: "neutral", score: 0.5 };
  try {
    const response = await fetch("http://localhost:5001/analyze", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text })
    });
    if (!response.ok) return { sentiment: "neutral", score: 0.5 };
    return await response.json();
  } catch (error) {
    console.error("TextBlob API error:", error);
    return { sentiment: "neutral", score: 0.5 };
  }
}
async function getKeyTopicsBySentiment(brandId, platform) {
  console.log(`Getting key topics for brandId: ${brandId}, platform: ${platform || "all"}`);
  const files = [
    brandId === 1 ? "mands_insta_off.csv" : "next_insta_off.csv",
    brandId === 1 ? "mands_tik_off.csv" : "next_tik_off.csv",
    brandId === 1 ? "mands_yt_off.csv" : "next_yt_off.csv"
  ];
  const filteredFiles = platform ? files.filter((file) => {
    if (platform === "instagram") return file.includes("insta");
    if (platform === "tiktok") return file.includes("tik");
    if (platform === "youtube") return file.includes("yt");
    return true;
  }) : files;
  const topicData = {};
  for (const file of filteredFiles) {
    const rows = readCSV(file);
    for (const row of rows) {
      if (!row.topic) continue;
      const topic = row.topic.replace(/\"/g, "");
      let text = "";
      if (file.includes("insta")) {
        text = row.caption || "";
      } else if (file.includes("tik")) {
        text = row.description || row.video_description || row.text || "";
      } else if (file.includes("yt")) {
        text = row.title || row.video_description || "";
      }
      const { sentiment } = await analyzeSentimentWithTextBlob(text);
      if (!topicData[topic]) {
        topicData[topic] = {
          positive: 0,
          neutral: 0,
          negative: 0,
          total: 0
        };
      }
      if (sentiment === "positive" || sentiment === "neutral" || sentiment === "negative") {
        topicData[topic][sentiment]++;
        topicData[topic].total++;
      }
    }
  }
  const result = Object.entries(topicData).map(([topic, data]) => {
    const positiveWeight = 100;
    const neutralWeight = 50;
    const negativeWeight = 0;
    let sentimentScore = 0;
    if (data.total > 0) {
      sentimentScore = (data.positive * positiveWeight + data.neutral * neutralWeight + data.negative * negativeWeight) / data.total;
    }
    return {
      topic,
      sentiment_score: sentimentScore
    };
  });
  for (let i = result.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [result[i], result[j]] = [result[j], result[i]];
  }
  return result;
}
async function getPostFrequency(brandId, platform, startDate, endDate) {
  const posts = aggregatePosts(brandId);
  let filteredPosts = posts;
  if (platform) {
    filteredPosts = posts.filter((post) => post.platform === platform.toLowerCase());
  }
  if (startDate || endDate) {
    filteredPosts = filteredPosts.filter((post) => isWithinDateRange(post.date, startDate, endDate));
  }
  const frequencyMap = {};
  filteredPosts.forEach((post) => {
    const date = post.date.split("T")[0];
    frequencyMap[date] = (frequencyMap[date] || 0) + 1;
  });
  return Object.entries(frequencyMap).map(([date, count]) => ({
    date,
    count
  })).sort((a, b) => a.date.localeCompare(b.date));
}
async function getEngagementOverTime(brandId, platform, dateRangeParam) {
  const posts = aggregatePosts(brandId);
  let filteredPosts = posts;
  if (platform && platform !== "all") {
    filteredPosts = posts.filter((post) => post.platform === platform.toLowerCase());
  }
  let latestDateInAllData = /* @__PURE__ */ new Date();
  const datesFromPosts = posts.filter((post) => post.date).map((post) => new Date(post.date));
  if (datesFromPosts.length > 0) {
    latestDateInAllData = new Date(Math.max(...datesFromPosts.map((date) => date.getTime())));
  }
  let startDate;
  if (dateRangeParam && dateRangeParam !== "all") {
    const referenceDate = new Date(latestDateInAllData.getTime());
    switch (dateRangeParam) {
      case "7days":
        startDate = new Date(referenceDate.setDate(referenceDate.getDate() - 7));
        break;
      case "30days":
        startDate = new Date(referenceDate.setDate(referenceDate.getDate() - 30));
        break;
      case "90days":
        startDate = new Date(referenceDate.setDate(referenceDate.getDate() - 90));
        break;
      default:
        startDate = new Date(referenceDate.setDate(referenceDate.getDate() - 30));
    }
    if (startDate) {
      const latestDate = latestDateInAllData;
      filteredPosts = filteredPosts.filter((post) => {
        if (!post.date) return false;
        const postDate = new Date(post.date);
        return postDate >= startDate && postDate <= latestDate;
      });
    }
  }
  let timeBuckets = [];
  const referenceLatestDate = latestDateInAllData;
  if (dateRangeParam === "7days" || dateRangeParam === "7d") {
    let bestStartDate = new Date(referenceLatestDate.getTime());
    bestStartDate.setDate(bestStartDate.getDate() - 6);
    for (let i = 0; i < 7; i++) {
      const bucketEndDate = new Date(bestStartDate.getTime());
      bucketEndDate.setDate(bucketEndDate.getDate() + i);
      const bucketStartDate = new Date(bucketEndDate.getTime());
      bucketStartDate.setHours(0, 0, 0, 0);
      bucketEndDate.setHours(23, 59, 59, 999);
      const dayNames = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
      let dayIndex = bucketStartDate.getDay() - 1;
      if (dayIndex < 0) dayIndex = 6;
      const label = dayNames[dayIndex];
      timeBuckets.unshift({ label, startDate: bucketStartDate, endDate: bucketEndDate });
    }
  } else if (dateRangeParam === "30days" || dateRangeParam === "30d") {
    for (let i = 0; i < 4; i++) {
      const bucketEndDate = new Date(referenceLatestDate.getTime());
      bucketEndDate.setDate(bucketEndDate.getDate() - i * 7);
      const bucketStartDate = new Date(bucketEndDate.getTime());
      bucketStartDate.setDate(bucketStartDate.getDate() - 6);
      bucketStartDate.setHours(0, 0, 0, 0);
      bucketEndDate.setHours(23, 59, 59, 999);
      const label = `Week ${4 - i}`;
      timeBuckets.unshift({ label, startDate: bucketStartDate, endDate: bucketEndDate });
    }
  } else if (dateRangeParam === "90days" || dateRangeParam === "90d") {
    for (let i = 0; i < 3; i++) {
      const bucketEndDate = new Date(referenceLatestDate.getTime());
      bucketEndDate.setMonth(bucketEndDate.getMonth() - i);
      const bucketStartDate = new Date(bucketEndDate.getTime());
      bucketStartDate.setDate(1);
      bucketStartDate.setHours(0, 0, 0, 0);
      bucketEndDate.setDate(new Date(bucketEndDate.getFullYear(), bucketEndDate.getMonth() + 1, 0).getDate());
      bucketEndDate.setHours(23, 59, 59, 999);
      const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
      const label = `${monthNames[bucketStartDate.getMonth()]}`;
      timeBuckets.unshift({ label, startDate: bucketStartDate, endDate: bucketEndDate });
    }
  } else {
    for (let i = 0; i < 6; i++) {
      const bucketEndDate = new Date(referenceLatestDate.getTime());
      bucketEndDate.setMonth(bucketEndDate.getMonth() - i);
      const bucketStartDate = new Date(bucketEndDate.getTime());
      bucketStartDate.setDate(1);
      bucketStartDate.setHours(0, 0, 0, 0);
      bucketEndDate.setDate(new Date(bucketEndDate.getFullYear(), bucketEndDate.getMonth() + 1, 0).getDate());
      bucketEndDate.setHours(23, 59, 59, 999);
      const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
      const label = `${monthNames[bucketStartDate.getMonth()]}`;
      timeBuckets.unshift({ label, startDate: bucketStartDate, endDate: bucketEndDate });
    }
  }
  const engagementData = timeBuckets.map((bucket) => {
    const postsInBucket = filteredPosts.filter((post) => {
      if (!post.date) return false;
      const postDate = new Date(post.date);
      return postDate >= bucket.startDate && postDate <= bucket.endDate;
    });
    let likes = 0;
    let comments = 0;
    let shares = 0;
    postsInBucket.forEach((post) => {
      const postLikes = typeof post.likes === "number" ? post.likes : parseInt(post.likes || "0", 10) || 0;
      const postComments = typeof post.comments === "number" ? post.comments : parseInt(post.comments || "0", 10) || 0;
      const postShares = typeof post.shares === "number" ? post.shares : parseInt(post.shares || "0", 10) || 0;
      likes += postLikes;
      comments += postComments;
      shares += postShares;
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
var storage = {
  // Example: getBrands returns static brands based on available CSVs
  async getBrands() {
    return [
      { id: 1, name: "Marks & Spencer", slug: "marks-spencer", industry: "Retail", description: "Marks & Spencer", createdAt: /* @__PURE__ */ new Date() },
      { id: 2, name: "Next Retail", slug: "next-retail", industry: "Retail", description: "Next Retail", createdAt: /* @__PURE__ */ new Date() }
    ];
  },
  async getBrandBySlug(slug) {
    const brands = await this.getBrands();
    return brands.find((b) => b.slug === slug);
  },
  async getSocialMetrics(brandId, startDate, endDate, platform) {
    if (brandId === 1) {
      if (!platform || platform === "all") {
        let tiktokOfficial = readCSV("mands_tik_off.csv");
        let tiktokHashtag = readCSV("mands_tik_hash.csv");
        let allTiktok = [...tiktokOfficial, ...tiktokHashtag];
        if (startDate || endDate) {
          allTiktok = allTiktok.filter((row) => isWithinDateRange(row.created_time, startDate, endDate));
        }
        let instagramOfficial = readCSV("mands_insta_off.csv");
        let instagramHashtag = readCSV("mands_insta_hash.csv");
        let allInstagram = [...instagramOfficial, ...instagramHashtag];
        if (startDate || endDate) {
          allInstagram = allInstagram.filter((row) => isWithinDateRange(row.timestamp, startDate, endDate));
        }
        let youtubeOfficial = readCSV("mands_yt_off.csv");
        let youtubeHashtag = readCSV("mands_yt_hash.csv");
        let allYoutube = [...youtubeOfficial, ...youtubeHashtag];
        if (startDate || endDate) {
          allYoutube = allYoutube.filter((row) => isWithinDateRange(row.date, startDate, endDate));
        }
        const totalPosts = allTiktok.length + allInstagram.length + allYoutube.length;
        const tiktokLikes = allTiktok.reduce((a, b) => a + Number(b.diggCount || 0), 0);
        const tiktokComments = allTiktok.reduce((a, b) => a + Number(b.commentCount || 0), 0);
        const tiktokShares = allTiktok.reduce((a, b) => a + Number(b.shareCount || 0), 0);
        const instagramLikes = allInstagram.reduce((a, b) => a + Number(b.likesCount || 0), 0);
        const instagramComments = allInstagram.reduce((a, b) => a + Number(b.commentsCount || 0), 0);
        const youtubeLikes = allYoutube.reduce((a, b) => a + Number(b.likes || 0), 0);
        const youtubeComments = allYoutube.reduce((a, b) => a + Number(b.commentsCount || 0), 0);
        const youtubeViews = allYoutube.reduce((a, b) => a + Number(b.viewCount || 0), 0);
        const totalLikes = tiktokLikes + instagramLikes + youtubeLikes;
        const totalComments = tiktokComments + instagramComments + youtubeComments;
        const totalShares = tiktokShares;
        const totalReach = allTiktok.reduce((a, b) => a + Number(b.playCount || 0), 0) + allInstagram.reduce((a, b) => a + Number(b.videoViewCount || 0), 0) + youtubeViews;
        const engagementScore = ((tiktokLikes + tiktokComments + instagramLikes + instagramComments + youtubeLikes + youtubeComments) / (totalReach || 1) * 100).toFixed(1) + "%";
        return [{
          id: 1,
          brandId,
          platform: "all",
          date: (/* @__PURE__ */ new Date()).toISOString().split("T")[0],
          totalPosts,
          followers: 0,
          mentions: totalPosts,
          likes: totalLikes,
          shares: totalShares,
          comments: totalComments,
          reach: totalReach,
          engagementScore,
          avgResponseTimeHours: "2",
          createdAt: /* @__PURE__ */ new Date()
        }];
      } else if (platform === "tiktok") {
        let tiktokOfficial = readCSV("mands_tik_off.csv");
        let tiktokHashtag = readCSV("mands_tik_hash.csv");
        let allTiktok = [...tiktokOfficial, ...tiktokHashtag];
        if (startDate || endDate) {
          allTiktok = allTiktok.filter((row) => isWithinDateRange(row.created_time, startDate, endDate));
        }
        const tiktokLikes = allTiktok.reduce((a, b) => a + Number(b.diggCount || 0), 0);
        const tiktokComments = allTiktok.reduce((a, b) => a + Number(b.commentCount || 0), 0);
        const tiktokShares = allTiktok.reduce((a, b) => a + Number(b.shareCount || 0), 0);
        const tiktokReach = allTiktok.reduce((a, b) => a + Number(b.playCount || 0), 0);
        const engagementScore = ((tiktokLikes + tiktokComments) / (tiktokReach || 1) * 100).toFixed(1) + "%";
        return [{
          id: 1,
          brandId,
          platform: "tiktok",
          date: (/* @__PURE__ */ new Date()).toISOString().split("T")[0],
          totalPosts: allTiktok.length,
          followers: 0,
          mentions: allTiktok.length,
          likes: tiktokLikes,
          shares: tiktokShares,
          comments: tiktokComments,
          reach: tiktokReach,
          engagementScore,
          avgResponseTimeHours: "1",
          createdAt: /* @__PURE__ */ new Date()
        }];
      } else if (platform === "instagram") {
        let instagramOfficial = readCSV("mands_insta_off.csv");
        let instagramHashtag = readCSV("mands_insta_hash.csv");
        let allInstagram = [...instagramOfficial, ...instagramHashtag];
        if (startDate || endDate) {
          allInstagram = allInstagram.filter((row) => isWithinDateRange(row.timestamp, startDate, endDate));
        }
        return [{
          id: 2,
          brandId,
          platform: "instagram",
          date: (/* @__PURE__ */ new Date()).toISOString().split("T")[0],
          totalPosts: allInstagram.length,
          followers: 0,
          mentions: allInstagram.length,
          likes: allInstagram.reduce((a, b) => a + Number(b.likesCount || 0), 0),
          shares: 0,
          // Instagram doesn't have shares in the CSV
          comments: allInstagram.reduce((a, b) => a + Number(b.commentsCount || 0), 0),
          reach: allInstagram.reduce((a, b) => a + Number(b.videoViewCount || 0), 0),
          engagementScore: calculateEngagementScore("instagram", allInstagram),
          avgResponseTimeHours: "1",
          createdAt: /* @__PURE__ */ new Date()
        }];
      } else if (platform === "youtube") {
        let youtubeOfficial = readCSV("mands_yt_off.csv");
        let youtubeHashtag = readCSV("mands_yt_hash.csv");
        let allYoutube = [...youtubeOfficial, ...youtubeHashtag];
        if (startDate || endDate) {
          allYoutube = allYoutube.filter((row) => isWithinDateRange(row.date, startDate, endDate));
        }
        return [{
          id: 3,
          brandId,
          platform: "youtube",
          date: (/* @__PURE__ */ new Date()).toISOString().split("T")[0],
          totalPosts: allYoutube.length,
          followers: 0,
          mentions: allYoutube.length,
          likes: allYoutube.reduce((a, b) => a + Number(b.likes || 0), 0),
          shares: 0,
          comments: allYoutube.reduce((a, b) => a + Number(b.commentsCount || 0), 0),
          reach: allYoutube.reduce((a, b) => a + Number(b.viewCount || 0), 0),
          engagementScore: calculateEngagementScore("youtube", allYoutube),
          avgResponseTimeHours: "1",
          createdAt: /* @__PURE__ */ new Date()
        }];
      }
    } else if (brandId === 2) {
      if (!platform || platform === "all") {
        let tiktokOfficial = readCSV("next_tik_off.csv");
        let tiktokHashtag = readCSV("next_tik_hash.csv");
        let allTiktok = [...tiktokOfficial, ...tiktokHashtag];
        if (startDate || endDate) {
          allTiktok = allTiktok.filter((row) => isWithinDateRange(row.created_time, startDate, endDate));
        }
        let instagramOfficial = readCSV("next_insta_off.csv");
        let instagramHashtag = readCSV("next_insta_hash.csv");
        let allInstagram = [...instagramOfficial, ...instagramHashtag];
        if (startDate || endDate) {
          allInstagram = allInstagram.filter((row) => isWithinDateRange(row.timestamp, startDate, endDate));
        }
        let youtubeOfficial = readCSV("next_yt_off.csv");
        let youtubeHashtag = readCSV("next_yt_hash.csv");
        let allYoutube = [...youtubeOfficial, ...youtubeHashtag];
        if (startDate || endDate) {
          allYoutube = allYoutube.filter((row) => isWithinDateRange(row.date, startDate, endDate));
        }
        const totalPosts = allTiktok.length + allInstagram.length + allYoutube.length;
        const tiktokLikes = allTiktok.reduce((a, b) => a + Number(b.diggCount || 0), 0);
        const tiktokComments = allTiktok.reduce((a, b) => a + Number(b.commentCount || 0), 0);
        const tiktokShares = allTiktok.reduce((a, b) => a + Number(b.shareCount || 0), 0);
        const instagramLikes = allInstagram.reduce((a, b) => a + Number(b.likesCount || 0), 0);
        const instagramComments = allInstagram.reduce((a, b) => a + Number(b.commentsCount || 0), 0);
        const youtubeLikes = allYoutube.reduce((a, b) => a + Number(b.likes || 0), 0);
        const youtubeComments = allYoutube.reduce((a, b) => a + Number(b.commentsCount || 0), 0);
        const totalLikes = tiktokLikes + instagramLikes + youtubeLikes;
        const totalComments = tiktokComments + instagramComments + youtubeComments;
        const totalShares = tiktokShares;
        const totalReach = allTiktok.reduce((a, b) => a + Number(b.playCount || 0), 0) + allInstagram.reduce((a, b) => a + Number(b.videoViewCount || 0), 0) + allYoutube.reduce((a, b) => a + Number(b.viewCount || 0), 0);
        const engagementScore = ((tiktokLikes + tiktokComments + instagramLikes + instagramComments + youtubeLikes + youtubeComments) / (totalReach || 1) * 100).toFixed(1) + "%";
        return [{
          id: 1,
          brandId,
          platform: "all",
          date: (/* @__PURE__ */ new Date()).toISOString().split("T")[0],
          totalPosts,
          followers: 0,
          mentions: totalPosts,
          likes: totalLikes,
          shares: totalShares,
          comments: totalComments,
          reach: totalReach,
          engagementScore,
          avgResponseTimeHours: "2",
          createdAt: /* @__PURE__ */ new Date()
        }];
      } else if (platform === "tiktok") {
        let tiktokOfficial = readCSV("next_tik_off.csv");
        let tiktokHashtag = readCSV("next_tik_hash.csv");
        let allTiktok = [...tiktokOfficial, ...tiktokHashtag];
        if (startDate || endDate) {
          allTiktok = allTiktok.filter((row) => isWithinDateRange(row.created_time, startDate, endDate));
        }
        return [{
          id: 1,
          brandId,
          platform: "tiktok",
          date: (/* @__PURE__ */ new Date()).toISOString().split("T")[0],
          totalPosts: allTiktok.length,
          followers: 0,
          mentions: allTiktok.length,
          likes: allTiktok.reduce((a, b) => a + Number(b.diggCount || 0), 0),
          shares: allTiktok.reduce((a, b) => a + Number(b.shareCount || 0), 0),
          comments: allTiktok.reduce((a, b) => a + Number(b.commentCount || 0), 0),
          reach: allTiktok.reduce((a, b) => a + Number(b.playCount || 0), 0),
          engagementScore: calculateEngagementScore("tiktok", allTiktok),
          avgResponseTimeHours: "1",
          createdAt: /* @__PURE__ */ new Date()
        }];
      } else if (platform === "instagram") {
        let instagramOfficial = readCSV("next_insta_off.csv");
        let instagramHashtag = readCSV("next_insta_hash.csv");
        let allInstagram = [...instagramOfficial, ...instagramHashtag];
        if (startDate || endDate) {
          allInstagram = allInstagram.filter((row) => isWithinDateRange(row.timestamp, startDate, endDate));
        }
        return [{
          id: 2,
          brandId,
          platform: "instagram",
          date: (/* @__PURE__ */ new Date()).toISOString().split("T")[0],
          totalPosts: allInstagram.length,
          followers: 0,
          mentions: allInstagram.length,
          likes: allInstagram.reduce((a, b) => a + Number(b.likesCount || 0), 0),
          shares: 0,
          comments: allInstagram.reduce((a, b) => a + Number(b.commentsCount || 0), 0),
          reach: allInstagram.reduce((a, b) => a + Number(b.videoViewCount || 0), 0),
          engagementScore: calculateEngagementScore("instagram", allInstagram),
          avgResponseTimeHours: "1",
          createdAt: /* @__PURE__ */ new Date()
        }];
      } else if (platform === "youtube") {
        let youtubeOfficial = readCSV("next_yt_off.csv");
        let youtubeHashtag = readCSV("next_yt_hash.csv");
        let allYoutube = [...youtubeOfficial, ...youtubeHashtag];
        if (startDate || endDate) {
          allYoutube = allYoutube.filter((row) => isWithinDateRange(row.date, startDate, endDate));
        }
        return [{
          id: 3,
          brandId,
          platform: "youtube",
          date: (/* @__PURE__ */ new Date()).toISOString().split("T")[0],
          totalPosts: allYoutube.length,
          followers: 0,
          mentions: allYoutube.length,
          likes: allYoutube.reduce((a, b) => a + Number(b.likes || 0), 0),
          shares: 0,
          comments: allYoutube.reduce((a, b) => a + Number(b.commentsCount || 0), 0),
          reach: allYoutube.reduce((a, b) => a + Number(b.viewCount || 0), 0),
          engagementScore: calculateEngagementScore("youtube", allYoutube),
          avgResponseTimeHours: "1",
          createdAt: /* @__PURE__ */ new Date()
        }];
      }
    }
    return [];
  },
  async getTopContent(brandId, limit = 10, platform, startDate, endDate) {
    let allContent = [];
    if (brandId === 1) {
      if (!platform || platform === "tiktok") {
        const tiktokOfficial = readCSV("mands_tik_off.csv");
        const tiktokHashtag = readCSV("mands_tik_hash.csv");
        let allTiktok = [...tiktokOfficial, ...tiktokHashtag];
        if (startDate || endDate) {
          allTiktok = allTiktok.filter((row) => isWithinDateRange(row.created_time, startDate, endDate));
        }
        allContent = allContent.concat(allTiktok.map((row) => ({
          brandId,
          platform: "tiktok",
          content: row.text || "",
          postType: "video",
          publishedAt: row.created_time || (/* @__PURE__ */ new Date()).toISOString(),
          views: Number(row.playCount || 0),
          likes: Number(row.diggCount || 0),
          comments: Number(row.commentCount || 0),
          shares: Number(row.shareCount || 0),
          engagementRate: 0,
          createdAt: /* @__PURE__ */ new Date()
        })));
      }
      if (!platform || platform === "instagram") {
        const instagramOfficial = readCSV("mands_insta_off.csv");
        const instagramHashtag = readCSV("mands_insta_hash.csv");
        let allInstagram = [...instagramOfficial, ...instagramHashtag];
        if (startDate || endDate) {
          allInstagram = allInstagram.filter((row) => isWithinDateRange(row.timestamp, startDate, endDate));
        }
        allContent = allContent.concat(allInstagram.map((row) => ({
          brandId,
          platform: "instagram",
          content: row.caption || "",
          postType: "image",
          publishedAt: row.timestamp || (/* @__PURE__ */ new Date()).toISOString(),
          views: Number(row.videoViewCount || 0),
          likes: Number(row.likesCount || 0),
          comments: Number(row.commentsCount || 0),
          shares: 0,
          engagementRate: 0,
          createdAt: /* @__PURE__ */ new Date()
        })));
      }
      if (!platform || platform === "youtube") {
        const youtubeOfficial = readCSV("mands_yt_off.csv");
        const youtubeHashtag = readCSV("mands_yt_hash.csv");
        let allYoutube = [...youtubeOfficial, ...youtubeHashtag];
        if (startDate || endDate) {
          allYoutube = allYoutube.filter((row) => isWithinDateRange(row.date, startDate, endDate));
        }
        allContent = allContent.concat(allYoutube.map((row) => ({
          brandId,
          platform: "youtube",
          content: row.video_description || "",
          postType: "video",
          publishedAt: row.date || (/* @__PURE__ */ new Date()).toISOString(),
          views: Number(row.view_count || 0),
          likes: Number(row.likes || 0),
          comments: Number(row.commentsCount || 0),
          shares: 0,
          engagementRate: 0,
          createdAt: /* @__PURE__ */ new Date()
        })));
      }
    } else if (brandId === 2) {
      if (!platform || platform === "tiktok") {
        const tiktokOfficial = readCSV("next_tik_off.csv");
        const tiktokHashtag = readCSV("next_tik_hash.csv");
        let allTiktok = [...tiktokOfficial, ...tiktokHashtag];
        if (startDate || endDate) {
          allTiktok = allTiktok.filter((row) => isWithinDateRange(row.created_time, startDate, endDate));
        }
        allContent = allContent.concat(allTiktok.map((row) => ({
          brandId,
          platform: "tiktok",
          content: row.text || "",
          postType: "video",
          publishedAt: row.created_time || (/* @__PURE__ */ new Date()).toISOString(),
          views: Number(row.playCount || 0),
          likes: Number(row.diggCount || 0),
          comments: Number(row.commentCount || 0),
          shares: Number(row.shareCount || 0),
          engagementRate: 0,
          createdAt: /* @__PURE__ */ new Date()
        })));
      }
      if (!platform || platform === "instagram") {
        const instagramOfficial = readCSV("next_insta_off.csv");
        const instagramHashtag = readCSV("next_insta_hash.csv");
        let allInstagram = [...instagramOfficial, ...instagramHashtag];
        if (startDate || endDate) {
          allInstagram = allInstagram.filter((row) => isWithinDateRange(row.timestamp, startDate, endDate));
        }
        allContent = allContent.concat(allInstagram.map((row) => ({
          brandId,
          platform: "instagram",
          content: row.caption || "",
          postType: "image",
          publishedAt: row.timestamp || (/* @__PURE__ */ new Date()).toISOString(),
          views: Number(row.videoViewCount || 0),
          likes: Number(row.likesCount || 0),
          comments: Number(row.commentsCount || 0),
          shares: 0,
          engagementRate: 0,
          createdAt: /* @__PURE__ */ new Date()
        })));
      }
      if (!platform || platform === "youtube") {
        let youtubeOfficial = readCSV("next_yt_off.csv");
        let youtubeHashtag = readCSV("next_yt_hash.csv");
        let allYoutube = [...youtubeOfficial, ...youtubeHashtag];
        if (startDate || endDate) {
          allYoutube = allYoutube.filter((row) => isWithinDateRange(row.date, startDate, endDate));
        }
        allContent = allContent.concat(allYoutube.map((row) => ({
          brandId,
          platform: "youtube",
          content: row.video_description || "",
          postType: "video",
          publishedAt: row.date || (/* @__PURE__ */ new Date()).toISOString(),
          views: Number(row.viewCount || 0),
          likes: Number(row.likes || 0),
          comments: Number(row.commentsCount || 0),
          shares: 0,
          engagementRate: 0,
          createdAt: /* @__PURE__ */ new Date()
        })));
      }
    }
    allContent = allContent.sort((a, b) => b.views + b.likes + b.comments + b.shares - (a.views + a.likes + a.comments + a.shares));
    allContent = allContent.map((row, i) => ({ ...row, id: i + 1 }));
    return allContent.slice(0, limit);
  },
  async getBrandHashtags(brandId, limit = 10, platform, dateRange) {
    const posts = await aggregatePosts(brandId);
    let filteredPosts = posts;
    if (platform && platform !== "all") {
      filteredPosts = filteredPosts.filter((post) => post.platform === platform);
    }
    if (dateRange) {
      const now = /* @__PURE__ */ new Date();
      let startDate;
      switch (dateRange) {
        case "7days":
        case "7d":
          startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1e3).toISOString();
          break;
        case "30days":
        case "30d":
          startDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1e3).toISOString();
          break;
        case "90days":
        case "90d":
          startDate = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1e3).toISOString();
          break;
        default:
          startDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1e3).toISOString();
      }
      filteredPosts = filteredPosts.filter((post) => isWithinDateRange(post.date, startDate, now.toISOString()));
    }
    const hashtagCounts = /* @__PURE__ */ new Map();
    filteredPosts.forEach((post) => {
      if (post.hashtags) {
        const hashtags = post.hashtags.split(",").map((h) => h.trim());
        hashtags.forEach((hashtag) => {
          if (hashtag) {
            hashtagCounts.set(hashtag, (hashtagCounts.get(hashtag) || 0) + 1);
          }
        });
      }
    });
    return Array.from(hashtagCounts.entries()).map(([hashtag, usageCount], i) => ({
      id: i + 1,
      brandId,
      platform: platform || "all",
      hashtag,
      usageCount,
      engagementRate: 0,
      createdAt: /* @__PURE__ */ new Date()
    })).sort((a, b) => b.usageCount - a.usageCount).slice(0, limit);
  },
  async getIndustryHashtags() {
    const hashtagFiles = [
      "mands_tik_hash.csv",
      "mands_insta_hash.csv",
      "next_tik_hash.csv",
      "next_insta_hash.csv"
    ];
    const hashtagCounts = {};
    for (const file of hashtagFiles) {
      const rows = readCSV(file);
      rows.forEach((row) => {
        if (row.hashtags) {
          const hashtags = row.hashtags.split(",").map((h) => h.trim());
          hashtags.forEach((hashtag) => {
            if (hashtag) {
              hashtagCounts[hashtag] = (hashtagCounts[hashtag] || 0) + 1;
            }
          });
        }
      });
    }
    return Object.entries(hashtagCounts).map(([hashtag, count], i) => ({
      id: i + 1,
      hashtag,
      usageCount: count,
      engagementRate: 0,
      createdAt: /* @__PURE__ */ new Date()
    })).sort((a, b) => b.usageCount - a.usageCount).slice(0, 20);
  },
  async getAudienceDemographics(brandId, date, platform) {
    return [];
  },
  async getSentimentData(brandId, startDate, endDate, platform) {
    try {
      if (global.sentimentData && global.sentimentData.length > 0) {
        console.log(`Using cached sentiment data (${global.sentimentData.length} records)`);
        let filteredData = global.sentimentData.filter((item) => item.brandId === brandId);
        if (platform && platform !== "all") {
          filteredData = filteredData.filter((item) => item.platform === platform);
        }
        if (startDate || endDate) {
          filteredData = filteredData.filter(
            (item) => isWithinDateRange(item.date.toISOString(), startDate, endDate)
          );
        }
        console.log(`Returning ${filteredData.length} filtered sentiment records`);
        return filteredData;
      }
      console.log("No cached sentiment data found, analyzing on-the-fly");
      const sentimentData = [];
      const instagramFile = brandId === 1 ? "mands_insta_off.csv" : "next_insta_off.csv";
      const tiktokFile = brandId === 1 ? "mands_tik_off.csv" : "next_tik_off.csv";
      const youtubeFile = brandId === 1 ? "mands_yt_off.csv" : "next_yt_off.csv";
      if (!platform || platform === "instagram" || platform === "all") {
        for (const post of readCSV(instagramFile)) {
          const text = post.caption || "";
          if (!text) continue;
          const { sentiment, score, subjectivity } = await analyzeSentimentWithTextBlob(text);
          const date = new Date(post.timestamp || Date.now());
          if (!isWithinDateRange(post.timestamp, startDate, endDate)) continue;
          sentimentData.push({
            brandId,
            platform: "instagram",
            sentiment,
            score,
            subjectivity: subjectivity || 0.5,
            mentionCount: 1,
            text: text.substring(0, 100),
            // Store a snippet of the text
            date,
            createdAt: date
          });
        }
      }
      if (!platform || platform === "tiktok" || platform === "all") {
        for (const video of readCSV(tiktokFile)) {
          const text = video.description || video.text || "";
          if (!text) continue;
          const { sentiment, score, subjectivity } = await analyzeSentimentWithTextBlob(text);
          const date = new Date(video.created_time || Date.now());
          if (!isWithinDateRange(video.created_time, startDate, endDate)) continue;
          sentimentData.push({
            brandId,
            platform: "tiktok",
            sentiment,
            score,
            subjectivity: subjectivity || 0.5,
            mentionCount: 1,
            text: text.substring(0, 100),
            date,
            createdAt: date
          });
        }
      }
      if (!platform || platform === "youtube" || platform === "all") {
        for (const video of readCSV(youtubeFile)) {
          const text = video.title || video.video_description || "";
          if (!text) continue;
          const { sentiment, score, subjectivity } = await analyzeSentimentWithTextBlob(text);
          const date = new Date(video.publishedAt || video.date || Date.now());
          if (!isWithinDateRange(video.publishedAt || video.date, startDate, endDate)) continue;
          sentimentData.push({
            brandId,
            platform: "youtube",
            sentiment,
            score,
            subjectivity: subjectivity || 0.5,
            mentionCount: 1,
            text: text.substring(0, 100),
            date,
            createdAt: date
          });
        }
      }
      console.log(`Generated ${sentimentData.length} sentiment records on-the-fly`);
      return sentimentData;
    } catch (error) {
      console.error("Error fetching sentiment data:", error);
      return [];
    }
  },
  async getContentStrategy(brandId, platform, dateRangeParam) {
    const posts = aggregatePosts(brandId);
    let latestDateInAllData = null;
    posts.forEach((post) => {
      if (post.date) {
        const postDate = new Date(post.date);
        if (!latestDateInAllData || postDate > latestDateInAllData) {
          latestDateInAllData = postDate;
        }
      }
    });
    if (!latestDateInAllData) {
      latestDateInAllData = /* @__PURE__ */ new Date();
    }
    let filteredPosts = posts;
    if (platform && platform !== "all") {
      filteredPosts = posts.filter((post) => post.platform === platform.toLowerCase());
    }
    let startDate;
    if (dateRangeParam && dateRangeParam !== "all") {
      const referenceDate = new Date(latestDateInAllData.getTime());
      switch (dateRangeParam) {
        case "7days":
          startDate = new Date(referenceDate.setDate(referenceDate.getDate() - 7));
          break;
        case "30days":
          startDate = new Date(referenceDate.setDate(referenceDate.getDate() - 30));
          break;
        case "90days":
          startDate = new Date(referenceDate.setDate(referenceDate.getDate() - 90));
          break;
        default:
          startDate = new Date(referenceDate.setDate(referenceDate.getDate() - 30));
      }
      if (startDate) {
        filteredPosts = filteredPosts.filter((post) => {
          if (!post.date) return false;
          const postDate = new Date(post.date);
          return postDate >= startDate && postDate <= latestDateInAllData;
        });
      }
    }
    const totalPosts = filteredPosts.length;
    let earliestDate = null;
    let latestDate = null;
    if (filteredPosts.length > 0) {
      filteredPosts.forEach((post) => {
        if (post.date) {
          const postDate = new Date(post.date);
          if (!earliestDate || postDate < earliestDate) {
            earliestDate = postDate;
          }
          if (!latestDate || postDate > latestDate) {
            latestDate = postDate;
          }
        }
      });
    }
    let daysInRange = 1;
    if (earliestDate && latestDate) {
      const early = earliestDate;
      const late = latestDate;
      daysInRange = Math.max(1, Math.ceil((late.getTime() - early.getTime()) / (1e3 * 60 * 60 * 24)) + 1);
    } else if (dateRangeParam && dateRangeParam !== "all") {
      daysInRange = dateRangeParam === "7days" ? 7 : dateRangeParam === "30days" ? 30 : dateRangeParam === "90days" ? 90 : 30;
    } else {
      daysInRange = 180;
    }
    const postsPerDay = totalPosts > 0 ? totalPosts / daysInRange : 0;
    const contentTypes = filteredPosts.reduce((acc, post) => {
      const type = post.platform === "youtube" ? "video" : post.platform === "instagram" ? "image" : "video";
      if (!acc[type]) {
        acc[type] = { count: 0, type };
      }
      acc[type].count++;
      return acc;
    }, {});
    const contentTypesArray = Object.values(contentTypes).map((type) => ({
      type: type.type,
      count: type.count,
      percentage: Math.round(type.count / totalPosts * 100)
    }));
    return {
      postsPerDay: parseFloat(postsPerDay.toFixed(2)),
      totalPosts,
      contentTypes: contentTypesArray
    };
  },
  async getAudienceOverlap(brand1Id, brand2Id, platform, dateRange) {
    const brand1Posts = aggregatePosts(brand1Id);
    const brand2Posts = aggregatePosts(brand2Id);
    let filteredBrand1Posts = brand1Posts;
    let filteredBrand2Posts = brand2Posts;
    if (platform && platform !== "all") {
      filteredBrand1Posts = brand1Posts.filter((post) => post.platform === platform.toLowerCase());
      filteredBrand2Posts = brand2Posts.filter((post) => post.platform === platform.toLowerCase());
    }
    if (dateRange && dateRange !== "all") {
      const now = /* @__PURE__ */ new Date();
      let startDate;
      switch (dateRange) {
        case "7days":
          startDate = new Date(now.setDate(now.getDate() - 7));
          break;
        case "30days":
          startDate = new Date(now.setDate(now.getDate() - 30));
          break;
        case "90days":
          startDate = new Date(now.setDate(now.getDate() - 90));
          break;
        default:
          startDate = new Date(now.setDate(now.getDate() - 30));
      }
      filteredBrand1Posts = filteredBrand1Posts.filter((post) => new Date(post.date) >= startDate);
      filteredBrand2Posts = filteredBrand2Posts.filter((post) => new Date(post.date) >= startDate);
    }
    const brand1Keywords = /* @__PURE__ */ new Set();
    const brand2Keywords = /* @__PURE__ */ new Set();
    filteredBrand1Posts.forEach((post) => {
      const text = `${post.hashtags || ""} ${post.mentions || ""}`;
      const keywords = text.toLowerCase().split(/\s+/).filter((word) => word.length > 0);
      keywords.forEach((keyword) => brand1Keywords.add(keyword));
    });
    filteredBrand2Posts.forEach((post) => {
      const text = `${post.hashtags || ""} ${post.mentions || ""}`;
      const keywords = text.toLowerCase().split(/\s+/).filter((word) => word.length > 0);
      keywords.forEach((keyword) => brand2Keywords.add(keyword));
    });
    const commonKeywords = /* @__PURE__ */ new Set();
    brand1Keywords.forEach((keyword) => {
      if (brand2Keywords.has(keyword)) {
        commonKeywords.add(keyword);
      }
    });
    const totalUniqueKeywords = brand1Keywords.size + brand2Keywords.size - commonKeywords.size;
    const overlapPercentage = totalUniqueKeywords > 0 ? Math.round(commonKeywords.size / totalUniqueKeywords * 100) : 0;
    const commonHashtags = Array.from(commonKeywords).filter((keyword) => keyword.startsWith("#")).slice(0, 5);
    return {
      overlapPercentage,
      commonHashtags
    };
  },
  // Get engagement data over time from CSV files
  async getEngagementOverTime(brandId, platform, dateRangeParam) {
    return getEngagementOverTime(brandId, platform, dateRangeParam);
  },
  // Get social media metrics for KPI cards based on real data
  async getSocialMetrics(brandId, startDate, endDate, platform) {
    console.log(`Getting real social metrics for brand ${brandId}, platform: ${platform || "all"}, date range: ${startDate} to ${endDate}`);
    const posts = aggregatePosts(brandId);
    let latestDateInAllData = null;
    posts.forEach((post) => {
      if (post.date) {
        const postDate = new Date(post.date);
        if (!latestDateInAllData || postDate > latestDateInAllData) {
          latestDateInAllData = postDate;
        }
      }
    });
    if (!latestDateInAllData) {
      latestDateInAllData = /* @__PURE__ */ new Date();
    }
    let filteredPosts = posts;
    if (platform && platform !== "all") {
      filteredPosts = posts.filter((post) => post.platform === platform.toLowerCase());
    }
    let dateRange = "";
    if (startDate) {
      if (startDate === "7days") dateRange = "7days";
      else if (startDate === "30days") dateRange = "30days";
      else if (startDate === "90days") dateRange = "90days";
    }
    if (dateRange) {
      const referenceDate = new Date(latestDateInAllData.getTime());
      let startDateFilter;
      switch (dateRange) {
        case "7days":
          startDateFilter = new Date(referenceDate.setDate(referenceDate.getDate() - 7));
          break;
        case "30days":
          startDateFilter = new Date(referenceDate.setDate(referenceDate.getDate() - 30));
          break;
        case "90days":
          startDateFilter = new Date(referenceDate.setDate(referenceDate.getDate() - 90));
          break;
        default:
          startDateFilter = new Date(referenceDate.setDate(referenceDate.getDate() - 30));
      }
      filteredPosts = filteredPosts.filter((post) => {
        if (!post.date) return false;
        const postDate = new Date(post.date);
        return postDate >= startDateFilter && postDate <= latestDateInAllData;
      });
    }
    const totalPosts = filteredPosts.length;
    let totalLikes = 0;
    let totalComments = 0;
    let totalShares = 0;
    let totalReach = 0;
    filteredPosts.forEach((post) => {
      totalLikes += Number(post.likes || 0);
      totalComments += Number(post.comments || 0);
      totalShares += Number(post.shares || 0);
      totalReach += Number(post.reach || 0);
    });
    let engagementRate = 0;
    if (totalReach > 0) {
      engagementRate = (totalLikes + totalComments + totalShares) / totalReach * 100;
    }
    const metrics = [{
      platform: platform || "all",
      mentions: totalPosts,
      // Use total posts count as mentions
      reach: totalReach,
      likes: totalLikes,
      comments: totalComments,
      shares: totalShares,
      engagementScore: `${engagementRate.toFixed(1)}%`,
      totalPosts
    }];
    console.log("Real metrics calculated:", metrics[0]);
    return metrics;
  }
};

// server/auth.ts
import passport from "passport";
import { Strategy as LocalStrategy } from "passport-local";
import session from "express-session";
function setupAuth(app2) {
  const sessionSettings = {
    secret: process.env.SESSION_SECRET || "csv-only-secret-key",
    resave: false,
    saveUninitialized: false,
    cookie: {
      secure: false,
      // Set to true in production with HTTPS
      maxAge: 24 * 60 * 60 * 1e3
      // 24 hours
    }
  };
  app2.set("trust proxy", 1);
  app2.use(session(sessionSettings));
  app2.use(passport.initialize());
  app2.use(passport.session());
  passport.use(
    new LocalStrategy(
      { usernameField: "email" },
      async (email, password, done) => {
        try {
          const user = {
            id: 1,
            email: email || "user@example.com",
            firstName: "Demo",
            lastName: "User"
          };
          return done(null, user);
        } catch (error) {
          return done(error);
        }
      }
    )
  );
  passport.serializeUser((user, done) => done(null, user.id));
  passport.deserializeUser(async (id, done) => {
    try {
      const user = {
        id: 1,
        email: "admin@example.com",
        firstName: "Admin",
        lastName: "User"
      };
      done(null, user);
    } catch (error) {
      done(error);
    }
  });
  app2.post("/api/register", async (req, res, next) => {
    try {
      const { email, password, firstName, lastName } = req.body;
      if (!email || !password || !firstName || !lastName) {
        return res.status(400).json({ message: "All fields are required" });
      }
      const user = {
        id: 1,
        email,
        firstName,
        lastName
      };
      req.login(user, (err) => {
        if (err) {
          console.error("Login error after registration:", err);
          return res.status(500).json({ message: "Registration successful but login failed" });
        }
        res.status(201).json({
          id: user.id,
          email: user.email,
          firstName: user.firstName,
          lastName: user.lastName
        });
      });
    } catch (error) {
      console.error("Registration error:", error);
      res.status(500).json({ message: "Registration failed", error: String(error) });
    }
  });
  app2.post("/api/login", passport.authenticate("local"), (req, res) => {
    const user = req.user;
    res.status(200).json({
      id: user.id,
      email: user.email,
      firstName: user.firstName,
      lastName: user.lastName
    });
  });
  app2.post("/api/logout", (req, res, next) => {
    req.logout((err) => {
      if (err) return next(err);
      req.session.destroy((destroyErr) => {
        if (destroyErr) return next(destroyErr);
        res.clearCookie("connect.sid");
        res.status(200).json({ message: "Logged out successfully" });
      });
    });
  });
  app2.get("/api/logout", (req, res, next) => {
    req.logout((err) => {
      if (err) return next(err);
      req.session.destroy((destroyErr) => {
        if (destroyErr) return next(destroyErr);
        res.clearCookie("connect.sid");
        res.redirect("/auth");
      });
    });
  });
  app2.get("/api/auth/user", (req, res) => {
    if (!req.isAuthenticated()) {
      return res.status(401).json({ message: "Unauthorized" });
    }
    const user = req.user;
    res.json({
      id: user.id,
      email: user.email,
      firstName: user.firstName,
      lastName: user.lastName
    });
  });
  app2.get("/api/login", (req, res) => {
    res.status(405).json({ message: "Login via POST only" });
  });
}

// server/routes.ts
var isAuthenticated = (req, res, next) => {
  if (req.isAuthenticated && req.isAuthenticated()) {
    return next();
  }
  res.status(401).json({ message: "Unauthorized" });
};
async function registerRoutes(app2) {
  setupAuth(app2);
  app2.get("/api/brands", isAuthenticated, async (req, res) => {
    try {
      const brands = await storage.getBrands();
      res.json(brands);
    } catch (error) {
      console.error("Error fetching brands:", error);
      res.status(500).json({ message: "Failed to fetch brands" });
    }
  });
  app2.get("/api/brands/audience-overlap", isAuthenticated, async (req, res) => {
    try {
      const brand1Id = parseInt(req.query.brand1Id) || 1;
      const brand2Id = parseInt(req.query.brand2Id) || 2;
      const platform = req.query.platform;
      const dateRange = req.query.dateRange;
      console.log("API Call: /api/brands/audience-overlap", { brand1Id, brand2Id, platform, dateRange });
      const overlapData = await storage.getAudienceOverlap(brand1Id, brand2Id, platform, dateRange);
      res.json(overlapData);
    } catch (error) {
      console.error("Error getting audience overlap:", error);
      res.status(500).json({ success: false, message: "Failed to get audience overlap" });
    }
  });
  app2.get("/api/brands/:slug", isAuthenticated, async (req, res) => {
    try {
      const brand = await storage.getBrandBySlug(req.params.slug);
      if (!brand) {
        return res.status(404).json({ message: "Brand not found" });
      }
      res.json(brand);
    } catch (error) {
      console.error("Error fetching brand:", error);
      res.status(500).json({ message: "Failed to fetch brand" });
    }
  });
  app2.get("/api/brands/:brandId/metrics", isAuthenticated, async (req, res) => {
    try {
      const brandId = parseInt(req.params.brandId);
      const { startDate, endDate, platform, dateRange } = req.query;
      let calculatedStartDate = startDate;
      let calculatedEndDate = endDate;
      if (dateRange && !startDate && !endDate) {
        if (dateRange === "all") {
          calculatedStartDate = "";
          calculatedEndDate = "";
        } else {
          const now = /* @__PURE__ */ new Date();
          calculatedEndDate = now.toISOString().split("T")[0];
          switch (dateRange) {
            case "7days":
              calculatedStartDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1e3).toISOString().split("T")[0];
              break;
            case "30days":
              calculatedStartDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1e3).toISOString().split("T")[0];
              break;
            case "90days":
              calculatedStartDate = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1e3).toISOString().split("T")[0];
              break;
            default:
              calculatedStartDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1e3).toISOString().split("T")[0];
          }
        }
      }
      res.set({
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0"
      });
      const metrics = await storage.getSocialMetrics(
        brandId,
        calculatedStartDate,
        calculatedEndDate,
        platform
      );
      res.json(metrics);
    } catch (error) {
      console.error("Error fetching metrics:", error);
      res.status(500).json({ message: "Failed to fetch metrics" });
    }
  });
  app2.get("/api/brands/:brandId/content", isAuthenticated, async (req, res) => {
    try {
      const brandId = parseInt(req.params.brandId);
      const limit = parseInt(req.query.limit) || 10;
      const { platform } = req.query;
      res.set({
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0"
      });
      const content = await storage.getTopContent(brandId, limit, platform);
      res.json(content);
    } catch (error) {
      console.error("Error fetching content:", error);
      res.status(500).json({ message: "Failed to fetch content" });
    }
  });
  app2.get("/api/brands/:brandId/hashtags", isAuthenticated, async (req, res) => {
    try {
      const brandId = parseInt(req.params.brandId);
      const limit = parseInt(req.query.limit) || 10;
      const { platform, dateRange } = req.query;
      const hashtags = await storage.getBrandHashtags(brandId, limit, platform, dateRange);
      res.json(hashtags);
    } catch (error) {
      console.error("Error fetching brand hashtags:", error);
      res.status(500).json({ message: "Failed to fetch brand hashtags" });
    }
  });
  app2.get("/api/hashtags/industry", isAuthenticated, async (req, res) => {
    try {
      const hashtags = await storage.getIndustryHashtags();
      res.json(hashtags);
    } catch (error) {
      console.error("Error fetching industry hashtags:", error);
      res.status(500).json({ message: "Failed to fetch industry hashtags" });
    }
  });
  app2.get("/api/brands/:brandId/demographics", isAuthenticated, async (req, res) => {
    try {
      const brandId = parseInt(req.params.brandId);
      const { date, platform, dateRange } = req.query;
      console.log(`Demographics API called with platform: ${platform}, dateRange: ${dateRange}`);
      const demographics = await storage.getAudienceDemographics(brandId, date, platform);
      res.json(demographics);
    } catch (error) {
      console.error("Error fetching demographics:", error);
      res.status(500).json({ message: "Failed to fetch demographics" });
    }
  });
  app2.get("/api/brands/:brandId/sentiment", isAuthenticated, async (req, res) => {
    try {
      const brandId = parseInt(req.params.brandId);
      const { startDate, endDate, platform, dateRange } = req.query;
      console.log("Sentiment API called with platform:", platform, "dateRange:", dateRange);
      let calculatedStartDate = startDate;
      let calculatedEndDate = endDate;
      if (dateRange && !startDate && !endDate) {
        if (dateRange === "all") {
          calculatedStartDate = "";
          calculatedEndDate = "";
        } else {
          const now = /* @__PURE__ */ new Date();
          const endDateStr = now.toISOString().split("T")[0];
          let startDateStr;
          switch (dateRange) {
            case "7days":
            case "7d":
              startDateStr = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1e3).toISOString().split("T")[0];
              break;
            case "30days":
            case "30d":
              startDateStr = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1e3).toISOString().split("T")[0];
              break;
            case "90days":
            case "90d":
              startDateStr = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1e3).toISOString().split("T")[0];
              break;
            default:
              startDateStr = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1e3).toISOString().split("T")[0];
          }
          calculatedStartDate = startDateStr;
          calculatedEndDate = endDateStr;
        }
      }
      const sentiment = await storage.getSentimentData(
        brandId,
        calculatedStartDate,
        calculatedEndDate,
        platform
      );
      console.log("Retrieved", sentiment.length, "sentiment records for platform:", platform);
      res.json(sentiment);
    } catch (error) {
      console.error("Error fetching sentiment data:", error);
      res.status(500).json({ message: "Failed to fetch sentiment data" });
    }
  });
  app2.get("/api/brands/:brandId/content-strategy", isAuthenticated, async (req, res) => {
    try {
      const brandId = parseInt(req.params.brandId);
      const { platform, dateRange } = req.query;
      console.log(`Content strategy API called for brand ${brandId}, platform: ${platform}, dateRange: ${dateRange}`);
      const strategyData = await storage.getContentStrategy(brandId, platform, dateRange);
      res.json(strategyData);
    } catch (error) {
      console.error("Error fetching content strategy:", error);
      res.status(500).json({ message: "Failed to fetch content strategy" });
    }
  });
  app2.get("/api/brands/:brandId/engagement-over-time", isAuthenticated, async (req, res) => {
    try {
      const brandId = parseInt(req.params.brandId);
      const { platform, dateRange } = req.query;
      console.log(`Engagement over time API called for brand ${brandId}, platform: ${platform}, dateRange: ${dateRange}`);
      const engagementData = await storage.getEngagementOverTime(brandId, platform, dateRange);
      res.json(engagementData);
    } catch (error) {
      console.error("Error fetching engagement over time data:", error);
      res.status(500).json({ message: "Failed to fetch engagement over time data" });
    }
  });
  app2.post("/api/brands/:brandId/sentiment/extract", isAuthenticated, async (req, res) => {
    try {
      const { extractAndAnalyzeSentiments: extractAndAnalyzeSentiments2 } = await Promise.resolve().then(() => (init_sentiment_analyzer(), sentiment_analyzer_exports));
      const result = await extractAndAnalyzeSentiments2();
      res.json(result);
    } catch (error) {
      console.error("Error extracting sentiment data:", error);
      res.status(500).json({ message: "Failed to extract sentiment data", error: String(error) });
    }
  });
  app2.get("/api/hashtags/suggestions", isAuthenticated, async (req, res) => {
    try {
      const { brand } = req.query;
      if (!brand) {
        return res.json([]);
      }
      const brandName = brand.toLowerCase();
      const suggestions = [
        { tag: `#${brandName.replace(/\s+/g, "")}` },
        { tag: `#${brandName.replace(/\s+/g, "")}style` },
        { tag: `#${brandName.replace(/\s+/g, "")}fashion` },
        { tag: `#${brandName.replace(/\s+/g, "")}official` },
        { tag: `#${brandName.replace(/\s+/g, "")}brand` }
      ];
      const existingHashtags = await storage.getIndustryHashtags();
      const matchingHashtags = existingHashtags.filter(
        (h) => h.hashtag.toLowerCase().includes(brandName) || brandName.includes(h.hashtag.toLowerCase().replace("#", ""))
      ).slice(0, 5);
      res.json([...suggestions, ...matchingHashtags]);
    } catch (error) {
      console.error("Error generating hashtag suggestions:", error);
      res.status(500).json({ message: "Failed to generate suggestions" });
    }
  });
  app2.post("/api/user/onboarding", isAuthenticated, async (req, res) => {
    try {
      const userId = req.user.id;
      const { brandName, hashtags, competitors, platforms } = req.body;
      console.log(`User ${userId} completed onboarding:`, {
        brandName,
        hashtagCount: hashtags.length,
        competitorCount: competitors.length,
        platforms
      });
      res.json({
        success: true,
        message: "Onboarding completed successfully",
        data: { brandName, hashtags, competitors, platforms }
      });
    } catch (error) {
      console.error("Error saving onboarding data:", error);
      res.status(500).json({ message: "Failed to save onboarding data" });
    }
  });
  const httpServer = createServer(app2);
  return httpServer;
}

// server/vite.ts
import express from "express";
import fs4 from "fs";
import path5 from "path";
import { createServer as createViteServer, createLogger } from "vite";

// vite.config.ts
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path4 from "path";
import runtimeErrorOverlay from "@replit/vite-plugin-runtime-error-modal";
var vite_config_default = defineConfig({
  plugins: [
    react(),
    runtimeErrorOverlay(),
    ...process.env.NODE_ENV !== "production" && process.env.REPL_ID !== void 0 ? [
      await import("@replit/vite-plugin-cartographer").then(
        (m) => m.cartographer()
      )
    ] : []
  ],
  resolve: {
    alias: {
      "@": path4.resolve(import.meta.dirname, "client", "src"),
      "@shared": path4.resolve(import.meta.dirname, "shared"),
      "@assets": path4.resolve(import.meta.dirname, "attached_assets")
    }
  },
  root: path4.resolve(import.meta.dirname, "client"),
  build: {
    outDir: path4.resolve(import.meta.dirname, "dist/public"),
    emptyOutDir: true
  }
});

// server/vite.ts
import { nanoid } from "nanoid";
var viteLogger = createLogger();
function log(message, source = "express") {
  const formattedTime = (/* @__PURE__ */ new Date()).toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
    hour12: true
  });
  console.log(`${formattedTime} [${source}] ${message}`);
}
async function setupVite(app2, server) {
  const serverOptions = {
    middlewareMode: true,
    hmr: { server },
    allowedHosts: true
  };
  const vite = await createViteServer({
    ...vite_config_default,
    configFile: false,
    customLogger: {
      ...viteLogger,
      error: (msg, options) => {
        viteLogger.error(msg, options);
        process.exit(1);
      }
    },
    server: serverOptions,
    appType: "custom"
  });
  app2.use(vite.middlewares);
  app2.use("*", async (req, res, next) => {
    const url = req.originalUrl;
    try {
      const clientTemplate = path5.resolve(
        import.meta.dirname,
        "..",
        "client",
        "index.html"
      );
      let template = await fs4.promises.readFile(clientTemplate, "utf-8");
      template = template.replace(
        `src="/src/main.tsx"`,
        `src="/src/main.tsx?v=${nanoid()}"`
      );
      const page = await vite.transformIndexHtml(url, template);
      res.status(200).set({ "Content-Type": "text/html" }).end(page);
    } catch (e) {
      vite.ssrFixStacktrace(e);
      next(e);
    }
  });
}
function serveStatic(app2) {
  const distPath = path5.resolve(import.meta.dirname, "public");
  if (!fs4.existsSync(distPath)) {
    throw new Error(
      `Could not find the build directory: ${distPath}, make sure to build the client first`
    );
  }
  app2.use(express.static(distPath));
  app2.use("*", (_req, res) => {
    res.sendFile(path5.resolve(distPath, "index.html"));
  });
}

// server/index.ts
init_sentiment_analyzer();
var app = express2();
app.use(express2.json());
app.use(express2.urlencoded({ extended: false }));
app.use((req, res, next) => {
  const start = Date.now();
  const path6 = req.path;
  let capturedJsonResponse = void 0;
  const originalResJson = res.json;
  res.json = function(bodyJson, ...args) {
    capturedJsonResponse = bodyJson;
    return originalResJson.apply(res, [bodyJson, ...args]);
  };
  res.on("finish", () => {
    const duration = Date.now() - start;
    if (path6.startsWith("/api")) {
      let logLine = `${req.method} ${path6} ${res.statusCode} in ${duration}ms`;
      if (capturedJsonResponse) {
        logLine += ` :: ${JSON.stringify(capturedJsonResponse)}`;
      }
      if (logLine.length > 80) {
        logLine = logLine.slice(0, 79) + "\u2026";
      }
      log(logLine);
    }
  });
  next();
});
app.get("/api/brands/:brandId/topics", async (req, res) => {
  const brandId = parseInt(req.params.brandId);
  const platform = req.query.platform;
  const topics = await getKeyTopicsBySentiment(brandId, platform);
  res.json(topics);
});
app.get("/api/brands/:brandId/post-frequency", async (req, res) => {
  const brandId = parseInt(req.params.brandId);
  const platform = req.query.platform;
  const startDate = req.query.startDate;
  const endDate = req.query.endDate;
  const frequency = await getPostFrequency(brandId, platform, startDate, endDate);
  res.json(frequency);
});
(async () => {
  const server = await registerRoutes(app);
  try {
    log("Starting sentiment analysis extraction on server startup...");
    const result = await extractAndAnalyzeSentiments();
    log(`Sentiment analysis extraction completed: ${result.data.length} records processed`);
  } catch (error) {
    log(`Error running sentiment analysis extraction: ${error}`);
  }
  app.use((err, _req, res, _next) => {
    const status = err.status || err.statusCode || 500;
    const message = err.message || "Internal Server Error";
    res.status(status).json({ message });
    throw err;
  });
  if (app.get("env") === "development") {
    await setupVite(app, server);
  } else {
    serveStatic(app);
  }
  const port = 5e3;
  server.listen({
    port,
    host: "0.0.0.0",
    reusePort: true
  }, () => {
    log(`serving on port ${port}`);
  });
})();
