// server/sentiment-analyzer.ts
import fs2 from "fs";
import path2 from "path";
import { parse } from "csv-parse/sync";
import fetch from "node-fetch";

// server/sentiment-cache.ts
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
var __filename = fileURLToPath(import.meta.url);
var __dirname = path.dirname(__filename);
var SENTIMENT_CACHE_FILE = path.join(__dirname, "..", "data", "sentiment-cache.json");
function saveSentimentCache(sentimentData) {
  try {
    const dataDir = path.join(__dirname, "..", "data");
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true });
    }
    const serializedData = sentimentData.map((item) => ({
      ...item,
      date: item.date.toISOString(),
      createdAt: item.createdAt.toISOString()
    }));
    fs.writeFileSync(
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
    if (!fs.existsSync(SENTIMENT_CACHE_FILE)) {
      console.log("Sentiment cache file not found");
      return null;
    }
    const fileContent = fs.readFileSync(SENTIMENT_CACHE_FILE, "utf-8");
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
    if (!fs.existsSync(SENTIMENT_CACHE_FILE)) {
      return true;
    }
    const cacheStats = fs.statSync(SENTIMENT_CACHE_FILE);
    const cacheModified = cacheStats.mtime;
    const publicDir = path.join(__dirname, "..", "public");
    const csvFiles = fs.readdirSync(publicDir).filter((file) => file.endsWith(".csv"));
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
    console.error("Error checking if sentiment cache is outdated:", error);
    return true;
  }
}

// server/sentiment-analyzer.ts
function readCSV(filename) {
  const filePath = path2.join(process.cwd(), "public", filename);
  if (!fs2.existsSync(filePath)) {
    console.error(`CSV file not found: ${filename}`);
    return [];
  }
  const content = fs2.readFileSync(filePath, "utf-8");
  return parse(content, { columns: true, skip_empty_lines: true });
}
async function analyzeSentimentWithTextBlob(text) {
  if (!text || typeof text !== "string") return { sentiment: "neutral", score: 0.5, subjectivity: 0.5 };
  try {
    const response = await fetch("http://localhost:5001/analyze", {
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
  const data = readCSV(filename);
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
    const analysis = await analyzeSentimentWithTextBlob(textContent);
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
export {
  extractAndAnalyzeSentiments
};
