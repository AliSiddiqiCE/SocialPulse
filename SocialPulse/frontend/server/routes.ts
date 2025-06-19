import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import { setupAuth } from "./auth";

// Simple authentication middleware
const isAuthenticated = (req: any, res: any, next: any) => {
  if (req.isAuthenticated && req.isAuthenticated()) {
    return next();
  }
  res.status(401).json({ message: "Unauthorized" });
};

export async function registerRoutes(app: Express): Promise<Server> {
  // Auth middleware
  setupAuth(app);

  // Topics routes - authentic data from your datasets (placed first to avoid conflicts)
  // app.get("/api/brands/:brandId/topics", async (req, res) => {
  //   try {
  //     res.setHeader('Content-Type', 'application/json');
  //     
  //     const brandId = parseInt(req.params.brandId);
  //     const { platform, dateRange } = req.query;
  //     
  //     let query = `
  //       SELECT topic, platform, mention_count, sentiment_score
  //       FROM topics_analysis 
  //       WHERE brand_id = $1
  //     `;
  //     
  //     const params: (number | string)[] = [brandId];
  //     
  //     if (platform && platform !== 'all' && typeof platform === 'string') {
  //       query += ` AND platform = $${params.length + 1}`;
  //       params.push(platform);
  //     }
  //     
  //     // Add date range filtering based on created_at
  //     if (dateRange && dateRange !== 'all') {
  //       let daysBack = 30; // default
  //       if (dateRange === '7days') daysBack = 7;
  //       if (dateRange === '30days') daysBack = 30;
  //       if (dateRange === '90days') daysBack = 90;
  //       
  //       query += ` AND created_at >= NOW() - INTERVAL '${daysBack} days'`;
  //     }
  //     
  //     query += ` ORDER BY mention_count DESC LIMIT 10`;
  //     
  //     const { pool } = await import("./db");
  //     console.log('Topics query executing:', query);
  //     console.log('Query params:', params);
  //     
  //     const result = await pool.query(query, params);
  //     console.log('Topics found:', result.rows?.length || 0);
  //     
  //     if (result.rows && result.rows.length > 0) {
  //       console.log('Returning topics:', result.rows.map((r: any) => r.topic));
  //       return res.json(result.rows);
  //     } else {
  //       console.log('No topics found for platform:', platform);
  //       return res.json([]);
  //     }
  //   } catch (error: any) {
  //     console.error("Topics API error:", error);
  //     return res.status(500).json({ message: "Failed to fetch topics data" });
  //   }
  // });

  // Brand routes
  app.get("/api/brands", isAuthenticated, async (req, res) => {
    try {
      const brands = await storage.getBrands();
      res.json(brands);
    } catch (error) {
      console.error("Error fetching brands:", error);
      res.status(500).json({ message: "Failed to fetch brands" });
    }
  });

  // Get audience overlap between two brands
  app.get('/api/brands/audience-overlap', isAuthenticated, async (req, res) => {
    try {
      const brand1Id = parseInt(req.query.brand1Id as string) || 1;
      const brand2Id = parseInt(req.query.brand2Id as string) || 2;
      const platform = req.query.platform as string;
      const dateRange = req.query.dateRange as string;

      console.log('API Call: /api/brands/audience-overlap', { brand1Id, brand2Id, platform, dateRange });
      
      const overlapData = await storage.getAudienceOverlap(brand1Id, brand2Id, platform, dateRange);
      res.json(overlapData);
    } catch (error) {
      console.error('Error getting audience overlap:', error);
      res.status(500).json({ success: false, message: 'Failed to get audience overlap' });
    }
  });

  app.get("/api/brands/:slug", isAuthenticated, async (req, res) => {
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

  // Social metrics routes
  app.get("/api/brands/:brandId/metrics", isAuthenticated, async (req, res) => {
    try {
      const brandId = parseInt(req.params.brandId);
      const { platform, dateRange } = req.query;
      
      // Set timezone to UTC to ensure consistent date handling
      const now = new Date('2025-05-29T23:59:59.999Z');
      now.setUTCHours(23, 59, 59, 999); // End of day in UTC
      
      let startDate: string | undefined;
      let endDate: string | undefined;

      if (dateRange) {
        endDate = now.toISOString().split('T')[0];
        const startDateObj = new Date(now);
        
        switch (dateRange) {
          case '7days':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 6);
            break;
          case '30days':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 29);
            break;
          case '90days':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 89);
            break;
          case 'all':
            startDate = undefined;
            endDate = undefined;
            break;
          default:
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 29);
        }
        
        if (dateRange !== 'all') {
          startDateObj.setUTCHours(0, 0, 0, 0); // Start of day in UTC
          startDate = startDateObj.toISOString().split('T')[0];
        }
      }

      res.set({
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0'
      });
      
      console.log('Fetching metrics with:', { brandId, startDate, endDate, platform });
      const metrics = await storage.getSocialMetrics(
        brandId,
        startDate,
        endDate,
        platform as string
      );
      res.json(metrics);
    } catch (error) {
      console.error("Error fetching metrics:", error);
      res.status(500).json({ message: "Failed to fetch metrics" });
    }
  });

  // Content routes
  app.get("/api/brands/:brandId/content", isAuthenticated, async (req, res) => {
    try {
      const brandId = parseInt(req.params.brandId);
      const limit = parseInt(req.query.limit as string) || 10;
      const { platform, dateRange } = req.query;
      let startDate: string | undefined;
      let endDate: string | undefined;
      if (dateRange) {
        const now = new Date('2025-05-29T23:59:59.999Z');
        endDate = now.toISOString().split('T')[0];
        const startDateObj = new Date(now);
        switch (dateRange) {
          case '7days':
          case '7d':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 6);
            break;
          case '30days':
          case '30d':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 29);
            break;
          case '90days':
          case '90d':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 89);
            break;
          case 'all':
            startDate = undefined;
            endDate = undefined;
            break;
          default:
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 29);
        }
        if (dateRange !== 'all') {
          startDateObj.setUTCHours(0, 0, 0, 0);
          startDate = startDateObj.toISOString().split('T')[0];
        }
      }
      res.set({
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0'
      });
      const content = await storage.getTopContent(brandId, limit, platform as string, startDate, endDate);
      res.json(content);
    } catch (error) {
      console.error("Error fetching content:", error);
      res.status(500).json({ message: "Failed to fetch content" });
    }
  });

  // Hashtag routes
  app.get("/api/brands/:brandId/hashtags", isAuthenticated, async (req, res) => {
    try {
      const brandId = parseInt(req.params.brandId);
      const limit = parseInt(req.query.limit as string) || 10;
      const { platform, dateRange } = req.query;
      let startDate: string | undefined;
      let endDate: string | undefined;
      if (dateRange) {
        const now = new Date('2025-05-29T23:59:59.999Z');
        endDate = now.toISOString().split('T')[0];
        const startDateObj = new Date(now);
        switch (dateRange) {
          case '7days':
          case '7d':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 6);
            break;
          case '30days':
          case '30d':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 29);
            break;
          case '90days':
          case '90d':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 89);
            break;
          case 'all':
            startDate = undefined;
            endDate = undefined;
            break;
          default:
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 29);
        }
        if (dateRange !== 'all') {
          startDateObj.setUTCHours(0, 0, 0, 0);
          startDate = startDateObj.toISOString().split('T')[0];
        }
      }
      const hashtags = await storage.getBrandHashtags(brandId, limit, platform as string, startDate, endDate);
      res.json(hashtags);
    } catch (error) {
      console.error("Error fetching brand hashtags:", error);
      res.status(500).json({ message: "Failed to fetch brand hashtags" });
    }
  });

  app.get("/api/hashtags/industry", isAuthenticated, async (req, res) => {
    try {
      const hashtags = await storage.getIndustryHashtags();
      res.json(hashtags);
    } catch (error) {
      console.error("Error fetching industry hashtags:", error);
      res.status(500).json({ message: "Failed to fetch industry hashtags" });
    }
  });

  // Demographics routes
  app.get("/api/brands/:brandId/demographics", isAuthenticated, async (req, res) => {
    try {
      const brandId = parseInt(req.params.brandId);
      const { platform, dateRange } = req.query;
      let startDate: string | undefined;
      let endDate: string | undefined;
      if (dateRange) {
        const now = new Date('2025-05-29T23:59:59.999Z');
        endDate = now.toISOString().split('T')[0];
        const startDateObj = new Date(now);
        switch (dateRange) {
          case '7days':
          case '7d':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 6);
            break;
          case '30days':
          case '30d':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 29);
            break;
          case '90days':
          case '90d':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 89);
            break;
          case 'all':
            startDate = undefined;
            endDate = undefined;
            break;
          default:
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 29);
        }
        if (dateRange !== 'all') {
          startDateObj.setUTCHours(0, 0, 0, 0);
          startDate = startDateObj.toISOString().split('T')[0];
        }
      }
      const demographics = await storage.getAudienceDemographics(brandId, startDate, endDate, platform as string);
      res.json(demographics);
    } catch (error) {
      console.error("Error fetching demographics:", error);
      res.status(500).json({ message: "Failed to fetch demographics" });
    }
  });

  // Sentiment routes
  app.get("/api/brands/:brandId/sentiment", isAuthenticated, async (req, res) => {
    try {
      const brandId = parseInt(req.params.brandId);
      const { startDate, endDate, platform, dateRange } = req.query;
      
      console.log('Sentiment API called with platform:', platform, 'dateRange:', dateRange);
      
      // Convert date range to actual dates for filtering
      let calculatedStartDate = startDate as string;
      let calculatedEndDate = endDate as string;
      
      if (dateRange && !startDate && !endDate) {
        if (dateRange === 'all') {
          // For "All dates", don't set date filters
          calculatedStartDate = '';
          calculatedEndDate = '';
        } else {
          const now = new Date('2025-05-29T23:59:59.999Z');
          const endDateStr = now.toISOString().split('T')[0];
          let startDateStr: string;
          
          switch (dateRange) {
            case '7days':
            case '7d':
              startDateStr = new Date(now.getTime() - 6 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
              break;
            case '30days':
            case '30d':
              startDateStr = new Date(now.getTime() - 29 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
              break;
            case '90days':
            case '90d':
              startDateStr = new Date(now.getTime() - 89 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
              break;
            default:
              startDateStr = new Date(now.getTime() - 29 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
          }
          
          calculatedStartDate = startDateStr;
          calculatedEndDate = endDateStr;
        }
      }
      
      const sentiment = await storage.getSentimentData(
        brandId,
        calculatedStartDate,
        calculatedEndDate,
        platform as string
      );
      
      console.log('Retrieved', sentiment.length, 'sentiment records for platform:', platform);
      res.json(sentiment);
    } catch (error) {
      console.error("Error fetching sentiment data:", error);
      res.status(500).json({ message: "Failed to fetch sentiment data" });
    }
  });

  // Content strategy route with filters
  app.get("/api/brands/:brandId/content-strategy", isAuthenticated, async (req, res) => {
    try {
      const brandId = parseInt(req.params.brandId);
      const { platform, dateRange } = req.query;
      let startDate: string | undefined;
      let endDate: string | undefined;
      if (dateRange) {
        const now = new Date('2025-05-29T23:59:59.999Z');
        endDate = now.toISOString().split('T')[0];
        const startDateObj = new Date(now);
        switch (dateRange) {
          case '7days':
          case '7d':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 6);
            break;
          case '30days':
          case '30d':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 29);
            break;
          case '90days':
          case '90d':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 89);
            break;
          case 'all':
            startDate = undefined;
            endDate = undefined;
            break;
          default:
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 29);
        }
        if (dateRange !== 'all') {
          startDateObj.setUTCHours(0, 0, 0, 0);
          startDate = startDateObj.toISOString().split('T')[0];
        }
      }
      const strategyData = await storage.getContentStrategy(brandId, platform as string, startDate, endDate);
      res.json(strategyData);
    } catch (error) {
      console.error("Error fetching content strategy:", error);
      res.status(500).json({ message: "Failed to fetch content strategy" });
    }
  });
  
  // Engagement over time route with filters
  app.get("/api/brands/:brandId/engagement-over-time", isAuthenticated, async (req, res) => {
    try {
      const brandId = parseInt(req.params.brandId);
      const { platform, dateRange } = req.query;
      let startDate: string | undefined;
      let endDate: string | undefined;
      if (dateRange) {
        const now = new Date('2025-05-29T23:59:59.999Z');
        endDate = now.toISOString().split('T')[0];
        const startDateObj = new Date(now);
        switch (dateRange) {
          case '7days':
          case '7d':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 6);
            break;
          case '30days':
          case '30d':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 29);
            break;
          case '90days':
          case '90d':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 89);
            break;
          case 'all':
            startDate = undefined;
            endDate = undefined;
            break;
          default:
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 29);
        }
        if (dateRange !== 'all') {
          startDateObj.setUTCHours(0, 0, 0, 0);
          startDate = startDateObj.toISOString().split('T')[0];
        }
      }
      const engagementData = await storage.getEngagementOverTime(brandId, platform as string, startDate, endDate);
      res.json(engagementData);
    } catch (error) {
      console.error("Error fetching engagement over time data:", error);
      res.status(500).json({ message: "Failed to fetch engagement over time data" });
    }
  });

  // Trigger sentiment analysis extraction
  app.post("/api/brands/:brandId/sentiment/extract", isAuthenticated, async (req, res) => {
    try {
      const { extractAndAnalyzeSentiments } = await import("./sentiment-analyzer");
      const result = await extractAndAnalyzeSentiments();
      res.json(result);
    } catch (error) {
      console.error("Error extracting sentiment data:", error);
      res.status(500).json({ message: "Failed to extract sentiment data", error: String(error) });
    }
  });

  // Onboarding pipeline routes
  app.get("/api/hashtags/suggestions", isAuthenticated, async (req, res) => {
    try {
      const { brand } = req.query;
      if (!brand) {
        return res.json([]);
      }

      // Generate hashtags based on brand name
      const brandName = (brand as string).toLowerCase();
      const suggestions = [
        { tag: `#${brandName.replace(/\s+/g, '')}` },
        { tag: `#${brandName.replace(/\s+/g, '')}style` },
        { tag: `#${brandName.replace(/\s+/g, '')}fashion` },
        { tag: `#${brandName.replace(/\s+/g, '')}official` },
        { tag: `#${brandName.replace(/\s+/g, '')}brand` },
      ];

      // Get existing hashtags from database that might match
      const existingHashtags = await storage.getIndustryHashtags();
      const matchingHashtags = existingHashtags.filter(h => 
        h.hashtag.toLowerCase().includes(brandName) || 
        brandName.includes(h.hashtag.toLowerCase().replace('#', ''))
      ).slice(0, 5);

      res.json([...suggestions, ...matchingHashtags]);
    } catch (error) {
      console.error("Error generating hashtag suggestions:", error);
      res.status(500).json({ message: "Failed to generate suggestions" });
    }
  });

  app.post("/api/user/onboarding", isAuthenticated, async (req, res) => {
    try {
      const userId = (req.user as any).id;
      const { brandName, hashtags, competitors, platforms } = req.body;

      // Store user onboarding preferences (you can extend the user table or create a preferences table)
      // For now, we'll just return success as the data can be used for future personalization
      
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

  // Topics routes
  app.get("/api/brands/:brandId/topics", isAuthenticated, async (req, res) => {
    try {
      const brandId = parseInt(req.params.brandId);
      const { platform, dateRange } = req.query;
      let startDate: string | undefined;
      let endDate: string | undefined;
      if (dateRange) {
        const now = new Date('2025-05-29T23:59:59.999Z');
        endDate = now.toISOString().split('T')[0];
        const startDateObj = new Date(now);
        switch (dateRange) {
          case '7days':
          case '7d':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 6);
            break;
          case '30days':
          case '30d':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 29);
            break;
          case '90days':
          case '90d':
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 89);
            break;
          case 'all':
            startDate = undefined;
            endDate = undefined;
            break;
          default:
            startDateObj.setUTCDate(startDateObj.getUTCDate() - 29);
        }
        if (dateRange !== 'all') {
          startDateObj.setUTCHours(0, 0, 0, 0);
          startDate = startDateObj.toISOString().split('T')[0];
        }
      }
      const topics = await storage.getKeyTopicsBySentiment(brandId, platform as string, startDate, endDate);
      res.json(topics);
    } catch (error) {
      console.error("Error fetching topics:", error);
      res.status(500).json({ message: "Failed to fetch topics" });
    }
  });

  const httpServer = createServer(app);
  return httpServer;
}
