import {
  pgTable,
  text,
  varchar,
  timestamp,
  jsonb,
  index,
  serial,
  integer,
  decimal,
  date,
  boolean,
} from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

// Session storage table.
// (IMPORTANT) This table is mandatory for Replit Auth, don't drop it.
export const sessions = pgTable(
  "sessions",
  {
    sid: varchar("sid").primaryKey(),
    sess: jsonb("sess").notNull(),
    expire: timestamp("expire").notNull(),
  },
  (table) => [index("IDX_session_expire").on(table.expire)],
);

// User storage table.
export const users = pgTable("users", {
  id: serial("id").primaryKey(),
  email: varchar("email").unique().notNull(),
  password: varchar("password").notNull(),
  firstName: varchar("first_name").notNull(),
  lastName: varchar("last_name").notNull(),
  phone: varchar("phone"),
  company: varchar("company"),
  department: varchar("department"),
  role: varchar("role"),
  location: varchar("location"),
  website: varchar("website"),
  bio: text("bio"),
  profileImageUrl: varchar("profile_image_url"),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

// Brand profiles for social media analytics
export const brands = pgTable("brands", {
  id: serial("id").primaryKey(),
  name: varchar("name", { length: 255 }).notNull(),
  slug: varchar("slug", { length: 100 }).notNull().unique(),
  industry: varchar("industry", { length: 100 }),
  description: text("description"),
  createdAt: timestamp("created_at").defaultNow(),
});

// Social media metrics for each brand
export const socialMetrics = pgTable("social_metrics", {
  id: serial("id").primaryKey(),
  brandId: integer("brand_id").notNull().references(() => brands.id),
  platform: varchar("platform", { length: 50 }).notNull(), // twitter, facebook, instagram
  engagementScore: decimal("engagement_score", { precision: 5, scale: 2 }),
  totalPosts: integer("total_posts"),
  followers: integer("followers"),
  avgResponseTimeHours: decimal("avg_response_time_hours", { precision: 5, scale: 2 }),
  mentions: integer("mentions"),
  reach: integer("reach"),
  likes: integer("likes"),
  comments: integer("comments"),
  shares: integer("shares"),
  date: date("date").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
});

// Content posts performance
export const contentPosts = pgTable("content_posts", {
  id: serial("id").primaryKey(),
  brandId: integer("brand_id").notNull().references(() => brands.id),
  platform: varchar("platform", { length: 50 }).notNull(),
  content: text("content").notNull(),
  postType: varchar("post_type", { length: 50 }), // text, image, video
  publishedAt: timestamp("published_at").notNull(),
  views: integer("views"),
  likes: integer("likes"),
  comments: integer("comments"),
  shares: integer("shares"),
  engagementRate: decimal("engagement_rate", { precision: 5, scale: 2 }),
  createdAt: timestamp("created_at").defaultNow(),
});

// Hashtags and trending topics
export const hashtags = pgTable("hashtags", {
  id: serial("id").primaryKey(),
  tag: varchar("tag", { length: 255 }).notNull(),
  brandId: integer("brand_id").references(() => brands.id), // null for industry trends
  category: varchar("category", { length: 50 }), // brand, competitor, industry
  mentionCount: integer("mention_count").default(0),
  trendingScore: decimal("trending_score", { precision: 5, scale: 2 }),
  date: date("date").notNull(),
  platform: varchar("platform", { length: 50 }),
  createdAt: timestamp("created_at").defaultNow(),
});

// Audience demographics
export const audienceDemographics = pgTable("audience_demographics", {
  id: serial("id").primaryKey(),
  brandId: integer("brand_id").notNull().references(() => brands.id),
  platform: varchar("platform", { length: 50 }).notNull(),
  ageGroup: varchar("age_group", { length: 20 }), // 18-24, 25-34, etc.
  gender: varchar("gender", { length: 20 }), // male, female, other
  percentage: decimal("percentage", { precision: 5, scale: 2 }),
  date: date("date").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
});

// Sentiment analysis data
export const sentimentData = pgTable("sentiment_data", {
  id: serial("id").primaryKey(),
  brandId: integer("brand_id").notNull().references(() => brands.id),
  platform: varchar("platform", { length: 50 }).notNull(),
  sentiment: varchar("sentiment", { length: 20 }), // positive, negative, neutral
  mentionCount: integer("mention_count"),
  score: decimal("score", { precision: 5, scale: 2 }),
  date: date("date").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
});

export type InsertUser = typeof users.$inferInsert;
export type User = typeof users.$inferSelect;

export type InsertBrand = typeof brands.$inferInsert;
export type Brand = typeof brands.$inferSelect;

export type InsertSocialMetrics = typeof socialMetrics.$inferInsert;
export type SocialMetrics = typeof socialMetrics.$inferSelect;

export type InsertContentPost = typeof contentPosts.$inferInsert;
export type ContentPost = typeof contentPosts.$inferSelect;

export type InsertHashtag = typeof hashtags.$inferInsert;
export type Hashtag = typeof hashtags.$inferSelect;

export type InsertAudienceDemographics = typeof audienceDemographics.$inferInsert;
export type AudienceDemographics = typeof audienceDemographics.$inferSelect;

export type InsertSentimentData = typeof sentimentData.$inferInsert;
export type SentimentData = typeof sentimentData.$inferSelect;

export const insertUserSchema = createInsertSchema(users).omit({
  id: true,
  createdAt: true,
  updatedAt: true,
});

export const insertBrandSchema = createInsertSchema(brands);
export const insertSocialMetricsSchema = createInsertSchema(socialMetrics);
export const insertContentPostSchema = createInsertSchema(contentPosts);
export const insertHashtagSchema = createInsertSchema(hashtags);
export const insertAudienceDemographicsSchema = createInsertSchema(audienceDemographics);
export const insertSentimentDataSchema = createInsertSchema(sentimentData);

// M&S Authentic Dataset Tables - TikTok Official
export const dataset_tiktok_MS_official_cleaned = pgTable("dataset_tiktok_M&S_official_cleaned.xlsx_csv", {
  id: serial("id").primaryKey(),
  text: text("text"),
  created_time: varchar("created_time"),
  hashtags: text("hashtags"),
  shareCount: integer("shareCount"),
  mentions: text("mentions"),
  commentCount: integer("commentCount"),
  playCount: integer("playCount"),
  diggCount: integer("diggCount"),
  collectCount: integer("collectCount"),
});

// M&S Authentic Dataset Tables - TikTok Hashtag
export const dataset_tiktok_hashtag_MS_cleaned = pgTable("dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv", {
  id: serial("id").primaryKey(),
  text: text("text"),
  created_time: varchar("created_time"),
  hashtags: text("hashtags"),
  shareCount: integer("shareCount"),
  mentions: text("mentions"),
  commentCount: integer("commentCount"),
  playCount: integer("playCount"),
  collectCount: integer("collectCount"),
  diggCount: integer("diggCount"),
});

// M&S Authentic Dataset Tables - YouTube Official
export const dataset_youtube_channel_scraper_MS_official_cleaned = pgTable("dataset_youtube-channel-scraper_M&S-official 1_cleaned.xlsx_csv", {
  id: serial("id").primaryKey(),
  channelTotalViews: integer("channelTotalViews"),
  url: text("url"),
  duration: varchar("duration"),
  date: varchar("date"),
  title: text("title"),
  numberOfSubscribers: integer("numberOfSubscribers"),
  channelDescription: text("channelDescription"),
  channelTotalVideos: integer("channelTotalVideos"),
  channelJoinedDate: varchar("channelJoinedDate"),
});

// M&S Authentic Dataset Tables - YouTube Hashtag
export const dataset_youtube_hashtag_MS_cleaned = pgTable("dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv", {
  id: serial("id").primaryKey(),
  text: text("text"),
  hashtags: text("hashtags"),
  duration: varchar("duration"),
  date: varchar("date"),
  url: text("url"),
  commentsCount: integer("commentsCount"),
  title: text("title"),
  numberOfSubscribers: integer("numberOfSubscribers"),
  viewCount: integer("viewCount"),
  channelName: varchar("channelName"),
  likes: integer("likes"),
});

// M&S Authentic Dataset Tables - Instagram Official
export const insta_new_marksandspencer_cleaned = pgTable("Insta_new_marksandspencer_cleaned.xlsx_csv", {
  id: serial("id").primaryKey(),
  videoPlayCount: decimal("videoPlayCount", { precision: 10, scale: 1 }),
  hashtags: text("hashtags"),
  url: text("url"),
  locationName: varchar("locationName"),
  videoViewCount: decimal("videoViewCount", { precision: 10, scale: 1 }),
  videoDuration: decimal("videoDuration", { precision: 10, scale: 3 }),
  commentsCount: integer("commentsCount"),
  mentions: text("mentions"),
  caption: text("caption"),
  timestamp: varchar("timestamp"),
  likesCount: integer("likesCount"),
});

// M&S Authentic Dataset Tables - Instagram Hashtag
export const insta_new_mandshashtags_cleaned = pgTable("Insta_new_mandshashtags_cleaned.xlsx_csv", {
  id: serial("id").primaryKey(),
  hashtags: text("hashtags"),
  url: text("url"),
  locationName: varchar("locationName"),
  paidPartnership: varchar("paidPartnership"),
  caption: text("caption"),
  videoDuration: decimal("videoDuration", { precision: 10, scale: 3 }),
  commentsCount: decimal("commentsCount", { precision: 10, scale: 1 }),
  mentions: text("mentions"),
  isSponsored: decimal("isSponsored", { precision: 2, scale: 1 }),
  timestamp: varchar("timestamp"),
  likesCount: decimal("likesCount", { precision: 10, scale: 1 }),
});

// Type exports for the new M&S datasets
export type InsertTikTokMSOfficialData = typeof dataset_tiktok_MS_official_cleaned.$inferInsert;
export type TikTokMSOfficialData = typeof dataset_tiktok_MS_official_cleaned.$inferSelect;

export type InsertTikTokMSHashtagData = typeof dataset_tiktok_hashtag_MS_cleaned.$inferInsert;
export type TikTokMSHashtagData = typeof dataset_tiktok_hashtag_MS_cleaned.$inferSelect;

export type InsertYouTubeMSOfficialData = typeof dataset_youtube_channel_scraper_MS_official_cleaned.$inferInsert;
export type YouTubeMSOfficialData = typeof dataset_youtube_channel_scraper_MS_official_cleaned.$inferSelect;

export type InsertYouTubeMSHashtagData = typeof dataset_youtube_hashtag_MS_cleaned.$inferInsert;
export type YouTubeMSHashtagData = typeof dataset_youtube_hashtag_MS_cleaned.$inferSelect;

export type InsertInstagramMSOfficialData = typeof insta_new_marksandspencer_cleaned.$inferInsert;
export type InstagramMSOfficialData = typeof insta_new_marksandspencer_cleaned.$inferSelect;

export type InsertInstagramMSHashtagData = typeof insta_new_mandshashtags_cleaned.$inferInsert;
export type InstagramMSHashtagData = typeof insta_new_mandshashtags_cleaned.$inferSelect;
