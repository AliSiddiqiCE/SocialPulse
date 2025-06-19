import { Pool } from '@neondatabase/serverless';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

async function populateNextRetailContent() {
  console.log('Starting Next Retail content population...');
  
  try {
    // Get Instagram official data
    const instagramOfficial = await pool.query(`
      SELECT url, caption, timestamp, "videoViewCount", "commentsCount"
      FROM "nextretail_instagram_official" 
      WHERE caption IS NOT NULL AND caption != ''
      ORDER BY COALESCE("videoViewCount"::numeric, 0) DESC
      LIMIT 50
    `);
    
    console.log(`Found ${instagramOfficial.rows.length} Instagram official posts`);
    
    // Get Instagram hashtag data
    const instagramHashtag = await pool.query(`
      SELECT url, caption, timestamp, "likesCount", "commentsCount"
      FROM "nextretail_instagram_hashtag" 
      WHERE caption IS NOT NULL AND caption != ''
      ORDER BY COALESCE("likesCount"::numeric, 0) DESC
      LIMIT 50
    `);
    
    console.log(`Found ${instagramHashtag.rows.length} Instagram hashtag posts`);
    
    // Get TikTok official data
    const tiktokOfficial = await pool.query(`
      SELECT url, description as caption, created_time, "likesCount", "commentsCount", "shareCount"
      FROM "nextretail_tiktok_official" 
      WHERE description IS NOT NULL AND description != ''
      ORDER BY COALESCE("likesCount"::numeric, 0) DESC
      LIMIT 50
    `);
    
    console.log(`Found ${tiktokOfficial.rows.length} TikTok official posts`);
    
    // Get YouTube official data
    const youtubeOfficial = await pool.query(`
      SELECT url, title as caption, "publishedAt", "videoViewCount", "commentsCount", "likeCount"
      FROM "nextretail_youtube_official" 
      WHERE title IS NOT NULL AND title != ''
      ORDER BY COALESCE("videoViewCount"::numeric, 0) DESC
      LIMIT 50
    `);
    
    console.log(`Found ${youtubeOfficial.rows.length} YouTube official posts`);
    
    // Insert content records
    let insertedCount = 0;
    
    // Process Instagram official
    for (const post of instagramOfficial.rows) {
      try {
        const publishedAt = post.timestamp ? new Date(post.timestamp) : new Date();
        const likes = parseInt(post.videoViewCount) || 0;
        const comments = parseInt(post.commentsCount) || 0;
        const engagementRate = likes > 0 ? ((comments / likes) * 100).toFixed(2) : "0.00";
        
        await pool.query(`
          INSERT INTO content_posts (brand_id, platform, content, url, published_at, likes, comments, engagement_rate, created_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, [2, 'instagram', post.caption, post.url, publishedAt, likes, comments, engagementRate, new Date()]);
        
        insertedCount++;
      } catch (error) {
        console.log('Error inserting Instagram official post:', error.message);
      }
    }
    
    // Process Instagram hashtag
    for (const post of instagramHashtag.rows) {
      try {
        const publishedAt = post.timestamp ? new Date(post.timestamp) : new Date();
        const likes = parseInt(post.likesCount) || 0;
        const comments = parseInt(post.commentsCount) || 0;
        const engagementRate = likes > 0 ? ((comments / likes) * 100).toFixed(2) : "0.00";
        
        await pool.query(`
          INSERT INTO content_posts (brand_id, platform, content, url, published_at, likes, comments, engagement_rate, created_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, [2, 'instagram', post.caption, post.url, publishedAt, likes, comments, engagementRate, new Date()]);
        
        insertedCount++;
      } catch (error) {
        console.log('Error inserting Instagram hashtag post:', error.message);
      }
    }
    
    // Process TikTok official
    for (const post of tiktokOfficial.rows) {
      try {
        // Parse DD/MM/YYYY format from TikTok
        let publishedAt = new Date();
        if (post.created_time) {
          const dateParts = post.created_time.split('/');
          if (dateParts.length === 3) {
            publishedAt = new Date(`${dateParts[2]}-${dateParts[1].padStart(2, '0')}-${dateParts[0].padStart(2, '0')}`);
          }
        }
        
        const likes = parseInt(post.likesCount) || 0;
        const comments = parseInt(post.commentsCount) || 0;
        const shares = parseInt(post.shareCount) || 0;
        const engagementRate = likes > 0 ? (((comments + shares) / likes) * 100).toFixed(2) : "0.00";
        
        await pool.query(`
          INSERT INTO content_posts (brand_id, platform, content, url, published_at, likes, comments, shares, engagement_rate, created_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        `, [2, 'tiktok', post.caption, post.url, publishedAt, likes, comments, shares, engagementRate, new Date()]);
        
        insertedCount++;
      } catch (error) {
        console.log('Error inserting TikTok official post:', error.message);
      }
    }
    
    // Process YouTube official
    for (const post of youtubeOfficial.rows) {
      try {
        const publishedAt = post.publishedAt ? new Date(post.publishedAt) : new Date();
        const views = parseInt(post.videoViewCount) || 0;
        const likes = parseInt(post.likeCount) || 0;
        const comments = parseInt(post.commentsCount) || 0;
        const engagementRate = views > 0 ? (((likes + comments) / views) * 100).toFixed(2) : "0.00";
        
        await pool.query(`
          INSERT INTO content_posts (brand_id, platform, content, url, published_at, views, likes, comments, engagement_rate, created_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        `, [2, 'youtube', post.caption, post.url, publishedAt, views, likes, comments, engagementRate, new Date()]);
        
        insertedCount++;
      } catch (error) {
        console.log('Error inserting YouTube official post:', error.message);
      }
    }
    
    console.log(`Successfully inserted ${insertedCount} content posts for Next Retail`);
    
    // Verify the content was inserted
    const verifyResult = await pool.query('SELECT COUNT(*) as count FROM content_posts WHERE brand_id = 2');
    console.log(`Total Next Retail content posts in database: ${verifyResult.rows[0].count}`);
    
  } catch (error) {
    console.error('Error populating Next Retail content:', error);
  } finally {
    await pool.end();
  }
}

populateNextRetailContent();