import { Pool } from '@neondatabase/serverless';
import fetch from 'node-fetch';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const SENTIMENT_API_URL = process.env.SENTIMENT_API_URL || 'http://localhost:5001/analyze';

// TextBlob-based sentiment analysis through Python API
async function analyzeSentiment(text) {
  if (!text || typeof text !== 'string') return { sentiment: 'neutral', score: 0.5 };
  
  try {
    const response = await fetch(SENTIMENT_API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ text }),
    });

    if (!response.ok) {
      console.error('Sentiment API error:', await response.text());
      // Fallback to neutral if API fails
      return { sentiment: 'neutral', score: 0.5 };
    }

    const result = await response.json();
    return {
      sentiment: result.sentiment,
      score: result.score,
      subjectivity: result.subjectivity
    };
  } catch (error) {
    console.error('Error calling sentiment API:', error);
    // Fallback to neutral if API call fails
    return { sentiment: 'neutral', score: 0.5 };
  }
}

async function populateNextRetailSentiment() {
  console.log('Starting authentic sentiment analysis for Next Retail...');
  
  try {
    // Instagram content analysis
    const instagramQuery = `
      SELECT caption, timestamp, "videoViewCount", "commentsCount"
      FROM "nextretail_instagram_official" 
      WHERE caption IS NOT NULL AND caption != ''
      ORDER BY timestamp DESC
      LIMIT 30
    `;
    
    const instagramResult = await pool.query(instagramQuery);
    
    for (const row of instagramResult.rows) {
      const { sentiment, score } = analyzeSentiment(row.caption);
      const mentionCount = Math.max(1, Math.floor(parseInt(row.commentsCount) || 1));
      
      await pool.query(`
        INSERT INTO sentiment_data (brand_id, platform, sentiment, score, mention_count, date, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
      `, [2, 'instagram', sentiment, score, mentionCount, row.timestamp]);
    }
    
    console.log(`Processed ${instagramResult.rows.length} Instagram posts`);
    
    // TikTok content analysis
    const tiktokQuery = `
      SELECT text, created_time, "diggCount", "commentCount"
      FROM "nextretail_tiktok_official" 
      WHERE text IS NOT NULL AND text != ''
      ORDER BY created_time DESC
      LIMIT 30
    `;
    
    const tiktokResult = await pool.query(tiktokQuery);
    
    for (const row of tiktokResult.rows) {
      const { sentiment, score } = analyzeSentiment(row.text);
      const mentionCount = Math.max(1, Math.floor(parseInt(row.commentCount) || 1));
      
      // Parse date format DD/MM/YYYY
      let date;
      try {
        const [day, month, year] = row.created_time.split('/');
        date = new Date(`${year}-${month}-${day}`);
      } catch {
        date = new Date();
      }
      
      await pool.query(`
        INSERT INTO sentiment_data (brand_id, platform, sentiment, score, mention_count, date, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
      `, [2, 'tiktok', sentiment, score, mentionCount, date]);
    }
    
    console.log(`Processed ${tiktokResult.rows.length} TikTok posts`);
    
    // YouTube content analysis
    const youtubeQuery = `
      SELECT title, "viewCount"
      FROM "nextretail_youtube_official" 
      WHERE title IS NOT NULL AND title != ''
      ORDER BY "viewCount" DESC
      LIMIT 30
    `;
    
    const youtubeResult = await pool.query(youtubeQuery);
    
    for (const row of youtubeResult.rows) {
      const { sentiment, score } = analyzeSentiment(row.title);
      const mentionCount = Math.max(1, Math.floor(parseInt(row.viewCount) / 1000 || 1));
      
      await pool.query(`
        INSERT INTO sentiment_data (brand_id, platform, sentiment, score, mention_count, date, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
      `, [2, 'youtube', sentiment, score, mentionCount, new Date()]);
    }
    
    console.log(`Processed ${youtubeResult.rows.length} YouTube videos`);
    
    // Verify results
    const countResult = await pool.query(`
      SELECT platform, sentiment, COUNT(*) as count 
      FROM sentiment_data 
      WHERE brand_id = 2 
      GROUP BY platform, sentiment
      ORDER BY platform, sentiment
    `);
    
    console.log('Next Retail sentiment distribution:');
    countResult.rows.forEach(row => {
      console.log(`${row.platform} - ${row.sentiment}: ${row.count}`);
    });
    
  } catch (error) {
    console.error('Error populating sentiment data:', error);
  }
}

async function populateMarksSpencerSentiment() {
  console.log('Refreshing Marks & Spencer sentiment analysis...');
  
  try {
    // Clear existing M&S data
    await pool.query('DELETE FROM sentiment_data WHERE brand_id = 1');
    
    // Instagram content analysis
    const instagramQuery = `
      SELECT caption, timestamp, "likesCount", "commentsCount"
      FROM "ms_instagram_official" 
      WHERE caption IS NOT NULL AND caption != ''
      ORDER BY timestamp DESC
      LIMIT 30
    `;
    
    const instagramResult = await pool.query(instagramQuery);
    
    for (const row of instagramResult.rows) {
      const { sentiment, score } = analyzeSentiment(row.caption);
      const mentionCount = Math.max(1, Math.floor(parseInt(row.commentsCount) || 1));
      
      await pool.query(`
        INSERT INTO sentiment_data (brand_id, platform, sentiment, score, mention_count, date, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
      `, [1, 'instagram', sentiment, score, mentionCount, row.timestamp]);
    }
    
    console.log(`Processed ${instagramResult.rows.length} M&S Instagram posts`);
    
    // TikTok content analysis  
    const tiktokQuery = `
      SELECT text, created_time, "diggCount", "commentCount"
      FROM "ms_tiktok_official" 
      WHERE text IS NOT NULL AND text != ''
      ORDER BY created_time DESC
      LIMIT 30
    `;
    
    const tiktokResult = await pool.query(tiktokQuery);
    
    for (const row of tiktokResult.rows) {
      const { sentiment, score } = analyzeSentiment(row.text);
      const mentionCount = Math.max(1, Math.floor(parseInt(row.commentCount) || 1));
      
      let date;
      try {
        const [day, month, year] = row.created_time.split('/');
        date = new Date(`${year}-${month}-${day}`);
      } catch {
        date = new Date();
      }
      
      await pool.query(`
        INSERT INTO sentiment_data (brand_id, platform, sentiment, score, mention_count, date, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
      `, [1, 'tiktok', sentiment, score, mentionCount, date]);
    }
    
    console.log(`Processed ${tiktokResult.rows.length} M&S TikTok posts`);
    
    // YouTube content analysis
    const youtubeQuery = `
      SELECT title, "viewCount"
      FROM "ms_youtube_official" 
      WHERE title IS NOT NULL AND title != ''
      ORDER BY "viewCount" DESC
      LIMIT 30
    `;
    
    const youtubeResult = await pool.query(youtubeQuery);
    
    for (const row of youtubeResult.rows) {
      const { sentiment, score } = analyzeSentiment(row.title);
      const mentionCount = Math.max(1, Math.floor(parseInt(row.viewCount) / 1000 || 1));
      
      await pool.query(`
        INSERT INTO sentiment_data (brand_id, platform, sentiment, score, mention_count, date, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
      `, [1, 'youtube', sentiment, score, mentionCount, new Date()]);
    }
    
    console.log(`Processed ${youtubeResult.rows.length} M&S YouTube videos`);
    
  } catch (error) {
    console.error('Error populating M&S sentiment data:', error);
  }
}

async function main() {
  await populateMarksSpencerSentiment();
  await populateNextRetailSentiment();
  await pool.end();
  console.log('Sentiment analysis complete!');
}

main();