import { Pool, neonConfig } from '@neondatabase/serverless';
import ws from 'ws';
neonConfig.webSocketConstructor = ws;

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

// Simple sentiment analysis function
function analyzeSentiment(text) {
  if (!text) return { sentiment: 'neutral', score: 0.5 };
  
  const lowerText = text.toLowerCase();
  
  const positiveWords = [
    'amazing', 'awesome', 'beautiful', 'best', 'brilliant', 'excellent', 'fantastic', 
    'gorgeous', 'great', 'incredible', 'love', 'lovely', 'perfect', 'stunning', 
    'wonderful', 'good', 'nice', 'happy', 'excited', 'favorite', 'delicious',
    'unreal', 'standout', 'fab', 'fabulous', 'adorable', 'cute', 'stylish'
  ];
  
  const negativeWords = [
    'awful', 'bad', 'disappointing', 'hate', 'horrible', 'poor', 'terrible', 
    'worst', 'ugly', 'boring', 'expensive', 'overpriced', 'cheap', 'disgusting',
    'uncomfortable', 'useless', 'annoying', 'frustrating', 'disappointing'
  ];
  
  let positiveScore = 0;
  let negativeScore = 0;
  
  positiveWords.forEach(word => {
    const regex = new RegExp(`\\b${word}\\b`, 'gi');
    const matches = lowerText.match(regex);
    if (matches) positiveScore += matches.length;
  });
  
  negativeWords.forEach(word => {
    const regex = new RegExp(`\\b${word}\\b`, 'gi');
    const matches = lowerText.match(regex);
    if (matches) negativeScore += matches.length;
  });
  
  // Normalize scores to a 0-1 range
  const totalScore = positiveScore + negativeScore;
  if (totalScore === 0) return { sentiment: 'neutral', score: 0.5 };
  
  const normalizedScore = positiveScore / totalScore;
  
  // Use thresholds for sentiment classification
  if (normalizedScore > 0.6) {
    return { sentiment: 'positive', score: normalizedScore };
  } else if (normalizedScore < 0.4) {
    return { sentiment: 'negative', score: normalizedScore };
  } else {
    return { sentiment: 'neutral', score: normalizedScore };
  }
}

async function extractSentiments() {
  try {
    console.log('Starting sentiment analysis extraction...');
    
    // Clear existing sentiment data
    await pool.query('DELETE FROM sentiment_data WHERE brand_id = 1');
    
    // Get Instagram data
    const instagramResult = await pool.query(`
      SELECT caption, hashtags 
      FROM mark_spencers_dataset_instagram 
      WHERE caption IS NOT NULL AND caption != ''
      LIMIT 50
    `);
    
    console.log(`Processing ${instagramResult.rows.length} Instagram posts...`);
    
    for (const row of instagramResult.rows) {
      const textContent = `${row.caption} ${row.hashtags || ''}`;
      const analysis = analyzeSentiment(textContent);
      
      await pool.query(`
        INSERT INTO sentiment_data (brand_id, platform, sentiment, mention_count, score, date)
        VALUES ($1, $2, $3, $4, $5, $6)
      `, [1, 'instagram', analysis.sentiment, 1, analysis.score.toString(), new Date().toISOString().split('T')[0]]);
    }
    
    // Get TikTok data
    const tiktokResult = await pool.query(`
      SELECT video_description, hashtags 
      FROM mark_spencers_dataset_tiktok 
      WHERE video_description IS NOT NULL AND video_description != ''
      LIMIT 50
    `);
    
    console.log(`Processing ${tiktokResult.rows.length} TikTok videos...`);
    
    for (const row of tiktokResult.rows) {
      const textContent = `${row.video_description} ${row.hashtags || ''}`;
      const analysis = analyzeSentiment(textContent);
      
      await pool.query(`
        INSERT INTO sentiment_data (brand_id, platform, sentiment, mention_count, score, date)
        VALUES ($1, $2, $3, $4, $5, $6)
      `, [1, 'tiktok', analysis.sentiment, 1, analysis.score.toString(), new Date().toISOString().split('T')[0]]);
    }
    
    // Get YouTube data
    const youtubeResult = await pool.query(`
      SELECT video_description, channel_name 
      FROM mark_spencers_dataset_youtube 
      WHERE video_description IS NOT NULL AND video_description != ''
      LIMIT 50
    `);
    
    console.log(`Processing ${youtubeResult.rows.length} YouTube videos...`);
    
    for (const row of youtubeResult.rows) {
      const textContent = `${row.video_description} ${row.channel_name || ''}`;
      const analysis = analyzeSentiment(textContent);
      
      await pool.query(`
        INSERT INTO sentiment_data (brand_id, platform, sentiment, mention_count, score, date)
        VALUES ($1, $2, $3, $4, $5, $6)
      `, [1, 'youtube', analysis.sentiment, 1, analysis.score.toString(), new Date().toISOString().split('T')[0]]);
    }
    
    // Get summary statistics
    const statsResult = await pool.query(`
      SELECT 
        platform,
        sentiment,
        COUNT(*) as count
      FROM sentiment_data 
      WHERE brand_id = 1
      GROUP BY platform, sentiment
      ORDER BY platform, sentiment
    `);
    
    console.log('\nSentiment analysis completed successfully!');
    console.log('\nSummary Statistics:');
    console.log(statsResult.rows);
    
    await pool.end();
    
  } catch (error) {
    console.error('Error in sentiment analysis:', error);
    await pool.end();
    process.exit(1);
  }
}

extractSentiments();