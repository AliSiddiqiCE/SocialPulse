// Simple test script to verify topic extraction
const { getKeyTopicsBySentiment } = require('./dist/storage');

async function testTopics() {
  console.log('Testing topics extraction for Next brand (brandId=2)');
  
  // Test with all platforms
  const allTopics = await getKeyTopicsBySentiment(2);
  console.log(`Found ${allTopics.length} topics across all platforms`);
  
  // Test with specific platform
  const instaTopics = await getKeyTopicsBySentiment(2, 'instagram');
  console.log(`Found ${instaTopics.length} topics for Instagram`);
  
  // Print some sample topics
  if (allTopics.length > 0) {
    console.log('Sample topics:');
    allTopics.slice(0, 5).forEach(topic => {
      console.log(`- ${topic.topic}: mentioned ${topic.mention_count} times, sentiment score: ${Math.round(topic.sentiment_score)}%`);
    });
  } else {
    console.log('No topics found. Checking CSV files...');
    
    // Import fs to check CSV files
    const fs = require('fs');
    const path = require('path');
    
    const files = [
      'next_insta_off.csv',
      'next_tik_off.csv',
      'next_yt_off.csv'
    ];
    
    for (const file of files) {
      const filePath = path.join(process.cwd(), '..', 'public', file);
      try {
        const exists = fs.existsSync(filePath);
        const stats = exists ? fs.statSync(filePath) : null;
        console.log(`File ${file}: exists=${exists}, size=${stats ? stats.size : 'N/A'} bytes`);
        
        if (exists && stats.size > 0) {
          // Read first few lines to check structure
          const content = fs.readFileSync(filePath, 'utf-8').split('\n').slice(0, 3).join('\n');
          console.log(`First 3 lines of ${file}:`);
          console.log(content);
        }
      } catch (err) {
        console.error(`Error checking file ${file}:`, err);
      }
    }
  }
}

testTopics().catch(console.error);
