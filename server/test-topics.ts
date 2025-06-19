import { getKeyTopicsBySentiment } from './storage';

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
      console.log(`- ${topic.topic}: mentioned ${topic.mention_count} times, sentiment score: ${topic.sentiment_score.toFixed(2)}`);
    });
  }
}

testTopics().catch(console.error);
