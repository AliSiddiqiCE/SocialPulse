const { aggregatePosts } = require('./server/storage.ts');

// Test data processing
console.log('Testing data processing...');

// Test M&S data
console.log('\n=== M&S Data ===');
const msPosts = aggregatePosts(1);
console.log(`Total M&S posts: ${msPosts.length}`);

// Group by platform
const msByPlatform = msPosts.reduce((acc, post) => {
  if (!acc[post.platform]) acc[post.platform] = [];
  acc[post.platform].push(post);
  return acc;
}, {});

Object.keys(msByPlatform).forEach(platform => {
  const posts = msByPlatform[platform];
  const totalLikes = posts.reduce((sum, post) => sum + (post.likes || 0), 0);
  const totalComments = posts.reduce((sum, post) => sum + (post.comments || 0), 0);
  const totalReach = posts.reduce((sum, post) => sum + (post.reach || 0), 0);
  const engagementRate = totalReach > 0 ? ((totalLikes + totalComments) / totalReach) * 100 : 0;
  
  console.log(`${platform}: ${posts.length} posts, ${totalLikes} likes, ${totalComments} comments, ${totalReach} reach, ${engagementRate.toFixed(2)}% engagement`);
});

// Test Next Retail data
console.log('\n=== Next Retail Data ===');
const nextPosts = aggregatePosts(2);
console.log(`Total Next posts: ${nextPosts.length}`);

// Group by platform
const nextByPlatform = nextPosts.reduce((acc, post) => {
  if (!acc[post.platform]) acc[post.platform] = [];
  acc[post.platform].push(post);
  return acc;
}, {});

Object.keys(nextByPlatform).forEach(platform => {
  const posts = nextByPlatform[platform];
  const totalLikes = posts.reduce((sum, post) => sum + (post.likes || 0), 0);
  const totalComments = posts.reduce((sum, post) => sum + (post.comments || 0), 0);
  const totalReach = posts.reduce((sum, post) => sum + (post.reach || 0), 0);
  const engagementRate = totalReach > 0 ? ((totalLikes + totalComments) / totalReach) * 100 : 0;
  
  console.log(`${platform}: ${posts.length} posts, ${totalLikes} likes, ${totalComments} comments, ${totalReach} reach, ${engagementRate.toFixed(2)}% engagement`);
}); 