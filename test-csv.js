import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse/sync';

// Helper to read and parse a CSV file from the public folder
function readCSV(filename) {
  const filePath = path.join(process.cwd(), 'public', filename);
  console.log('Reading file:', filePath);
  if (!fs.existsSync(filePath)) {
    console.log('File does not exist:', filePath);
    return [];
  }
  const content = fs.readFileSync(filePath, 'utf-8');
  const data = parse(content, { columns: true, skip_empty_lines: true });
  console.log(`Read ${data.length} rows from ${filename}`);
  return data;
}

// Test the CSV reading
console.log('Testing CSV file reading...\n');

// Test TikTok files
const tiktokOfficial = readCSV('mands_tik_off.csv');
const tiktokHashtag = readCSV('mands_tik_hash.csv');

if (tiktokOfficial.length > 0) {
  console.log('Sample TikTok official data:');
  console.log('First row:', tiktokOfficial[0]);
  console.log('Total likes (diggCount):', tiktokOfficial.reduce((a, b) => a + Number(b.diggCount || 0), 0));
  console.log('Total views (playCount):', tiktokOfficial.reduce((a, b) => a + Number(b.playCount || 0), 0));
  console.log('Total comments:', tiktokOfficial.reduce((a, b) => a + Number(b.commentCount || 0), 0));
  console.log('Total shares:', tiktokOfficial.reduce((a, b) => a + Number(b.shareCount || 0), 0));
}

if (tiktokHashtag.length > 0) {
  console.log('\nSample TikTok hashtag data:');
  console.log('First row:', tiktokHashtag[0]);
  console.log('Total likes (diggCount):', tiktokHashtag.reduce((a, b) => a + Number(b.diggCount || 0), 0));
  console.log('Total views (playCount):', tiktokHashtag.reduce((a, b) => a + Number(b.playCount || 0), 0));
  console.log('Total comments:', tiktokHashtag.reduce((a, b) => a + Number(b.commentCount || 0), 0));
  console.log('Total shares:', tiktokHashtag.reduce((a, b) => a + Number(b.shareCount || 0), 0));
}

// Test Instagram files
const instagramOfficial = readCSV('mands_insta_off.csv');
const instagramHashtag = readCSV('mands_insta_hash.csv');

if (instagramOfficial.length > 0) {
  console.log('\nSample Instagram official data:');
  console.log('First row:', instagramOfficial[0]);
  console.log('Total likes:', instagramOfficial.reduce((a, b) => a + Number(b.likesCount || 0), 0));
  console.log('Total comments:', instagramOfficial.reduce((a, b) => a + Number(b.commentsCount || 0), 0));
}

if (instagramHashtag.length > 0) {
  console.log('\nSample Instagram hashtag data:');
  console.log('First row:', instagramHashtag[0]);
  console.log('Total likes:', instagramHashtag.reduce((a, b) => a + Number(b.likesCount || 0), 0));
  console.log('Total comments:', instagramHashtag.reduce((a, b) => a + Number(b.commentsCount || 0), 0));
}

console.log('\nTest completed!'); 