// Global types for the application

// Sentiment data interface
export interface SentimentRecord {
  brandId: number;
  platform: string;
  sentiment: 'positive' | 'negative' | 'neutral';
  score: number;
  subjectivity: number;
  mentionCount: number;
  text?: string;
  date: Date;
  createdAt: Date;
}

// Extend global namespace to include our sentiment data
declare global {
  var sentimentData: SentimentRecord[];
}
