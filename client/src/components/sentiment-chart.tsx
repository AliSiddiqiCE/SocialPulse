import React, { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent } from "@/components/ui/card";
import { Switch } from "@/components/ui/switch";
import { Heart } from "lucide-react";
import { LineChart, Line, XAxis, YAxis, ResponsiveContainer, Tooltip, Legend, CartesianGrid } from "recharts";
import { formatNumber } from "@/lib/utils";

// Fixed reference date for all date range calculations
const FIXED_REFERENCE_DATE = new Date('2025-05-29T23:59:59.999Z');

interface SentimentChartProps {
  selectedBrand: string;
  selectedPlatform: string;
  selectedDateRange: string;
}

export function SentimentChart({ selectedBrand, selectedPlatform, selectedDateRange }: SentimentChartProps) {
  const [compareAllPlatforms, setCompareAllPlatforms] = useState(true);

  const { data: brand } = useQuery({
    queryKey: ["/api/brands", selectedBrand],
    queryFn: async () => {
      const response = await fetch(`/api/brands/${selectedBrand}`, {
        credentials: "include"
      });
      if (!response.ok) throw new Error("Failed to fetch brand");
      return response.json();
    },
  });

  const { data: metricsData } = useQuery({
    queryKey: ["/api/brands", brand?.id, "metrics", selectedPlatform, selectedDateRange],
    queryFn: async () => {
      if (!brand?.id) return [];
      const params = new URLSearchParams();
      if (selectedPlatform !== "all") params.set("platform", selectedPlatform);
      if (selectedDateRange) params.set("dateRange", selectedDateRange);
      
      const response = await fetch(`/api/brands/${brand.id}/metrics?${params}`, {
        credentials: "include"
      });
      if (!response.ok) return [];
      return response.json();
    },
    enabled: !!brand?.id,
  });
  
  // Fetch real sentiment data from the API
  const { data: sentimentData = [] } = useQuery({
    queryKey: ["/api/brands", brand?.id, "sentiment", selectedPlatform, selectedDateRange],
    queryFn: async () => {
      if (!brand?.id) return [];
      const params = new URLSearchParams();
      if (selectedPlatform !== "all") params.set("platform", selectedPlatform);
      if (selectedDateRange) params.set("dateRange", selectedDateRange);
      
      const response = await fetch(`/api/brands/${brand.id}/sentiment?${params}`, {
        credentials: "include"
      });
      if (!response.ok) return [];
      return response.json();
    },
    enabled: !!brand?.id,
  });

  // Generate time-series data for sentiment using real sentiment data
  const generateChartData = () => {
    if (!sentimentData || sentimentData.length === 0) return [];
    
    // Group sentiment data by date
    const groupedByDate = sentimentData.reduce((acc: Record<string, any[]>, item: any) => {
      const date = new Date(item.date);
      const dateStr = date.toISOString().split('T')[0]; // Format as YYYY-MM-DD
      if (!acc[dateStr]) acc[dateStr] = [];
      acc[dateStr].push(item);
      return acc;
    }, {});
    
    // Sort dates
    const sortedDates = Object.keys(groupedByDate).sort();
    
    // Generate date range based on fixed reference date
    let dateRange: string[] = [];
    switch(selectedDateRange) {
      case '7days':
      case '7d':
        // Generate 7 days backwards from fixed reference date
        dateRange = Array.from({ length: 7 }, (_, index) => {
          const date = new Date(FIXED_REFERENCE_DATE);
          date.setDate(date.getDate() - (6 - index)); // Start 6 days back, go forward
          return date.toISOString().split('T')[0];
        });
        break;
      case '30days':
      case '30d':
        // Generate 30 days backwards from fixed reference date
        dateRange = Array.from({ length: 30 }, (_, index) => {
          const date = new Date(FIXED_REFERENCE_DATE);
          date.setDate(date.getDate() - (29 - index)); // Start 29 days back, go forward
          return date.toISOString().split('T')[0];
        });
        break;
      case '90days':
      case '90d':
        // Generate 90 days backwards from fixed reference date
        dateRange = Array.from({ length: 90 }, (_, index) => {
          const date = new Date(FIXED_REFERENCE_DATE);
          date.setDate(date.getDate() - (89 - index)); // Start 89 days back, go forward
          return date.toISOString().split('T')[0];
        });
        break;
      default:
        dateRange = sortedDates;
    }
    
    // Determine date format based on selected range
    let dateFormat: string;
    switch(selectedDateRange) {
      case '7days':
      case '7d':
        dateFormat = 'EEE'; // Mon, Tue, etc.
        break;
      case '30days':
      case '30d':
        dateFormat = 'MMM d'; // Jan 1, etc.
        break;
      case '90days':
      case '90d':
        dateFormat = 'MMM d'; // Jan 1, etc.
        break;
      default:
        dateFormat = 'MMM d';
    }
    
    if (compareAllPlatforms) {
      const platforms = ['instagram', 'tiktok', 'youtube'];
      
      return dateRange.map(dateStr => {
        const dataPoint: any = { date: dateStr };
        
        platforms.forEach(platform => {
          const platformItems = groupedByDate[dateStr]?.filter((item: any) => item.platform === platform) || [];
          
          if (platformItems.length > 0) {
            // Calculate average sentiment score for this platform on this date
            const positiveCount = platformItems.filter((item: any) => item.sentiment === 'positive').length;
            const neutralCount = platformItems.filter((item: any) => item.sentiment === 'neutral').length;
            const negativeCount = platformItems.filter((item: any) => item.sentiment === 'negative').length;
            
            // Calculate the weighted sentiment score based on actual sentiment counts
            const totalItems = platformItems.length;
            const weightedScore = ((positiveCount * 100) + (neutralCount * 50) + (negativeCount * 0)) / totalItems;
            
            dataPoint[`${platform}_sentiment`] = Math.round(weightedScore);
            dataPoint[`${platform}_count`] = totalItems; // Store the actual count for reference
          } else {
            dataPoint[`${platform}_sentiment`] = null; // No data for this platform on this date
            dataPoint[`${platform}_count`] = 0;
          }
        });
        
        return dataPoint;
      });
    } else {
      return dateRange.map(dateStr => {
        const items = groupedByDate[dateStr] || [];
        let filteredItems = items;
        
        // Filter by platform if not 'all'
        if (selectedPlatform !== 'all') {
          filteredItems = items.filter((item: any) => item.platform === selectedPlatform);
        }
        
        if (filteredItems.length === 0) {
          return {
            date: dateStr,
            sentiment: 50, // Default to neutral if no data
            count: 0
          };
        }
        
        // Count sentiment categories
        const positiveCount = filteredItems.filter((item: any) => item.sentiment === 'positive').length;
        const neutralCount = filteredItems.filter((item: any) => item.sentiment === 'neutral').length;
        const negativeCount = filteredItems.filter((item: any) => item.sentiment === 'negative').length;
        
        // Calculate weighted sentiment score
        const totalItems = filteredItems.length;
        const weightedScore = ((positiveCount * 100) + (neutralCount * 50) + (negativeCount * 0)) / totalItems;
        
        return {
          date: dateStr,
          sentiment: Math.round(weightedScore),
          count: totalItems, // Store the actual post count
          positive: positiveCount,
          neutral: neutralCount,
          negative: negativeCount
        };
      });
    }
  };

  const chartData = generateChartData();

  return (
    <Card className="bg-white shadow-sm border">
      <CardContent className="p-6">
        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center space-x-4">
            <Heart className="text-gray-600" size={20} />
            <span className="font-semibold text-navy">Sentiment</span>
          </div>
          <div className="flex items-center space-x-2">
            <span className="text-sm text-gray-600">Compare all platforms</span>
            <Switch 
              checked={compareAllPlatforms}
              onCheckedChange={setCompareAllPlatforms}
            />
          </div>
        </div>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData}>
              <XAxis 
                dataKey="date" 
                axisLine={true}
                tickLine={true}
                className="text-sm text-gray-500"
                stroke="#e2e8f0"
                tick={{ fontSize: 12, fill: '#6b7280' }}
              />
              <YAxis 
                axisLine={true}
                tickLine={true}
                className="text-sm text-gray-500"
                domain={[0, 100]}
                tickFormatter={(value) => `${value}`}
                stroke="#e2e8f0"
                tick={{ fontSize: 12, fill: '#6b7280' }}
              />
              <CartesianGrid 
                strokeDasharray="3 3" 
                stroke="#f1f5f9" 
                vertical={false}
              />
              <Tooltip 
                contentStyle={{
                  backgroundColor: 'white',
                  border: '1px solid #e2e8f0',
                  borderRadius: '8px',
                  boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)'
                }}
                formatter={(value: any, name: string, props: any) => {
                  // Extract the platform from the dataKey if it's a platform-specific sentiment
                  const isPlatformSentiment = name.includes('_sentiment');
                  const platform = isPlatformSentiment ? name.split('_')[0] : '';
                  
                  // Get the count if available
                  const countKey = isPlatformSentiment ? `${platform}_count` : 'count';
                  const count = props.payload[countKey] || 0;
                  
                  // For sentiment values, show both the sentiment score and post count
                  if (name === 'sentiment' || name.includes('_sentiment')) {
                    return [`Score: ${value}, Posts: ${count}`, name.replace('_sentiment', '')];
                  }
                  
                  return [value, name];
                }}
              />
              {compareAllPlatforms && (
                <Legend 
                  verticalAlign="bottom" 
                  height={36}
                  wrapperStyle={{ paddingTop: '20px' }}
                />
              )}
              {compareAllPlatforms ? (
                <>
                  <Line 
                    type="monotone" 
                    dataKey="instagram_sentiment"
                    stroke="#E1306C" 
                    strokeWidth={2}
                    dot={{ fill: "#E1306C", strokeWidth: 0, r: 4 }}
                    name="Instagram"
                  />
                  <Line 
                    type="monotone" 
                    dataKey="tiktok_sentiment"
                    stroke="#000000" 
                    strokeWidth={2}
                    dot={{ fill: "#000000", strokeWidth: 0, r: 4 }}
                    name="TikTok"
                  />
                  <Line 
                    type="monotone" 
                    dataKey="youtube_sentiment"
                    stroke="#FF0000" 
                    strokeWidth={2}
                    dot={{ fill: "#FF0000", strokeWidth: 0, r: 4 }}
                    name="YouTube"
                  />
                </>
              ) : (
                <Line 
                  type="monotone" 
                  dataKey="sentiment" 
                  stroke="#22C55E" 
                  strokeWidth={2}
                  fill="#22C55E"
                  fillOpacity={0.1}
                  dot={{ fill: "#22C55E", strokeWidth: 0, r: 4 }}
                  name="Sentiment"
                />
              )}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}