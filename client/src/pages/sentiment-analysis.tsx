import { useState, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { LineChart, Line, XAxis, YAxis, ResponsiveContainer, Tooltip, CartesianGrid } from "recharts";
import { TopNav } from "@/components/top-nav";
import { Sidebar } from "@/components/sidebar";
import { formatNumber } from "@/lib/utils";

// Fixed reference date for all date range calculations
const FIXED_REFERENCE_DATE = new Date('2025-05-29T23:59:59.999Z');

export default function SentimentAnalysis() {
  const [selectedBrand, setSelectedBrand] = useState("marks-spencer");
  const [selectedPlatform, setSelectedPlatform] = useState("all");
  const [selectedDateRange, setSelectedDateRange] = useState("30days");
  const [activeTopicTab, setActiveTopicTab] = useState<"all" | "positive" | "neutral" | "negative">("all");

  // Fetch brand data based on selected brand
  const { data: brand } = useQuery({
    queryKey: ["/api/brands", selectedBrand],
    queryFn: async () => {
      const response = await fetch(`/api/brands/${selectedBrand}`, {
        credentials: "include"
      });
      if (!response.ok) return null;
      return response.json();
    },
  });

  // Fetch sentiment data for the selected brand
  const { data: sentimentData = [], refetch } = useQuery({
    queryKey: ["/api/brands", brand?.id, "sentiment", selectedPlatform, selectedDateRange],
    queryFn: async () => {
      if (!brand?.id) return [];
      console.log('Fetching sentiment data for brand:', brand.id, 'platform:', selectedPlatform, 'dateRange:', selectedDateRange);
      const platformParam = selectedPlatform || 'all';
      const url = `/api/brands/${brand.id}/sentiment?platform=${encodeURIComponent(platformParam)}&dateRange=${encodeURIComponent(selectedDateRange)}`;
      console.log('API URL:', url);
      
      const response = await fetch(url, {
        credentials: "include"
      });
      if (!response.ok) {
        console.error('API response not ok:', response.status, response.statusText);
        return [];
      }
      const data = await response.json();
      console.log('Received sentiment data:', data.length, 'items for platform:', selectedPlatform);
      console.log('Sample data:', data.slice(0, 3));
      return data;
    },
    enabled: !!brand?.id,
  });

  // Force refetch when platform changes
  useEffect(() => {
    console.log('Platform changed to:', selectedPlatform, 'triggering refetch');
  }, [selectedPlatform]);

  // Calculate overall sentiment metrics from authentic data with client-side filtering
  const calculateSentimentMetrics = () => {
    if (!sentimentData || sentimentData.length === 0) {
      return { positive: 0, neutral: 0, negative: 0 };
    }
    
    // Filter data by platform on the client side to ensure it works
    let filteredData = sentimentData;
    if (selectedPlatform && selectedPlatform !== 'all') {
      filteredData = sentimentData.filter((item: any) => item.platform === selectedPlatform);
    }
    
    console.log('Calculating metrics for platform:', selectedPlatform, 'filtered data:', filteredData.length, 'items');
    
    if (filteredData.length === 0) {
      return { positive: 0, neutral: 0, negative: 0 };
    }
    
    const total = filteredData.length;
    const positive = filteredData.filter((item: any) => item.sentiment === 'positive').length;
    const negative = filteredData.filter((item: any) => item.sentiment === 'negative').length;
    const neutral = filteredData.filter((item: any) => item.sentiment === 'neutral').length;
    
    const result = {
      positive: Math.round((positive / total) * 100),
      neutral: Math.round((neutral / total) * 100),
      negative: Math.round((negative / total) * 100)
    };
    
    console.log('Sentiment distribution for', selectedPlatform + ':', result);
    return result;
  };

  const overallSentiment = calculateSentimentMetrics();

  // Generate sentiment trends data based on real sentiment data
  const generateTrendsData = () => {
    if (!sentimentData || sentimentData.length === 0) {
      return [];
    }
    
    // Group sentiment data by date
    const groupedByDate = sentimentData.reduce((acc: Record<string, any[]>, item: any) => {
      // Extract just the date part (YYYY-MM-DD)
      const date = new Date(item.date);
      const dateStr = date.toISOString().split('T')[0];
      
      if (!acc[dateStr]) acc[dateStr] = [];
      acc[dateStr].push(item);
      return acc;
    }, {});
    
    // Sort dates
    const sortedDates = Object.keys(groupedByDate).sort();
    
    // Format dates based on selected range using fixed reference date
    let formattedDates: {dateStr: string, displayLabel: string}[] = [];
    
    switch(selectedDateRange) {
      case '7days':
      case '7d':
        // For 7 days, show each day based on fixed reference date
        formattedDates = Array.from({ length: 7 }, (_, index) => {
          const date = new Date(FIXED_REFERENCE_DATE);
          date.setDate(date.getDate() - (6 - index)); // Start 6 days back, go forward
          const dateStr = date.toISOString().split('T')[0];
          const day = date.toLocaleDateString('en-US', { weekday: 'short' }); // Mon, Tue, etc.
          return { dateStr, displayLabel: day };
        });
        break;
        
      case '30days':
      case '30d':
        // For 30 days, group by every few days based on fixed reference date
        formattedDates = Array.from({ length: 15 }, (_, index) => {
          const date = new Date(FIXED_REFERENCE_DATE);
          date.setDate(date.getDate() - (29 - (index * 2))); // Start 29 days back, go forward by 2s
          const dateStr = date.toISOString().split('T')[0];
          const day = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }); // Jan 1, etc.
          return { dateStr, displayLabel: day };
        });
        break;
        
      case '90days':
      case '90d':
        // For longer periods, group by week or month based on fixed reference date
        formattedDates = Array.from({ length: 12 }, (_, index) => {
          const date = new Date(FIXED_REFERENCE_DATE);
          date.setDate(date.getDate() - (89 - (index * 7))); // Start 89 days back, go forward by weeks
          const dateStr = date.toISOString().split('T')[0];
          const day = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
          return { dateStr, displayLabel: day };
        });
        break;
        
      default:
        formattedDates = sortedDates.map(dateStr => {
          const date = new Date(dateStr);
          const day = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
          return { dateStr, displayLabel: day };
        });
    }
    
    // Calculate sentiment percentages for each date
    return formattedDates.map(({ dateStr, displayLabel }) => {
      const dayData = groupedByDate[dateStr];
      
      // Filter by platform if not 'all'
      const filteredData = selectedPlatform && selectedPlatform !== 'all'
        ? dayData.filter((item: any) => item.platform === selectedPlatform)
        : dayData;
      
      if (filteredData.length === 0) {
        return { day: displayLabel, positive: 0, neutral: 0, negative: 0 };
      }
      
      const total = filteredData.length;
      const positive = filteredData.filter((item: any) => item.sentiment === 'positive').length;
      const negative = filteredData.filter((item: any) => item.sentiment === 'negative').length;
      const neutral = filteredData.filter((item: any) => item.sentiment === 'neutral').length;
      
      return {
        day: displayLabel,
        positive: Math.round((positive / total) * 100),
        neutral: Math.round((neutral / total) * 100),
        negative: Math.round((negative / total) * 100)
      };
    });
  };

  const sentimentTrends = generateTrendsData();

  // Fetch authentic topics data from your datasets for the selected brand
  const { data: topicsData = [], isLoading: topicsLoading, error: topicsError } = useQuery({
    queryKey: ["/api/brands", brand?.id, "topics", selectedPlatform, selectedDateRange],
    queryFn: async () => {
      if (!brand?.id) return [];
      const response = await fetch(`/api/brands/${brand.id}/topics?platform=${selectedPlatform}&dateRange=${selectedDateRange}`, {
        credentials: "include"
      });
      if (!response.ok) {
        throw new Error(`Failed to fetch topics: ${response.status}`);
      }
      const data = await response.json();
      console.log('Retrieved topics data for brand:', brand.id, 'platform:', selectedPlatform, 'dateRange:', selectedDateRange, data);
      console.log('Topics response status:', response.status);
      console.log('Topics data type:', typeof data, 'isArray:', Array.isArray(data));
      return data;
    },
    enabled: !!brand?.id,
    retry: 3,
    retryDelay: 1000,
  });

  // Transform authentic topics data for display
  console.log('Raw topics data:', topicsData);
  console.log('Is array:', Array.isArray(topicsData));
  console.log('Length:', topicsData?.length);
  
  const keyTopics = Array.isArray(topicsData) ? topicsData.map((topic: any) => ({
    topic: topic.topic,
    mentioned: `${topic.mention_count} times`,
    sentiment: Math.round(topic.sentiment_score),
    color: topic.sentiment_score >= 85 ? "bg-green-500" : 
           topic.sentiment_score >= 75 ? "bg-yellow-500" : "bg-red-500"
  })) : [];
  
  console.log('Transformed topics:', keyTopics);

  const topicTabs = [
    { id: "all", label: "All Topics", count: 10 },
    { id: "positive", label: "Positive", count: 10 },
    { id: "neutral", label: "Neutral", count: 0 },
    { id: "negative", label: "Negative", count: 0 },
  ];

  const platformOptions = [
    { value: "all", label: "All Platforms" },
    { value: "instagram", label: "Instagram" },
    { value: "tiktok", label: "TikTok" },
    { value: "youtube", label: "YouTube" },
  ];

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="flex">
        <div className="fixed left-0 top-0 z-50">
          <Sidebar />
        </div>
        <div className="flex-1 ml-60">
          <TopNav 
            selectedBrand={selectedBrand}
            onBrandChange={setSelectedBrand}
            selectedPlatform={selectedPlatform}
            onPlatformChange={setSelectedPlatform}
            selectedDateRange={selectedDateRange}
            onDateRangeChange={setSelectedDateRange}
          />
          <div className="p-8">
            {/* Header */}
            <div className="mb-8">
              <div className="flex items-center justify-between mb-4">
                <div>
                  <h1 className="text-2xl font-bold text-navy mb-2">Sentiment Analysis</h1>
                  <p className="text-gray-600">Track customer sentiment, identify key topics, and analyze emotional trends across your social media platforms.</p>
                </div>
              </div>
            </div>

            {/* Main Content */}
            <div className="text-center py-8">
              <p className="text-gray-500">Most sentiment analysis components have been moved to the Analytics tab for a consolidated view.</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}