import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { Bar, BarChart, ResponsiveContainer, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, PieChart, Pie, Cell, LineChart, Line } from "recharts";
import { MessageSquare, Eye, ThumbsUp, ThumbsDown, Smile, Info } from "lucide-react";
import { TopNav } from "@/components/top-nav";
import { Sidebar } from "@/components/sidebar";
import { useState, useEffect } from "react";
import { formatNumber } from "@/lib/utils";

// Fixed reference date for all date range calculations
const FIXED_REFERENCE_DATE = new Date('2025-05-29T23:59:59.999Z');

export default function Analytics() {
  const [selectedBrand, setSelectedBrand] = useState("marks-spencer");
  const [selectedPlatform, setSelectedPlatform] = useState("all");
  const [selectedDateRange, setSelectedDateRange] = useState("30days");
  const [activeTopicTab, setActiveTopicTab] = useState("all");

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

  // Fetch analytics metrics
  const { data: metricsData } = useQuery({
    queryKey: ["/api/brands", brand?.id, "analytics", selectedPlatform, selectedDateRange],
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

  // Fetch content data for content analysis
  const { data: contentData } = useQuery({
    queryKey: ["/api/brands", brand?.id, "content", selectedPlatform, selectedDateRange],
    queryFn: async () => {
      if (!brand?.id) return [];
      const params = new URLSearchParams();
      if (selectedPlatform !== "all") params.set("platform", selectedPlatform);
      if (selectedDateRange) params.set("dateRange", selectedDateRange);
      
      const response = await fetch(`/api/brands/${brand.id}/content?${params}`, {
        credentials: "include"
      });
      if (!response.ok) return [];
      return response.json();
    },
    enabled: !!brand?.id,
  });

  // Fetch sentiment data
  const { data: sentimentData } = useQuery({
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

  // Fetch topics data
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
      return data;
    },
    enabled: !!brand?.id,
    retry: 3,
    retryDelay: 1000,
  });

  // Generate analytics data from authentic database metrics
  const getAnalyticsData = () => {
    if (!metricsData || metricsData.length === 0) {
      return {
        mentions: 0,
        reach: 0,
        interactions: 0,
        negativeMentions: 0,
        positiveMentions: 0
      };
    }

    const totals = metricsData.reduce((acc: any, metric: any) => {
      acc.mentions = (metric.mentions || 0);
      acc.reach = (metric.reach || 0);
      acc.interactions = (metric.likes || 0) + (metric.comments || 0) + (metric.shares || 0);
      return acc;
    }, { mentions: 0, reach: 0, interactions: 0 });

    // Get the platform sentiment percentages for all selected platforms
    const platformData = getPlatformSentimentData();
    
    // For 'all' platforms, calculate weighted average of percentages
    const totalMentions = totals.mentions;
    let positiveMentions = 0;
    let negativeMentions = 0;

    if (selectedPlatform === 'all') {
      // Calculate weighted percentages across all platforms
      const totalPlatformMentions = platformData.reduce((sum, p) => {
        const platformMentions = sentimentData.filter((item: any) => 
          item.platform.toLowerCase() === p.platform.toLowerCase()
        ).length;
        return sum + platformMentions;
      }, 0);

      platformData.forEach(p => {
        const platformMentions = sentimentData.filter((item: any) => 
          item.platform.toLowerCase() === p.platform.toLowerCase()
        ).length;
        const weight = platformMentions / totalPlatformMentions;
        
        positiveMentions += Math.round((p.positive / 100) * totalMentions * weight);
        negativeMentions += Math.round((p.negative / 100) * totalMentions * weight);
      });
    } else {
      // Single platform selected
      const platformInfo = platformData.find(p => 
        p.platform.toLowerCase() === selectedPlatform.toLowerCase()
      );
      if (platformInfo) {
        positiveMentions = Math.round((platformInfo.positive / 100) * totalMentions);
        negativeMentions = Math.round((platformInfo.negative / 100) * totalMentions);
      }
    }

    return {
      mentions: totals.mentions,
      reach: totals.reach,
      interactions: totals.interactions,
      negativeMentions,
      positiveMentions
    };
  };

  // Generate platform sentiment data from authentic sentiment analysis
  const getPlatformSentimentData = () => {
    if (!sentimentData || sentimentData.length === 0) return [];

    const platforms = selectedPlatform === "all" ? ['instagram', 'tiktok', 'youtube'] : [selectedPlatform];
    
    return platforms.map(platform => {
      const platformSentiments = sentimentData.filter((item: any) => item.platform === platform);
      const total = platformSentiments.length;
      
      if (total === 0) return { platform: platform.charAt(0).toUpperCase() + platform.slice(1), positive: 0, neutral: 0, negative: 0 };
      
      const positive = platformSentiments.filter((item: any) => item.sentiment === 'positive').length;
      const negative = platformSentiments.filter((item: any) => item.sentiment === 'negative').length;
      const neutral = platformSentiments.filter((item: any) => item.sentiment === 'neutral').length;
      
      const positivePercentage = Math.round((positive / total) * 100);
      const negativePercentage = Math.round((negative / total) * 100);
      const neutralPercentage = Math.round((neutral / total) * 100);

      // Ensure percentages sum to 100%
      const sum = positivePercentage + negativePercentage + neutralPercentage;
      let adjustedPositive = positivePercentage;
      let adjustedNeutral = neutralPercentage;
      let adjustedNegative = negativePercentage;

      if (sum !== 100) {
        // Distribute the remainder to maintain 100% total
        const remainder = 100 - sum;
        if (neutral > 0) {
          adjustedNeutral += remainder;
        } else if (positive > 0) {
          adjustedPositive += remainder;
        } else {
          adjustedNegative += remainder;
        }
      }

      return {
        platform: platform.charAt(0).toUpperCase() + platform.slice(1),
        positive: adjustedPositive,
        neutral: adjustedNeutral,
        negative: adjustedNegative
      };
    });
  };

  // Generate content type distribution from authentic database data (platform-specific)
  const getContentTypeData = () => {
    if (!contentData || contentData.length === 0) return [];

    // Filter content by selected platform if not "all"
    const filteredContent = selectedPlatform === "all" 
      ? contentData 
      : contentData.filter((post: any) => post.platform === selectedPlatform);

    const typeCount = filteredContent.reduce((acc: any, post: any) => {
      let type = 'mixed';
      
      // Use authentic data structure based on platform
      if (post.platform === 'instagram' && post.media_type) {
        // Instagram has media_type: Carousel, Sidecar, Image, Video
        switch(post.media_type.toLowerCase()) {
          case 'image': type = 'photos'; break;
          case 'video': type = 'videos'; break;
          case 'carousel': type = 'carousel'; break;
          case 'sidecar': type = 'sidecar'; break;
          default: type = 'mixed';
        }
      } else if (post.platform === 'youtube' && post.video_type) {
        // YouTube has video_type: video
        type = 'videos';
      } else if (post.platform === 'tiktok') {
        // TikTok is all videos based on the platform nature
        type = 'videos';
      }
      
      acc[type] = (acc[type] || 0) + 1;
      return acc;
    }, {});

    const total = Object.values(typeCount).reduce((sum: number, count: any) => sum + count, 0);
    
    if (total === 0) return [];
    
    // Map types to display names and colors
    const typeMapping: any = {
      'photos': { name: 'Photos', color: '#8B5CF6' },
      'videos': { name: 'Videos', color: '#EF4444' },
      'carousel': { name: 'Carousel', color: '#10B981' },
      'sidecar': { name: 'Sidecar', color: '#F59E0B' },
      'mixed': { name: 'Mixed', color: '#6B7280' }
    };

    return Object.entries(typeCount).map(([type, count]: [string, any]) => ({
      name: typeMapping[type]?.name || type,
      value: Math.round((count / total) * 100),
      color: typeMapping[type]?.color || '#6B7280'
    }));
  };

  // Calculate overall sentiment from authentic sentiment data
  const getOverallSentiment = () => {
    if (!sentimentData || sentimentData.length === 0) {
      return { positive: 0, neutral: 0, negative: 0 };
    }
    
    // Filter data by platform if not "all"
    let filteredData = sentimentData;
    if (selectedPlatform && selectedPlatform !== 'all') {
      filteredData = sentimentData.filter((item: any) => item.platform === selectedPlatform);
    }
    
    if (filteredData.length === 0) {
      return { positive: 0, neutral: 0, negative: 0 };
    }
    
    const total = filteredData.length;
    const positive = filteredData.filter((item: any) => item.sentiment === 'positive').length;
    const negative = filteredData.filter((item: any) => item.sentiment === 'negative').length;
    const neutral = filteredData.filter((item: any) => item.sentiment === 'neutral').length;
    
    return {
      positive: Math.round((positive / total) * 100),
      neutral: Math.round((neutral / total) * 100),
      negative: Math.round((negative / total) * 100)
    };
  };

  // Generate sentiment trends data from authentic sentiment analysis
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
        // For 7 days, show weekday names based on fixed reference date
        const weekdays = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
        formattedDates = Array.from({ length: 7 }, (_, index) => {
          const date = new Date(FIXED_REFERENCE_DATE);
          date.setDate(date.getDate() - (6 - index)); // Start 6 days back, go forward
          const dateStr = date.toISOString().split('T')[0];
          return { 
            dateStr,
            displayLabel: weekdays[date.getDay()]
          };
        });
        break;
        
      case '30days':
        // For 30 days, show every second day up to 30 based on fixed reference date
        formattedDates = Array.from({ length: 15 }, (_, index) => {
          const dayNumber = (index + 1) * 2; // Generate numbers 2, 4, ..., 30
          const date = new Date(FIXED_REFERENCE_DATE);
          date.setDate(date.getDate() - (29 - (index * 2))); // Start 29 days back, go forward by 2s
          const dateStr = date.toISOString().split('T')[0];
          return {
            dateStr,
            displayLabel: `Day ${dayNumber}`
          };
        });
        break;
        
      case '90days':
        // For 90 days, show all 12 weeks based on fixed reference date
        formattedDates = Array.from({ length: 12 }, (_, index) => {
          const weekNumber = index + 1; // Weeks 1 to 12
          const date = new Date(FIXED_REFERENCE_DATE);
          date.setDate(date.getDate() - (89 - (index * 7))); // Start 89 days back, go forward by weeks
          const dateStr = date.toISOString().split('T')[0];
          return {
            dateStr,
            displayLabel: `Week ${weekNumber}`
          };
        });
        break;
        
      case 'all':
        // For all time, show months numbered
        formattedDates = Array.from({ length: 6 }, (_, index) => {
          const monthNumber = index + 1; // Months 1 to 6
          const dateIndex = Math.min(index * 30, sortedDates.length - 1);
          return {
            dateStr: sortedDates[dateIndex],
            displayLabel: `Month ${monthNumber}`
          };
        });
        break;
        
      default:
        formattedDates = sortedDates.map((dateStr, index) => {
          const dayNumber = index + 1;
          return { 
            dateStr, 
            displayLabel: `Day ${dayNumber}`
          };
        });
    }
    
    // Calculate sentiment percentages for each date
    return formattedDates.map(({ dateStr, displayLabel }) => {
      let dayData = groupedByDate[dateStr] || [];
      
      // For 7-day view, if a date has no data, initialize with zeros
      if (selectedDateRange === '7days' && dayData.length === 0) {
        return { day: displayLabel, positive: 0, neutral: 0, negative: 0 };
      }
      
      // Filter by platform if not 'all'
      const filteredData = selectedPlatform !== 'all'
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

  const analyticsData = getAnalyticsData();
  const platformSentimentData = getPlatformSentimentData();
  const contentTypeData = getContentTypeData();
  const overallSentiment = getOverallSentiment();
  const sentimentTrends = generateTrendsData();

  // Transform authentic topics data for display
  const keyTopics = Array.isArray(topicsData) ? topicsData.map((topic: any) => ({
    topic: topic.topic,
    sentiment: Math.round(topic.sentiment_score),
    color: topic.sentiment_score >= 75 ? "bg-green-500" :  // Positive: >= 75
           topic.sentiment_score >= 25 && topic.sentiment_score < 75 ? "bg-yellow-500" : // Neutral: 25-74
           "bg-red-500" // Negative: < 25
  })) : [];

  const topicTabs = [
    { id: "all", label: "All Topics", count: keyTopics.length },
    { id: "positive", label: "Positive", count: keyTopics.filter(t => t.sentiment >= 75).length }, // Positive: >= 75
    { id: "neutral", label: "Neutral", count: keyTopics.filter(t => t.sentiment >= 25 && t.sentiment < 75).length }, // Neutral: 25-74
    { id: "negative", label: "Negative", count: keyTopics.filter(t => t.sentiment < 25).length }, // Negative: < 25
  ];

  if (!analyticsData) {
    return <div className="p-6">Loading analytics...</div>;
  }

  return (
    <div className="flex h-screen bg-gray-50 dark:bg-gray-900">
      <Sidebar />
      <div className="flex-1 flex flex-col overflow-hidden">
        <TopNav 
          selectedBrand={selectedBrand}
          onBrandChange={setSelectedBrand}
          selectedPlatform={selectedPlatform}
          onPlatformChange={setSelectedPlatform}
          selectedDateRange={selectedDateRange}
          onDateRangeChange={setSelectedDateRange}
        />
        <main className="flex-1 overflow-y-auto p-6 space-y-6">
          {/* Header */}
          <div className="flex justify-between items-start">
            <div>
              <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Analytics</h1>
              <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                Detailed metrics and analysis for your social media performance.
              </p>
            </div>
          </div>

          {/* Key Metrics Cards with Info Buttons */}
          <TooltipProvider>
            <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
              <Card>
                <CardContent className="p-4">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center space-x-2">
                      <MessageSquare className="h-5 w-5 text-orange-500" />
                      <span className="text-sm text-gray-500">mentions</span>
                    </div>
                    <Tooltip>
                      <TooltipTrigger>
                        <Info size={16} className="text-gray-400 hover:text-gray-600 cursor-help" />
                      </TooltipTrigger>
                      <TooltipContent className="max-w-xs">
                        <p className="text-sm">Count of all posts, comments, and references to your brand across platforms</p>
                      </TooltipContent>
                    </Tooltip>
                  </div>
                  <div className="text-2xl font-bold mt-1">{(analyticsData.mentions / 1000).toFixed(1)}K</div>
                </CardContent>
              </Card>

              <Card>
                <CardContent className="p-4">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center space-x-2">
                      <Eye className="h-5 w-5 text-blue-500" />
                      <span className="text-sm text-gray-500">reach</span>
                    </div>
                    <Tooltip>
                      <TooltipTrigger>
                        <Info size={16} className="text-gray-400 hover:text-gray-600 cursor-help" />
                      </TooltipTrigger>
                      <TooltipContent className="max-w-xs">
                        <p className="text-sm">Total unique users who saw your content across all platforms</p>
                      </TooltipContent>
                    </Tooltip>
                  </div>
                  <div className="text-2xl font-bold mt-1">{(analyticsData.reach / 1000).toFixed(1)}K</div>
                </CardContent>
              </Card>

              <Card>
                <CardContent className="p-4">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center space-x-2">
                      <ThumbsUp className="h-5 w-5 text-purple-500" />
                      <span className="text-sm text-gray-500">interactions</span>
                    </div>
                    <Tooltip>
                      <TooltipTrigger>
                        <Info size={16} className="text-gray-400 hover:text-gray-600 cursor-help" />
                      </TooltipTrigger>
                      <TooltipContent className="max-w-xs">
                        <p className="text-sm">Interactions = Σ(Likes + Comments + Shares + Saves + Clicks) for all content</p>
                      </TooltipContent>
                    </Tooltip>
                  </div>
                  <div className="text-2xl font-bold mt-1">{formatNumber(analyticsData.interactions)}</div>
                </CardContent>
              </Card>

              <Card>
                <CardContent className="p-4">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center space-x-2">
                      <ThumbsDown className="h-5 w-5 text-red-500" />
                      <span className="text-sm text-gray-500">negative mentions</span>
                    </div>
                    <Tooltip>
                      <TooltipTrigger>
                        <Info size={16} className="text-gray-400 hover:text-gray-600 cursor-help" />
                      </TooltipTrigger>
                      <TooltipContent className="max-w-xs">
                        <p className="text-sm">Count of posts and comments with negative sentiment about your brand</p>
                      </TooltipContent>
                    </Tooltip>
                  </div>
                  <div className="text-2xl font-bold mt-1">{analyticsData.negativeMentions}</div>
                </CardContent>
              </Card>

              <Card>
                <CardContent className="p-4">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center space-x-2">
                      <Smile className="h-5 w-5 text-green-500" />
                      <span className="text-sm text-gray-500">positive mentions</span>
                    </div>
                    <Tooltip>
                      <TooltipTrigger>
                        <Info size={16} className="text-gray-400 hover:text-gray-600 cursor-help" />
                      </TooltipTrigger>
                      <TooltipContent className="max-w-xs">
                        <p className="text-sm">Count of posts and comments with positive sentiment about your brand</p>
                      </TooltipContent>
                    </Tooltip>
                  </div>
                  <div className="text-2xl font-bold mt-1">{(analyticsData.positiveMentions / 1000).toFixed(1)}K</div>
                </CardContent>
              </Card>
            </div>
          </TooltipProvider>

          {/* Social Media Platform Analysis and Overall Sentiment */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Card>
              <CardHeader>
                <CardTitle>Social Media Platform Analysis</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="flex justify-center">
                  <div className="w-full h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart 
                        data={platformSentimentData} 
                        margin={{ top: 20, right: 30, left: 20, bottom: 5 }}
                        barCategoryGap="30%"
                      >
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="platform" />
                        <YAxis domain={[0, 100]} tickFormatter={(value) => `${value}%`} />
                        <RechartsTooltip />
                        <Bar dataKey="positive" stackId="a" fill="#10B981" maxBarSize={60} />
                        <Bar dataKey="neutral" stackId="a" fill="#F59E0B" maxBarSize={60} />
                        <Bar dataKey="negative" stackId="a" fill="#EF4444" maxBarSize={60} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Overall Sentiment</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-6">
                  {/* Positive */}
                  <div className="space-y-2">
                    <div className="flex items-center justify-between">
                      <span className="text-sm text-gray-600">Positive</span>
                      <span className="text-sm font-medium text-green-600">{getOverallSentiment().positive}%</span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div 
                        className="bg-green-500 h-2 rounded-full" 
                        style={{ width: `${getOverallSentiment().positive}%` }}
                      ></div>
                    </div>
                  </div>

                  {/* Neutral */}
                  <div className="space-y-2">
                    <div className="flex items-center justify-between">
                      <span className="text-sm text-gray-600">Neutral</span>
                      <span className="text-sm font-medium text-blue-600">{getOverallSentiment().neutral}%</span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div 
                        className="bg-blue-500 h-2 rounded-full" 
                        style={{ width: `${getOverallSentiment().neutral}%` }}
                      ></div>
                    </div>
                  </div>

                  {/* Negative */}
                  <div className="space-y-2">
                    <div className="flex items-center justify-between">
                      <span className="text-sm text-gray-600">Negative</span>
                      <span className="text-sm font-medium text-red-600">{getOverallSentiment().negative}%</span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div 
                        className="bg-red-500 h-2 rounded-full" 
                        style={{ width: `${getOverallSentiment().negative}%` }}
                      ></div>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Key Topics and Issues */}
          <Card>
            <CardHeader>
              <div>
                <CardTitle className="text-lg font-semibold text-navy mb-2">Key Topics and Issues</CardTitle>
                <p className="text-sm text-gray-600">Key subjects that are frequently discussed in customer feedback and interactions, categorized by sentiment.</p>
              </div>
            </CardHeader>
            <CardContent>
              {/* Topic Tabs */}
              <div className="flex space-x-6 mb-6 border-b">
                {topicTabs.map((tab) => (
                  <button
                    key={tab.id}
                    onClick={() => setActiveTopicTab(tab.id as any)}
                    className={`pb-3 border-b-2 transition-colors ${
                      activeTopicTab === tab.id
                        ? 'border-orange text-orange font-medium'
                        : 'border-transparent text-gray-500 hover:text-gray-700'
                    }`}
                  >
                    <span className="mr-2">{tab.label}</span>
                    <Badge 
                      variant="secondary" 
                      className={`${
                        tab.id === 'positive' ? 'bg-green-100 text-green-800' :
                        tab.id === 'negative' ? 'bg-red-100 text-red-800' :
                        tab.id === 'neutral' ? 'bg-blue-100 text-blue-800' :
                        'bg-gray-100 text-gray-700'
                      }`}
                    >
                      {tab.count}
                    </Badge>
                  </button>
                ))}
              </div>

              {/* Topics List */}
              <div className="space-y-4">
                {topicsLoading ? (
                  <div className="text-center py-4">
                    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-orange-500 mx-auto"></div>
                    <p className="text-gray-500 mt-2">Loading topics...</p>
                  </div>
                ) : topicsError ? (
                  <div className="text-center py-4">
                    <p className="text-red-500">Error loading topics data</p>
                  </div>
                ) : keyTopics.length === 0 ? (
                  <div className="text-center py-4">
                    <p className="text-gray-500">No topics found for this platform</p>
                  </div>
                ) : (
                  keyTopics
                    .filter((topic: any) => {
                      if (activeTopicTab === 'all') return true;
                      if (activeTopicTab === 'positive') return topic.sentiment >= 75; // Positive: >= 75
                      if (activeTopicTab === 'neutral') return topic.sentiment >= 25 && topic.sentiment < 75; // Neutral: 25-74
                      if (activeTopicTab === 'negative') return topic.sentiment < 25; // Negative: < 25
                      return true;
                    })
                    .map((topic: any, index: number) => (
                      <div key={index} className="flex items-center justify-between">
                        <div className="flex items-center space-x-4 flex-1">
                          <span className="text-sm font-medium text-gray-900 w-32">{topic.topic}</span>
                          <div className="flex-1 bg-gray-200 rounded-full h-2 mx-4">
                            <div
                              className={`${topic.color} h-2 rounded-full`}
                              style={{ width: `${topic.sentiment}%` }}
                            ></div>
                          </div>
                        </div>
                        <span className="text-sm font-medium text-green-600 w-12 text-right">
                          {topic.sentiment}%
                        </span>
                      </div>
                    ))
                )}
              </div>
            </CardContent>
          </Card>

          {/* Sentiment Trends */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg font-semibold text-navy">Sentiment Trends</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={sentimentTrends}>
                    <XAxis 
                      dataKey="day" 
                      axisLine={true}
                      tickLine={true}
                      className="text-sm text-gray-500"
                      stroke="#e2e8f0"
                      tick={{ fontSize: 12, fill: '#6b7280' }}
                    />
                    <YAxis 
                      domain={[0, 100]}
                      axisLine={true}
                      tickLine={true}
                      className="text-sm text-gray-500"
                      stroke="#e2e8f0"
                      tick={{ fontSize: 12, fill: '#6b7280' }}
                    />
                    <CartesianGrid 
                      strokeDasharray="3 3" 
                      stroke="#f1f5f9" 
                      vertical={false}
                    />
                    <RechartsTooltip 
                      contentStyle={{
                        backgroundColor: 'white',
                        border: '1px solid #e2e8f0',
                        borderRadius: '8px',
                        boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)'
                      }}
                    />
                    <Line 
                      type="monotone" 
                      dataKey="positive" 
                      stroke="#10b981"
                      strokeWidth={2}
                      dot={{ fill: "#10b981", strokeWidth: 0, r: 4 }}
                      name="Positive"
                    />
                    <Line 
                      type="monotone" 
                      dataKey="neutral" 
                      stroke="#3b82f6"
                      strokeWidth={2}
                      dot={{ fill: "#3b82f6", strokeWidth: 0, r: 4 }}
                      name="Neutral"
                    />
                    <Line 
                      type="monotone" 
                      dataKey="negative" 
                      stroke="#ef4444"
                      strokeWidth={2}
                      dot={{ fill: "#ef4444", strokeWidth: 0, r: 4 }}
                      name="Negative"
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>
        </main>
      </div>
    </div>
  );
}