import React, { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { AtSign } from "lucide-react";
import { LineChart, Line, XAxis, YAxis, ResponsiveContainer, Tooltip, Legend, CartesianGrid } from "recharts";
import { formatNumber } from "@/lib/utils";

// Fixed reference date for all date range calculations
const FIXED_REFERENCE_DATE = new Date('2025-05-29T23:59:59.999Z');

interface MentionsChartProps {
  selectedBrand: string;
  selectedPlatform: string;
  selectedDateRange: string;
}

export function MentionsChart({ selectedBrand, selectedPlatform, selectedDateRange }: MentionsChartProps) {
  const [viewType, setViewType] = useState<"mentions" | "reach" | "sentiment">("mentions");
  
  // Use main platform dropdown to determine if we should compare all platforms
  const compareAllPlatforms = selectedPlatform === "all";

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

  // Generate realistic time-series data for mentions, reach, and sentiment
  const generateChartData = () => {
    if (!metricsData || metricsData.length === 0) return [];
    
    const getDataPoints = () => {
      switch(selectedDateRange) {
        case '7days':
        case '7d': 
          // Generate 7 days backwards from fixed reference date
          const weekdays = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
          const sevenDayLabels = Array.from({ length: 7 }, (_, index) => {
            const date = new Date(FIXED_REFERENCE_DATE);
            date.setDate(date.getDate() - (6 - index)); // Start 6 days back, go forward
            return weekdays[date.getDay()];
          });
          return { count: 7, labels: sevenDayLabels };
        case '30days':
        case '30d': 
          // Generate 30 days backwards from fixed reference date
          const thirtyDayLabels = Array.from({ length: 15 }, (_, index) => {
            const dayNumber = (index + 1) * 2; // Generate numbers 2, 4, ..., 30
            return `Day ${dayNumber}`;
          });
          return { 
            count: 15, 
            labels: thirtyDayLabels
          };
        case '90days':
        case '90d': 
          // Generate 90 days backwards from fixed reference date
          const ninetyDayLabels = Array.from({ length: 12 }, (_, index) => {
            const weekNumber = index + 1; // Weeks 1 to 12
            return `Week ${weekNumber}`;
          });
          return { 
            count: 12, 
            labels: ninetyDayLabels
          };
        case 'all': 
          return { 
            count: 6, 
            labels: Array.from({ length: 6 }, (_, i) => `Month ${i + 1}`)
          };
        default: 
          return { 
            count: 15, 
            labels: Array.from({ length: 15 }, (_, i) => `Day ${i + 1}`)
          };
      }
    };
    
    const dataPoints = getDataPoints();
    const data = [];
    
    if (compareAllPlatforms) {
      const platforms = ['instagram', 'tiktok', 'youtube'];
      
      const platformBaseMetrics = platforms.map(platform => {
        const platformMetrics = metricsData.filter((m: any) => m.platform === platform);
        return {
          platform,
          mentions: platformMetrics.length > 0 ? platformMetrics[0]?.mentions || 100 : 100,
          reach: platformMetrics.length > 0 ? platformMetrics[0]?.reach || 1000 : 1000,
          sentiment: platformMetrics.length > 0 ? platformMetrics[0]?.sentiment || 75 : 75
        };
      });
      
      for (let i = 0; i < dataPoints.count; i++) {
        const dataPoint: any = { date: dataPoints.labels[i] };
        
        platformBaseMetrics.forEach(({ platform, mentions, reach, sentiment }) => {
          // Adjust time phase based on date range
          const timePhase = selectedDateRange === 'all' 
            ? (i / dataPoints.count) * Math.PI * 2  // Slower variation for monthly data
            : (i / dataPoints.count) * Math.PI * 4; // Faster variation for daily/weekly data
          const platformOffset = platform.length * 0.7;
          
          const mentionVariation = 1 + 
            (Math.sin(timePhase + platformOffset) * 0.4) + 
            (Math.sin(timePhase * 2.5 + platformOffset) * 0.25) + 
            (Math.cos(timePhase * 1.3 + platformOffset) * 0.2) +
            (Math.random() * 0.2 - 0.1);
          
          const reachVariation = 1 + 
            (Math.cos(timePhase + platformOffset) * 0.45) + 
            (Math.sin(timePhase * 1.8 + platformOffset) * 0.3) + 
            (Math.cos(timePhase * 3.2 + platformOffset) * 0.15) +
            (Math.random() * 0.15 - 0.075);
          
          const sentimentVariation = sentiment + 
            (Math.sin(timePhase + platformOffset) * 15) + 
            (Math.cos(timePhase * 2 + platformOffset) * 8) +
            (Math.random() * 10 - 5);
          
          // YouTube mentions should always be zero
          dataPoint[`${platform}_mentions`] = platform === 'youtube' ? 0 : Math.max(1, Math.floor(mentions * mentionVariation / (selectedDateRange === 'all' ? 10 : 20)));
          dataPoint[`${platform}_reach`] = Math.max(1, Math.floor(reach * reachVariation / (selectedDateRange === 'all' ? 15 : 25)));
          dataPoint[`${platform}_sentiment`] = Math.max(0, Math.min(100, Math.round(sentimentVariation)));
        });
        
        data.push(dataPoint);
      }
    } else {
      const metric = metricsData[0];
      const baseMentions = metric.mentions || 0;
      const baseReach = metric.reach || 0;
      const baseSentiment = metric.sentiment || 75;
      
      for (let i = 0; i < dataPoints.count; i++) {
        // Adjust time phase based on date range
        const timePhase = selectedDateRange === 'all'
          ? (i / dataPoints.count) * Math.PI * 2  // Slower variation for monthly data
          : (i / dataPoints.count) * Math.PI * 3; // Faster variation for daily/weekly data
        
        const mentionVariation = 1 + 
          (Math.sin(timePhase) * 0.35) + 
          (Math.sin(timePhase * 2.2) * 0.2) + 
          (Math.cos(timePhase * 1.5) * 0.15) +
          (Math.random() * 0.15 - 0.075);
        
        const reachVariation = 1 + 
          (Math.cos(timePhase) * 0.4) + 
          (Math.sin(timePhase * 1.7) * 0.25) + 
          (Math.cos(timePhase * 2.8) * 0.15) +
          (Math.random() * 0.1 - 0.05);
        
        const sentimentVariation = baseSentiment + 
          (Math.sin(timePhase) * 12) + 
          (Math.cos(timePhase * 1.5) * 6) +
          (Math.random() * 8 - 4);
        
        data.push({
          date: dataPoints.labels[i],
          mentions: selectedPlatform === 'youtube' ? 0 : Math.floor(baseMentions * mentionVariation / (selectedDateRange === 'all' ? 10 : 20)),
          reach: Math.floor(baseReach * reachVariation / (selectedDateRange === 'all' ? 15 : 25)),
          sentiment: Math.max(0, Math.min(100, Math.round(sentimentVariation))),
        });
      }
    }
    
    return data;
  };

  const chartData = generateChartData();
  
  // Debug logging to see chart data structure
  console.log('Chart data for compareAllPlatforms:', compareAllPlatforms, chartData);

  return (
    <Card className="bg-white shadow-sm border mb-8">
      <CardContent className="p-6">
        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center space-x-4">
            <AtSign className="text-gray-600" size={20} />
            <span className="font-semibold text-navy">Mentions & Reach</span>
            <div className="flex bg-gray-100 rounded-lg p-1">
              <Button 
                variant={viewType === "mentions" ? "default" : "ghost"}
                size="sm"
                className={`px-3 py-1 text-sm font-medium ${
                  viewType === "mentions" 
                    ? 'bg-orange text-white hover:bg-orange/90' 
                    : 'text-gray-600 hover:bg-gray-200'
                }`}
                onClick={() => setViewType("mentions")}
              >
                Mentions
              </Button>
              <Button 
                variant={viewType === "reach" ? "default" : "ghost"}
                size="sm"
                className={`px-3 py-1 text-sm font-medium ${
                  viewType === "reach" 
                    ? 'bg-orange text-white hover:bg-orange/90' 
                    : 'text-gray-600 hover:bg-gray-200'
                }`}
                onClick={() => setViewType("reach")}
              >
                Reach
              </Button>
              <Button 
                variant={viewType === "sentiment" ? "default" : "ghost"}
                size="sm"
                className={`px-3 py-1 text-sm font-medium ${
                  viewType === "sentiment" 
                    ? 'bg-orange text-white hover:bg-orange/90' 
                    : 'text-gray-600 hover:bg-gray-200'
                }`}
                onClick={() => setViewType("sentiment")}
              >
                Sentiment
              </Button>
            </div>
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
                tickFormatter={viewType === "sentiment" ? (value) => `${value}%` : formatNumber}
                domain={viewType === "sentiment" ? [0, 100] : undefined}
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
                formatter={viewType === "sentiment" ? (value: any) => [`${value}%`, ''] : undefined}
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
                    dataKey={`instagram_${viewType}`}
                    stroke="#E1306C" 
                    strokeWidth={2}
                    dot={{ fill: "#E1306C", strokeWidth: 0, r: 4 }}
                    name="Instagram"
                  />
                  <Line 
                    type="monotone" 
                    dataKey={`tiktok_${viewType}`}
                    stroke="#000000" 
                    strokeWidth={2}
                    dot={{ fill: "#000000", strokeWidth: 0, r: 4 }}
                    name="TikTok"
                  />
                  <Line 
                    type="monotone" 
                    dataKey={`youtube_${viewType}`}
                    stroke="#FF0000" 
                    strokeWidth={2}
                    dot={{ fill: "#FF0000", strokeWidth: 0, r: 4 }}
                    name="YouTube"
                  />
                </>
              ) : (
                <Line 
                  type="monotone" 
                  dataKey={viewType} 
                  stroke={
                    viewType === "mentions" ? "hsl(var(--orange))" :
                    viewType === "reach" ? "#4A90E2" :
                    "#22C55E"
                  }
                  strokeWidth={2}
                  fill={
                    viewType === "mentions" ? "hsl(var(--orange))" :
                    viewType === "reach" ? "#4A90E2" :
                    "#22C55E"
                  }
                  fillOpacity={0.1}
                  dot={{ 
                    fill: viewType === "mentions" ? "hsl(var(--orange))" :
                          viewType === "reach" ? "#4A90E2" :
                          "#22C55E", 
                    strokeWidth: 0, 
                    r: 4 
                  }}
                  name={viewType.charAt(0).toUpperCase() + viewType.slice(1)}
                />
              )}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
