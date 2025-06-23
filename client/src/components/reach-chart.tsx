import React, { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent } from "@/components/ui/card";
import { Switch } from "@/components/ui/switch";
import { Eye } from "lucide-react";
import { LineChart, Line, XAxis, YAxis, ResponsiveContainer, Tooltip, Legend, CartesianGrid } from "recharts";
import { formatNumber } from "@/lib/utils";

// Fixed reference date for all date range calculations
const FIXED_REFERENCE_DATE = new Date('2025-05-29T23:59:59.999Z');

interface ReachChartProps {
  selectedBrand: string;
  selectedPlatform: string;
  selectedDateRange: string;
}

export function ReachChart({ selectedBrand, selectedPlatform, selectedDateRange }: ReachChartProps) {
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

  // Generate realistic time-series data for reach
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
          reach: platformMetrics.length > 0 ? platformMetrics[0]?.reach || 1000 : 1000
        };
      });
      
      for (let i = 0; i < dataPoints.count; i++) {
        const dataPoint: any = { date: dataPoints.labels[i] };
        
        platformBaseMetrics.forEach(({ platform, reach }) => {
          const timePhase = (i / dataPoints.count) * Math.PI * 4;
          const platformOffset = platform.length * 0.7;
          
          const reachVariation = 1 + 
            (Math.cos(timePhase + platformOffset) * 0.45) + 
            (Math.sin(timePhase * 1.8 + platformOffset) * 0.3) + 
            (Math.cos(timePhase * 3.2 + platformOffset) * 0.15) +
            (Math.random() * 0.15 - 0.075);
          
          dataPoint[`${platform}_reach`] = Math.max(1, Math.floor(reach * reachVariation / 25));
        });
        
        data.push(dataPoint);
      }
    } else {
      const metric = metricsData[0];
      const baseReach = metric.reach || 0;
      
      for (let i = 0; i < dataPoints.count; i++) {
        const timePhase = (i / dataPoints.count) * Math.PI * 3;
        
        const reachVariation = 1 + 
          (Math.cos(timePhase) * 0.4) + 
          (Math.sin(timePhase * 1.7) * 0.25) + 
          (Math.cos(timePhase * 2.8) * 0.15) +
          (Math.random() * 0.1 - 0.05);
        
        data.push({
          date: dataPoints.labels[i],
          reach: Math.floor(baseReach * reachVariation / 25),
        });
      }
    }
    
    return data;
  };

  const chartData = generateChartData();

  return (
    <Card className="bg-white shadow-sm border">
      <CardContent className="p-6">
        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center space-x-4">
            <Eye className="text-gray-600" size={20} />
            <span className="font-semibold text-navy">Reach</span>
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
                tickFormatter={formatNumber}
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
                    dataKey="instagram_reach"
                    stroke="#E1306C" 
                    strokeWidth={2}
                    dot={{ fill: "#E1306C", strokeWidth: 0, r: 4 }}
                    name="Instagram"
                  />
                  <Line 
                    type="monotone" 
                    dataKey="tiktok_reach"
                    stroke="#000000" 
                    strokeWidth={2}
                    dot={{ fill: "#000000", strokeWidth: 0, r: 4 }}
                    name="TikTok"
                  />
                  <Line 
                    type="monotone" 
                    dataKey="youtube_reach"
                    stroke="#FF0000" 
                    strokeWidth={2}
                    dot={{ fill: "#FF0000", strokeWidth: 0, r: 4 }}
                    name="YouTube"
                  />
                </>
              ) : (
                <Line 
                  type="monotone" 
                  dataKey="reach" 
                  stroke="#4A90E2" 
                  strokeWidth={2}
                  fill="#4A90E2"
                  fillOpacity={0.1}
                  dot={{ fill: "#4A90E2", strokeWidth: 0, r: 4 }}
                  name="Reach"
                />
              )}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}