import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { AreaChart, Area, XAxis, YAxis, ResponsiveContainer, Tooltip } from "recharts";
import { formatNumber } from "@/lib/utils";

interface EngagementChartProps {
  selectedBrand: string;
  selectedPlatform: string;
  selectedDateRange: string;
}

export function EngagementChart({ selectedBrand, selectedPlatform, selectedDateRange }: EngagementChartProps) {
  const [activeTab, setActiveTab] = useState<"likes" | "comments" | "shares">("likes");
  const [isDataAvailable, setIsDataAvailable] = useState(true);

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

  // Fetch real engagement data from the API
  const { data: engagementData, isLoading: isEngagementLoading } = useQuery({
    queryKey: ["/api/brands", brand?.id, "engagement-over-time", selectedPlatform, selectedDateRange],
    queryFn: async () => {
      if (!brand?.id) return [];
      const params = new URLSearchParams();
      if (selectedPlatform !== "all") params.set("platform", selectedPlatform);
      if (selectedDateRange) params.set("dateRange", selectedDateRange);
      
      console.log(`Fetching engagement data for brand ${brand.id}, platform: ${selectedPlatform}, dateRange: ${selectedDateRange}`);
      
      const response = await fetch(`/api/brands/${brand.id}/engagement-over-time?${params}`, {
        credentials: "include"
      });
      
      if (!response.ok) {
        console.error("Failed to fetch engagement data:", response.status);
        return [];
      }
      
      const data = await response.json();
      console.log("Engagement data received:", data);
      
      // Check if data has any non-zero values
      const hasEngagementData = data.some((item: any) => {
        return item.likes > 0 || item.comments > 0 || item.shares > 0;
      });
      
      setIsDataAvailable(hasEngagementData);
      return data;
    },
    enabled: !!brand?.id,
  });

  const chartData = engagementData || [];
  
  // Format the data to ensure consistent display
  const formattedChartData = chartData.map((item: any) => ({
    week: item.week,
    likes: parseInt(item.likes || 0, 10),
    comments: parseInt(item.comments || 0, 10),
    shares: parseInt(item.shares || 0, 10),
  }));

  const tabs = [
    { id: "likes", label: "Likes" },
    { id: "comments", label: "Comments" },
    { id: "shares", label: "Shares" },
  ];

  return (
    <Card className="bg-white shadow-sm border">
      <CardContent className="p-6">
        <div className="mb-6">
          <h3 className="font-semibold text-navy mb-1">Engagement Over Time</h3>
          {isEngagementLoading ? (
            <p className="text-sm text-gray-500 mt-2">Loading engagement data...</p>
          ) : !isDataAvailable ? (
            <p className="text-sm text-gray-500 mt-2">No engagement data available for the selected filters</p>
          ) : null}
          <div className="flex space-x-4 mt-4">
            {tabs.map((tab) => (
              <Button
                key={tab.id}
                variant={activeTab === tab.id ? "default" : "ghost"}
                size="sm"
                className={`px-3 py-1 text-sm font-medium ${
                  activeTab === tab.id
                    ? 'bg-orange text-white hover:bg-orange/90'
                    : 'text-gray-600 hover:bg-gray-100'
                }`}
                onClick={() => setActiveTab(tab.id as any)}
              >
                {tab.label}
              </Button>
            ))}
          </div>
        </div>
        <div className="h-48">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={formattedChartData}>
              <XAxis 
                dataKey="week" 
                axisLine={false}
                tickLine={false}
                className="text-sm text-gray-500"
              />
              <YAxis 
                axisLine={false}
                tickLine={false}
                className="text-sm text-gray-500"
                tickFormatter={formatNumber}
              />
              <Tooltip 
                contentStyle={{
                  backgroundColor: 'white',
                  border: '1px solid #e2e8f0',
                  borderRadius: '8px',
                  boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)'
                }}
              />
              <Area 
                type="monotone" 
                dataKey={activeTab} 
                stroke="#4A90E2" 
                fill="#4A90E2"
                fillOpacity={0.2}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
