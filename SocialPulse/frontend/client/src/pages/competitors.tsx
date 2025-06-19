import React, { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { Info } from "lucide-react";
import { Sidebar } from "@/components/sidebar";
import { TopNav } from "@/components/top-nav";

export default function Competitors() {
  // State for TopNav (even though we're comparing both brands)
  const [selectedBrand, setSelectedBrand] = useState("marks-spencer");
  const [selectedPlatform, setSelectedPlatform] = useState("all");
  const [selectedDateRange, setSelectedDateRange] = useState("30days");

  // Fetch both brands data for comparison
  const { data: marksSpencerBrand } = useQuery({
    queryKey: ["/api/brands", "marks-spencer"],
    queryFn: async () => {
      const response = await fetch("/api/brands/marks-spencer", {
        credentials: "include"
      });
      if (!response.ok) throw new Error("Failed to fetch Marks & Spencer");
      return response.json();
    },
  });

  const { data: nextRetailBrand } = useQuery({
    queryKey: ["/api/brands", "next-retail"],
    queryFn: async () => {
      const response = await fetch("/api/brands/next-retail", {
        credentials: "include"
      });
      if (!response.ok) throw new Error("Failed to fetch Next Retail");
      return response.json();
    },
  });

  // Fetch YouTube subscriber data (static, time-independent)
  const { data: youtubeSubscribers } = useQuery({
    queryKey: ["/api/youtube-subscribers"],
    queryFn: async () => {
      // Return actual subscriber counts from YouTube official datasets
      return {
        marksSpencer: 103000, // From marksAndSpencer_youtube_official
        nextRetail: 21400     // From nextretail_youtube_official
      };
    },
  });

  // Fetch metrics for both brands with filtering
  const { data: marksSpencerMetrics } = useQuery({
    queryKey: ["/api/brands", marksSpencerBrand?.id, "metrics", selectedPlatform, selectedDateRange],
    queryFn: async () => {
      if (!marksSpencerBrand?.id) return null;
      const params = new URLSearchParams();
      if (selectedPlatform !== "all") params.set("platform", selectedPlatform);
      if (selectedDateRange) params.set("dateRange", selectedDateRange);
      
      const response = await fetch(`/api/brands/${marksSpencerBrand.id}/metrics?${params}`, {
        credentials: "include"
      });
      if (!response.ok) return null;
      return response.json();
    },
    enabled: !!marksSpencerBrand?.id,
  });

  const { data: nextRetailMetrics } = useQuery({
    queryKey: ["/api/brands", nextRetailBrand?.id, "metrics", selectedPlatform, selectedDateRange],
    queryFn: async () => {
      if (!nextRetailBrand?.id) return null;
      const params = new URLSearchParams();
      if (selectedPlatform !== "all") params.set("platform", selectedPlatform);
      if (selectedDateRange) params.set("dateRange", selectedDateRange);
      
      const response = await fetch(`/api/brands/${nextRetailBrand.id}/metrics?${params}`, {
        credentials: "include"
      });
      if (!response.ok) return null;
      return response.json();
    },
    enabled: !!nextRetailBrand?.id,
  });

  // Fetch audience overlap data
  const { data: audienceOverlap } = useQuery({
    queryKey: ["/api/brands/audience-overlap", selectedPlatform, selectedDateRange],
    queryFn: async () => {
      const params = new URLSearchParams();
      params.set("brand1Id", "1");
      params.set("brand2Id", "2");
      if (selectedPlatform !== "all") params.set("platform", selectedPlatform);
      if (selectedDateRange) params.set("dateRange", selectedDateRange);
      
      const response = await fetch(`/api/brands/audience-overlap?${params}`, {
        credentials: "include"
      });
      if (!response.ok) throw new Error("Failed to fetch audience overlap");
      return response.json();
    },
  });

  // Fetch content strategy data for both brands
  const { data: marksContentStrategy } = useQuery({
    queryKey: ["/api/brands", 1, "content-strategy", selectedPlatform, selectedDateRange],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (selectedPlatform !== "all") params.set("platform", selectedPlatform);
      if (selectedDateRange !== "all") params.set("dateRange", selectedDateRange);
      
      const response = await fetch(`/api/brands/1/content-strategy?${params}`, {
        credentials: "include"
      });
      if (!response.ok) throw new Error("Failed to fetch Marks & Spencer content strategy");
      return response.json();
    },
  });

  const { data: nextContentStrategy } = useQuery({
    queryKey: ["/api/brands", 2, "content-strategy", selectedPlatform, selectedDateRange],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (selectedPlatform !== "all") params.set("platform", selectedPlatform);
      if (selectedDateRange !== "all") params.set("dateRange", selectedDateRange);
      
      const response = await fetch(`/api/brands/2/content-strategy?${params}`, {
        credentials: "include"
      });
      if (!response.ok) throw new Error("Failed to fetch Next Retail content strategy");
      return response.json();
    },
  });

  // Calculate metrics for comparison
  const marksData = marksSpencerMetrics?.[0] || {};
  const nextData = nextRetailMetrics?.[0] || {};

  // Determine followers based on platform filter
  const getFollowersForPlatform = () => {
    if (selectedPlatform === "youtube" || selectedPlatform === "all") {
      return {
        marks: youtubeSubscribers?.marksSpencer || 103000,
        next: youtubeSubscribers?.nextRetail || 21400
      };
    }
    return null; // Don't show followers for Instagram/TikTok
  };

  const followersData = getFollowersForPlatform();
  const marksFollowers = followersData?.marks || 0;
  const nextFollowers = followersData?.next || 0;
  const maxFollowers = Math.max(marksFollowers, nextFollowers);

  const marksEngagement = parseFloat(marksData.engagementScore?.replace('%', '') || '0');
  const nextEngagement = parseFloat(nextData.engagementScore?.replace('%', '') || '0');
  const maxEngagement = Math.max(marksEngagement, nextEngagement);

  // Use authentic content strategy data for posting frequency
  const marksPostFreq = marksContentStrategy?.postsPerDay || 0;
  const nextPostFreq = nextContentStrategy?.postsPerDay || 0;
  const maxPosts = Math.max(marksPostFreq, nextPostFreq);

  // Show followers section only for YouTube and All Platforms
  const showFollowers = selectedPlatform === "youtube" || selectedPlatform === "all";

  return (
    <div className="h-screen flex bg-light-gray">
      <Sidebar />
      <div className="flex-1 overflow-auto">
        <TopNav
          selectedBrand={selectedBrand}
          onBrandChange={setSelectedBrand}
          selectedPlatform={selectedPlatform}
          onPlatformChange={setSelectedPlatform}
          selectedDateRange={selectedDateRange}
          onDateRangeChange={setSelectedDateRange}
        />
        <div className="p-6 space-y-6 bg-white min-h-screen">
          {/* Header */}
          <div>
            <h1 className="text-2xl font-semibold text-gray-900">Competitor Analysis</h1>
            <p className="text-sm text-gray-600 mt-1">
              Compare your social media performance with competitors to identify opportunities and threats.
            </p>
          </div>

          <div className="grid lg:grid-cols-3 gap-6">
            {/* Performance Comparison */}
            <div className="lg:col-span-2">
              <Card>
                <CardHeader>
                  <CardTitle className="text-lg">Performance Comparison</CardTitle>
                  <CardDescription>Comparing Marks & Spencer against competitors</CardDescription>
                </CardHeader>
                <CardContent className="space-y-8">
                  {/* Followers Comparison - Only for YouTube and All Platforms */}
                  {showFollowers && (
                    <div>
                      <div className="flex items-center justify-between mb-3">
                        <span className="text-sm font-medium text-gray-700">
                          {selectedPlatform === "youtube" ? "Subscribers" : "Followers"}
                        </span>
                        <div className="flex items-center space-x-4 text-xs">
                          <div className="flex items-center">
                            <div className="w-3 h-3 bg-orange-500 rounded-full mr-1"></div>
                            <span>Marks & Spencer</span>
                          </div>
                          <div className="flex items-center">
                            <div className="w-3 h-3 bg-gray-800 rounded-full mr-1"></div>
                            <span>Next Retail</span>
                          </div>
                        </div>
                      </div>
                      <div className="space-y-2">
                        <div className="flex items-center justify-between">
                          <div className="w-full bg-gray-200 rounded-full h-6 mr-4">
                            <div 
                              className="bg-orange-500 h-6 rounded-full flex items-center justify-end pr-2"
                              style={{ width: `${Math.max((marksFollowers / maxFollowers) * 100, 15)}%` }}
                            >
                              <span className="text-white text-xs font-bold">
                                {marksFollowers >= 1000 ? `${(marksFollowers / 1000).toFixed(0)}K` : marksFollowers}
                              </span>
                            </div>
                          </div>
                        </div>
                        <div className="flex items-center justify-between">
                          <div className="w-full bg-gray-200 rounded-full h-6 mr-4">
                            <div 
                              className="bg-gray-800 h-6 rounded-full flex items-center justify-end pr-2"
                              style={{ width: `${(nextFollowers / maxFollowers) * 100}%` }}
                            >
                              <span className="text-white text-xs font-medium">
                                {nextFollowers >= 1000 ? `${(nextFollowers / 1000).toFixed(0)}K` : nextFollowers}
                              </span>
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  )}

                  {/* Engagement Rate Comparison */}
                  <div>
                    <div className="flex items-center justify-between mb-3">
                      <span className="text-sm font-medium text-gray-700">Engagement Rate (%)</span>
                      <div className="flex items-center space-x-4 text-xs">
                        <div className="flex items-center">
                          <div className="w-3 h-3 bg-orange-500 rounded-full mr-1"></div>
                          <span>Marks & Spencer</span>
                        </div>
                        <div className="flex items-center">
                          <div className="w-3 h-3 bg-gray-800 rounded-full mr-1"></div>
                          <span>Next Retail</span>
                        </div>
                      </div>
                    </div>
                    <div className="space-y-2">
                      <div className="flex items-center justify-between">
                        <div className="w-full bg-gray-200 rounded-full h-6 mr-4">
                          <div 
                            className="bg-orange-500 h-6 rounded-full flex items-center justify-end pr-2"
                            style={{ width: `${(marksEngagement / maxEngagement) * 100}%` }}
                          >
                            <span className="text-white text-xs font-medium">{marksEngagement}%</span>
                          </div>
                        </div>
                      </div>
                      <div className="flex items-center justify-between">
                        <div className="w-full bg-gray-200 rounded-full h-6 mr-4">
                          <div 
                            className="bg-gray-800 h-6 rounded-full flex items-center justify-end pr-2"
                            style={{ width: `${(nextEngagement / maxEngagement) * 100}%` }}
                          >
                            <span className="text-white text-xs font-medium">{nextEngagement}%</span>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* Post Frequency Comparison */}
                  <div>
                    <div className="flex items-center justify-between mb-3">
                      <span className="text-sm font-medium text-gray-700">Post Frequency (per day)</span>
                      <div className="flex items-center space-x-4 text-xs">
                        <div className="flex items-center">
                          <div className="w-3 h-3 bg-orange-500 rounded-full mr-1"></div>
                          <span>Marks & Spencer</span>
                        </div>
                        <div className="flex items-center">
                          <div className="w-3 h-3 bg-gray-800 rounded-full mr-1"></div>
                          <span>Next Retail</span>
                        </div>
                      </div>
                    </div>
                    <div className="space-y-2">
                      <div className="flex items-center justify-between">
                        <div className="w-full bg-gray-200 rounded-full h-6 mr-4">
                          <div 
                            className="bg-orange-500 h-6 rounded-full flex items-center justify-end pr-2"
                            style={{ width: `${(marksPostFreq / maxPosts) * 100}%` }}
                          >
                            <span className="text-white text-xs font-medium">
                              {marksPostFreq}/day
                            </span>
                          </div>
                        </div>
                      </div>
                      <div className="flex items-center justify-between">
                        <div className="w-full bg-gray-200 rounded-full h-6 mr-4">
                          <div 
                            className="bg-gray-800 h-6 rounded-full flex items-center justify-end pr-2"
                            style={{ width: `${(nextPostFreq / maxPosts) * 100}%` }}
                          >
                            <span className="text-white text-xs font-medium">
                              {nextPostFreq}/day
                            </span>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>

              {/* Content Strategy Comparison */}
              <Card className="mt-6">
                <CardHeader>
                  <CardTitle className="text-lg">Content Strategy Comparison</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div>
                      <div className="flex items-center justify-between mb-3">
                        <h4 className="text-sm font-medium text-gray-700">Post Frequency</h4>
                        <TooltipProvider>
                          <Tooltip>
                            <TooltipTrigger>
                              <Info size={16} className="text-gray-400 hover:text-gray-600 cursor-help" />
                            </TooltipTrigger>
                            <TooltipContent className="max-w-xs">
                              <p className="text-sm">Posts per day = Total posts ÷ Days active (Dec 1, 2024 - May 29, 2025)</p>
                            </TooltipContent>
                          </Tooltip>
                        </TooltipProvider>
                      </div>
                      <div className="space-y-3">
                        <div className="flex items-center justify-between">
                          <div className="flex items-center space-x-2">
                            <span className="text-orange-500 font-medium text-sm">MARKS & SPENCER</span>
                          </div>
                          <span className="text-sm text-gray-600">{marksPostFreq}/day</span>
                          <div className="w-64 bg-gray-200 rounded-full h-3">
                            <div 
                              className="bg-orange-500 h-3 rounded-full"
                              style={{ width: `${(marksPostFreq / maxPosts) * 100}%` }}
                            ></div>
                          </div>
                        </div>
                        <div className="flex items-center justify-between">
                          <div className="flex items-center space-x-2">
                            <span className="text-gray-800 font-medium text-sm">NEXT RETAIL</span>
                          </div>
                          <span className="text-sm text-gray-600">{nextPostFreq}/day</span>
                          <div className="w-64 bg-gray-200 rounded-full h-3">
                            <div 
                              className="bg-gray-800 h-3 rounded-full"
                              style={{ width: `${(nextPostFreq / maxPosts) * 100}%` }}
                            ></div>
                          </div>
                        </div>
                      </div>
                    </div>

                    <div>
                      <div className="flex items-center justify-between mb-3">
                        <h4 className="text-sm font-medium text-gray-700">Content Type Distribution (Marks & Spencer)</h4>
                        <TooltipProvider>
                          <Tooltip>
                            <TooltipTrigger>
                              <Info size={16} className="text-gray-400 hover:text-gray-600 cursor-help" />
                            </TooltipTrigger>
                            <TooltipContent className="max-w-xs">
                              <p className="text-sm">Content breakdown by platform: Instagram posts vs TikTok videos from official accounts</p>
                            </TooltipContent>
                          </Tooltip>
                        </TooltipProvider>
                      </div>
                      {marksContentStrategy?.contentTypes ? (
                        <div className="grid grid-cols-2 gap-4 text-center">
                          {marksContentStrategy.contentTypes.map((type: any) => (
                            <div key={type.type}>
                              <span className="text-sm text-gray-600 capitalize">{type.type}s</span>
                              <p className="text-2xl font-bold text-gray-900">{type.percentage}%</p>
                              <p className="text-xs text-gray-500">({type.count} posts)</p>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="text-sm text-gray-500">Loading content types...</div>
                      )}
                      

                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Platform Metrics */}
            <div>
              <Card>
                <CardHeader>
                  <CardTitle className="text-lg">Platform Metrics</CardTitle>
                </CardHeader>
                <CardContent className="space-y-6">
                  {/* Marks & Spencer Metrics */}
                  <div>
                    <h4 className="font-medium text-gray-900 mb-3">Marks & Spencer</h4>
                    <div className="space-y-3">
                      {/* Show Followers/Subscribers only for YouTube and All Platforms */}
                      {showFollowers && (
                        <div className="flex justify-between items-center">
                          <span className="text-sm text-gray-600">
                            {selectedPlatform === "youtube" ? "Subscribers" : "Followers"}
                          </span>
                          <span className="font-medium">
                            {marksFollowers >= 1000 ? `${(marksFollowers / 1000).toFixed(0)}K` : marksFollowers}
                          </span>
                        </div>
                      )}
                      <div className="flex justify-between items-center">
                        <span className="text-sm text-gray-600">Engagement Rate</span>
                        <span className="font-medium">{marksEngagement}%</span>
                      </div>
                      <div className="flex justify-between items-center">
                        <span className="text-sm text-gray-600">Post Frequency</span>
                        <span className="font-medium">{marksPostFreq}/day</span>
                      </div>
                    </div>
                  </div>

                  {/* Next Retail Metrics */}
                  <div>
                    <h4 className="font-medium text-gray-900 mb-3">Next Retail</h4>
                    <div className="space-y-3">
                      {/* Show Followers/Subscribers only for YouTube and All Platforms */}
                      {showFollowers && (
                        <div className="flex justify-between items-center">
                          <span className="text-sm text-gray-600">
                            {selectedPlatform === "youtube" ? "Subscribers" : "Followers"}
                          </span>
                          <span className="font-medium">
                            {nextFollowers >= 1000 ? `${(nextFollowers / 1000).toFixed(0)}K` : nextFollowers}
                          </span>
                        </div>
                      )}
                      <div className="flex justify-between items-center">
                        <span className="text-sm text-gray-600">Engagement Rate</span>
                        <span className="font-medium">{nextEngagement}%</span>
                      </div>
                      <div className="flex justify-between items-center">
                        <span className="text-sm text-gray-600">Post Frequency</span>
                        <span className="font-medium">{nextPostFreq}/day</span>
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>

              {/* Audience Overlap */}
              <Card className="mt-6">
                <CardHeader>
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-lg">Audience Overlap</CardTitle>
                    <TooltipProvider>
                      <Tooltip>
                        <TooltipTrigger>
                          <Info size={16} className="text-gray-400 hover:text-gray-600 cursor-help" />
                        </TooltipTrigger>
                        <TooltipContent className="max-w-xs">
                          <p className="text-sm">Common hashtags used ÷ Total unique hashtags across both brands</p>
                        </TooltipContent>
                      </Tooltip>
                    </TooltipProvider>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-medium">Marks & Spencer - Next Retail</span>
                    <span className="font-bold">
                      {audienceOverlap ? `${audienceOverlap.overlapPercentage}% Overlap` : 'Loading...'}
                    </span>
                  </div>
                  <Progress 
                    value={audienceOverlap?.overlapPercentage || 0} 
                    className="h-3" 
                  />
                  
                  {audienceOverlap && (
                    <div className="text-xs text-gray-600">
                      <strong>Common hashtags:</strong> {audienceOverlap.commonHashtags.join(', ') || 'None'}
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}