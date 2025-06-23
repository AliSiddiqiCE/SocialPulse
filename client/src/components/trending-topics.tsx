import { useQuery } from "@tanstack/react-query";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { Info } from "lucide-react";

interface TrendingTopicsProps {
  selectedBrand: string;
  selectedPlatform: string;
  selectedDateRange: string;
}

export function TrendingTopics({ selectedBrand, selectedPlatform, selectedDateRange }: TrendingTopicsProps) {
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

  // Fetch brand hashtags
  const { data: brandHashtags } = useQuery({
    queryKey: ["/api/brands", brand?.id, "hashtags", selectedPlatform, selectedDateRange],
    queryFn: async () => {
      if (!brand?.id) return [];
      const params = new URLSearchParams({ limit: "10" });
      if (selectedPlatform !== "all") params.set("platform", selectedPlatform);
      if (selectedDateRange) params.set("dateRange", selectedDateRange);
      
      const response = await fetch(`/api/brands/${brand.id}/hashtags?${params}`, {
        credentials: "include"
      });
      if (!response.ok) return [];
      return response.json();
    },
    enabled: !!brand?.id,
  });

  // Fetch industry hashtags
  const { data: industryHashtags } = useQuery({
    queryKey: ["/api/hashtags/industry"],
    queryFn: async () => {
      const response = await fetch("/api/hashtags/industry?limit=10", {
        credentials: "include"
      });
      if (!response.ok) return [];
      return response.json();
    },
  });

  // Fetch competitor hashtags (using the other brand's hashtags)
  const { data: competitorHashtags } = useQuery({
    queryKey: ["/api/brands", brand?.id === 1 ? 2 : 1, "hashtags", selectedPlatform, selectedDateRange],
    queryFn: async () => {
      const competitorId = brand?.id === 1 ? 2 : 1;
      const params = new URLSearchParams({ limit: "10" });
      if (selectedPlatform !== "all") params.set("platform", selectedPlatform);
      if (selectedDateRange) params.set("dateRange", selectedDateRange);
      
      const response = await fetch(`/api/brands/${competitorId}/hashtags?${params}`, {
        credentials: "include"
      });
      if (!response.ok) return [];
      return response.json();
    },
    enabled: !!brand?.id,
  });

  return (
    <Card className="bg-white shadow-sm border">
      <CardContent className="p-6">
        <div className="flex items-center justify-between mb-6">
          <h3 className="font-semibold text-navy">Trending Topics & Hashtags</h3>
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger>
                <Info size={16} className="text-gray-400 hover:text-gray-600 cursor-help" />
              </TooltipTrigger>
              <TooltipContent className="max-w-xs">
                <p className="text-sm">Top hashtags categorized by brand, industry, and competitor usage</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
        
        <div className="grid grid-cols-2 gap-8">
          <div>
            {/* For Your Brand */}
            <div className="mb-6">
            <h4 className="text-sm font-medium text-gray-700 mb-4">For Your Brand</h4>
              <div className="flex flex-wrap gap-2">
                {brandHashtags?.map((hashtag: any) => (
                  <Badge 
                    key={hashtag.hashtag}
                    variant="secondary"
                    className="bg-orange-100 text-orange-600"
                  >
                    {hashtag.hashtag}
                    <span className="ml-1 text-xs text-orange-500">
                      ({hashtag.usageCount})
                </span>
                  </Badge>
              ))}
              </div>
            </div>
            
            {/* Industry Trends */}
            <div>
            <h4 className="text-sm font-medium text-gray-700 mb-4">Industry Trends</h4>
            <div className="flex flex-wrap gap-2">
                {industryHashtags?.map((hashtag: any) => (
                  <Badge 
                    key={hashtag.hashtag}
                    variant="secondary"
                    className="bg-blue-100 text-blue-600"
                  >
                    {hashtag.hashtag}
                    <span className="ml-1 text-xs text-blue-500">
                      ({hashtag.usageCount})
                </span>
                  </Badge>
              ))}
              </div>
            </div>
          </div>
          
          {/* Competitor Hashtags */}
          <div>
            <h4 className="text-sm font-medium text-gray-700 mb-4">Competitor Hashtags</h4>
            <div className="flex flex-wrap gap-2">
              {competitorHashtags?.map((hashtag: any) => (
                <Badge 
                  key={hashtag.hashtag}
                  variant="secondary"
                  className="bg-gray-100 text-gray-600"
                >
                  {hashtag.hashtag}
                  <span className="ml-1 text-xs text-gray-500">
                    ({hashtag.usageCount})
                  </span>
                </Badge>
              ))}
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
