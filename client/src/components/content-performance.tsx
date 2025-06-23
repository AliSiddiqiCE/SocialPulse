import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { ExternalLink, Shirt, Gift, Heart } from "lucide-react";

interface ContentPerformanceProps {
  selectedBrand: string;
  selectedPlatform: string;
  selectedDateRange: string;
}

export function ContentPerformance({ selectedBrand, selectedPlatform, selectedDateRange }: ContentPerformanceProps) {
  const [sortBy, setSortBy] = useState("engagement");
  const [currentPage, setCurrentPage] = useState(0);
  const itemsPerPage = 3;
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

  const { data: allContent } = useQuery({
    queryKey: ["/api/brands", brand?.id, "content", selectedPlatform, selectedDateRange],
    queryFn: async () => {
      if (!brand?.id) return [];
      const params = new URLSearchParams({ limit: "50" }); // Fetch more items for proper pagination
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

  // Sort and paginate authentic content from database
  const sortedContent = (allContent || []).sort((a: any, b: any) => {
    switch (sortBy) {
      case "engagement":
        const aEngagement = (a.likes || 0) + (a.shares || 0) + (a.comments || 0);
        const bEngagement = (b.likes || 0) + (b.shares || 0) + (b.comments || 0);
        return bEngagement - aEngagement;
      case "reach":
        return (b.views || 0) - (a.views || 0);
      case "date":
        return new Date(b.publishedAt).getTime() - new Date(a.publishedAt).getTime();
      default:
        return 0;
    }
  });

  const totalItems = sortedContent.length;
  const totalPages = Math.ceil(totalItems / itemsPerPage);
  const startIndex = currentPage * itemsPerPage;
  const displayContent = sortedContent.slice(startIndex, startIndex + itemsPerPage);

  const handleSortChange = (value: string) => {
    setSortBy(value);
    setCurrentPage(0); // Reset to first page when sorting changes
  };

  const handlePrevious = () => {
    if (currentPage > 0) {
      setCurrentPage(currentPage - 1);
    }
  };

  const handleNext = () => {
    if (currentPage < totalPages - 1) {
      setCurrentPage(currentPage + 1);
    }
  };

  const formatDate = (dateString: string) => {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', { 
      month: 'short', 
      day: 'numeric', 
      year: 'numeric' 
    });
  };

  const formatEngagement = (item: any) => {
    // Calculate total engagement from likes, shares, comments
    const totalEngagement = (item.likes || 0) + (item.shares || 0) + (item.comments || 0);
    
    // If we have engagement data, show it
    if (totalEngagement > 0) {
      if (totalEngagement >= 1000) {
        return `${(totalEngagement / 1000).toFixed(1)}K engagements`;
      }
      return `${totalEngagement} engagements`;
    }
    
    // Fallback to views if no engagement data
    const views = item.views || 0;
    if (views >= 1000) {
      return `${(views / 1000).toFixed(1)}K views`;
    }
    return views > 0 ? `${views} views` : 'No data';
  };

  return (
    <Card className="bg-white shadow-sm border mb-8">
      <CardContent className="p-6">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h3 className="font-semibold text-navy mb-1">Top Performing Content</h3>
          </div>
          <Select value={sortBy} onValueChange={handleSortChange}>
            <SelectTrigger className="bg-gray-100 border-none text-sm w-48">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="engagement">Sort by engagement</SelectItem>
              <SelectItem value="reach">Sort by reach</SelectItem>
              <SelectItem value="date">Sort by date</SelectItem>
            </SelectContent>
          </Select>
        </div>

        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow className="border-b border-gray-200">
                <TableHead className="text-left py-3 px-4 font-medium text-gray-700">Content</TableHead>
                <TableHead className="text-left py-3 px-4 font-medium text-gray-700">Source</TableHead>
                <TableHead className="text-left py-3 px-4 font-medium text-gray-700">Date</TableHead>
                <TableHead className="text-left py-3 px-4 font-medium text-gray-700">Engagement</TableHead>
                <TableHead className="text-left py-3 px-4 font-medium text-gray-700">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {displayContent.map((item: any) => {
                const IconComponent = item.icon || Shirt;
                return (
                  <TableRow key={item.id} className="border-b border-gray-100">
                    <TableCell className="py-4 px-4">
                      <div className="flex items-center">
                        <div className="w-8 h-8 bg-gray-100 rounded flex items-center justify-center mr-3">
                          <IconComponent size={14} className="text-gray-600" />
                        </div>
                        <div>
                          <p className="font-medium text-sm">{item.content}</p>
                          <p className="text-xs text-gray-500">
                            {item.likes} likes, {item.shares} reposts
                          </p>
                        </div>
                      </div>
                    </TableCell>
                    <TableCell className="py-4 px-4 text-sm text-gray-600">
                      {item.platform}
                    </TableCell>
                    <TableCell className="py-4 px-4 text-sm text-gray-600">
                      {formatDate(item.publishedAt)}
                    </TableCell>
                    <TableCell className="py-4 px-4">
                      <div className="text-sm font-medium">
                        {formatEngagement(item)}
                      </div>
                    </TableCell>
                    <TableCell className="py-4 px-4">
                      <ExternalLink size={16} className="text-orange cursor-pointer hover:text-orange/80" />
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </div>

        <div className="flex items-center justify-between mt-6">
          <span className="text-sm text-gray-600">
            Showing {startIndex + 1}-{Math.min(startIndex + itemsPerPage, totalItems)} of {totalItems} posts
          </span>
          <div className="flex space-x-2">
            <Button
              variant="ghost"
              size="sm"
              onClick={handlePrevious}
              disabled={currentPage === 0}
              className="px-3 py-1 text-gray-600 hover:text-gray-800 text-sm disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Previous
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={handleNext}
              disabled={currentPage >= totalPages - 1}
              className="px-3 py-1 text-gray-600 hover:text-gray-800 text-sm disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Next
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
