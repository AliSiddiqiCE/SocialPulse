import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { ChevronLeft, Plus, Bell } from "lucide-react";
import { MonitoringPipelineModal } from "@/components/monitoring-pipeline-modal";

interface TopNavProps {
  selectedBrand: string;
  onBrandChange: (brand: string) => void;
  selectedPlatform: string;
  onPlatformChange: (platform: string) => void;
  selectedDateRange: string;
  onDateRangeChange: (range: string) => void;
}

export function TopNav({ selectedBrand, onBrandChange, selectedPlatform, onPlatformChange, selectedDateRange, onDateRangeChange }: TopNavProps) {
  const [isMonitoringModalOpen, setIsMonitoringModalOpen] = useState(false);
  
  const brandNames = {
    "marks-spencer": "Marks and Spencer",
    "next-retail": "Next Retail"
  };

  const platformNames = {
    "all": "All Platforms",
    "instagram": "Instagram",
    "tiktok": "TikTok", 
    "youtube": "YouTube"
  };

  return (
    <header className="border-b border-gray-200 px-6 py-4 bg-[#22262a]">
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <ChevronLeft className="text-gray-400" size={20} />
          <div className="flex items-center space-x-2">
            <span className="text-orange font-semibold">EXL</span>
            <span className="text-gray-400">|</span>
            <span className="text-sm text-gray-600">Monitoring:</span>
            <Select value={selectedBrand} onValueChange={onBrandChange}>
              <SelectTrigger className="bg-gray-100 border-none text-sm font-medium w-48">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="marks-spencer">Marks and Spencer</SelectItem>
                <SelectItem value="next-retail">Next Retail</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
        <div className="flex items-center space-x-4">
          <Button 
            onClick={() => setIsMonitoringModalOpen(true)}
            className="bg-navy text-white px-4 py-2 text-sm font-medium hover:bg-navy/90"
          >
            <Plus size={16} className="mr-2" />
            New Monitoring
          </Button>
          <Select value={selectedPlatform} onValueChange={onPlatformChange}>
            <SelectTrigger className="bg-gray-100 border-none text-sm w-36">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Platforms</SelectItem>
              <SelectItem value="instagram">Instagram</SelectItem>
              <SelectItem value="tiktok">TikTok</SelectItem>
              <SelectItem value="youtube">YouTube</SelectItem>
            </SelectContent>
          </Select>
          <Select value={selectedDateRange} onValueChange={onDateRangeChange}>
            <SelectTrigger className="bg-gray-100 border-none text-sm w-32">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All dates</SelectItem>
              <SelectItem value="7days">Last 7 days</SelectItem>
              <SelectItem value="30days">Last 30 days</SelectItem>
              <SelectItem value="90days">Last 90 days</SelectItem>
            </SelectContent>
          </Select>
          <Bell className="text-gray-400" size={20} />
        </div>
      </div>
      
      <MonitoringPipelineModal
        isOpen={isMonitoringModalOpen}
        onClose={() => setIsMonitoringModalOpen(false)}
        onComplete={() => {
          setIsMonitoringModalOpen(false);
          // Optionally refresh the page or update brand list
          window.location.reload();
        }}
      />
    </header>
  );
}
