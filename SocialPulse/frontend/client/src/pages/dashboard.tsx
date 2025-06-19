import { useState } from "react";
import { Sidebar } from "@/components/sidebar";
import { TopNav } from "@/components/top-nav";
import { OverviewCards } from "@/components/overview-cards";
import { MentionsChart } from "@/components/mentions-chart";
import { EngagementChart } from "@/components/engagement-chart";

import { ContentPerformance } from "@/components/content-performance";
import { TrendingTopics } from "@/components/trending-topics";

export default function Dashboard() {
  const [selectedBrand, setSelectedBrand] = useState("marks-spencer");
  const [selectedPlatform, setSelectedPlatform] = useState("all");
  const [selectedDateRange, setSelectedDateRange] = useState("30days");

  // Debug brand changes
  const handleBrandChange = (brand: string) => {
    console.log(`Dashboard - Brand changed from ${selectedBrand} to ${brand}`);
    setSelectedBrand(brand);
  };

  return (
    <div className="h-screen flex bg-light-gray">
      <Sidebar />
      
      <div className="flex-1 flex flex-col h-screen">
        <TopNav 
          selectedBrand={selectedBrand} 
          onBrandChange={handleBrandChange}
          selectedPlatform={selectedPlatform}
          onPlatformChange={setSelectedPlatform}
          selectedDateRange={selectedDateRange}
          onDateRangeChange={setSelectedDateRange}
        />
        
        <main className="flex-1 overflow-y-auto bg-light-gray">
          <div className="p-6">
            <div className="mb-6">
              <h1 className="text-2xl font-semibold text-navy mb-2">Social Media Dashboard</h1>
              <p className="text-gray-600">Track and analyze your social media performance and competitor insights.</p>
            </div>

            <OverviewCards 
              selectedBrand={selectedBrand} 
              selectedPlatform={selectedPlatform}
              selectedDateRange={selectedDateRange}
            />
            
            <MentionsChart 
              selectedBrand={selectedBrand} 
              selectedPlatform={selectedPlatform}
              selectedDateRange={selectedDateRange}
            />
            
            <EngagementChart 
              selectedBrand={selectedBrand} 
              selectedPlatform={selectedPlatform}
              selectedDateRange={selectedDateRange}
            />
            
            <ContentPerformance 
              selectedBrand={selectedBrand} 
              selectedPlatform={selectedPlatform}
              selectedDateRange={selectedDateRange}
            />
            
            <TrendingTopics 
              selectedBrand={selectedBrand} 
              selectedPlatform={selectedPlatform}
              selectedDateRange={selectedDateRange}
            />
          </div>
        </main>
      </div>
    </div>
  );
}
