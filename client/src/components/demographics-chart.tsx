import { useQuery } from "@tanstack/react-query";
import { Card, CardContent } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, ResponsiveContainer, PieChart, Pie, Cell, Tooltip } from "recharts";

interface DemographicsChartProps {
  selectedBrand: string;
  selectedPlatform: string;
  selectedDateRange: string;
}

export function DemographicsChart({ selectedBrand, selectedPlatform, selectedDateRange }: DemographicsChartProps) {
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

  const { data: demographics } = useQuery({
    queryKey: ["/api/brands", brand?.id, "demographics", selectedPlatform, selectedDateRange],
    queryFn: async () => {
      if (!brand?.id) return [];
      const params = new URLSearchParams();
      if (selectedPlatform !== "all") params.set("platform", selectedPlatform);
      if (selectedDateRange) params.set("dateRange", selectedDateRange);
      
      const response = await fetch(`/api/brands/${brand.id}/demographics?${params}`, {
        credentials: "include"
      });
      if (!response.ok) return [];
      return response.json();
    },
    enabled: !!brand?.id,
  });

  // Process authentic demographics data from database
  const processedDemographics = demographics || [];
  
  // Debug log to see the actual data structure
  console.log('Demographics data:', processedDemographics);
  
  // Generate age data from authentic database records
  const ageData = [
    { age: "18-24", value: processedDemographics.filter((d: any) => d.ageGroup === "18-24").reduce((sum: number, d: any) => sum + parseFloat(d.percentage || 0), 0) },
    { age: "25-34", value: processedDemographics.filter((d: any) => d.ageGroup === "25-34").reduce((sum: number, d: any) => sum + parseFloat(d.percentage || 0), 0) },
    { age: "35-44", value: processedDemographics.filter((d: any) => d.ageGroup === "35-44").reduce((sum: number, d: any) => sum + parseFloat(d.percentage || 0), 0) },
    { age: "45-54", value: processedDemographics.filter((d: any) => d.ageGroup === "45-54").reduce((sum: number, d: any) => sum + parseFloat(d.percentage || 0), 0) },
    { age: "55+", value: processedDemographics.filter((d: any) => d.ageGroup === "55+").reduce((sum: number, d: any) => sum + parseFloat(d.percentage || 0), 0) },
  ];

  // Generate gender data from authentic database records
  const femalePercentage = processedDemographics.filter((d: any) => d.gender === "female").reduce((sum: number, d: any) => sum + parseFloat(d.percentage || 0), 0);
  const malePercentage = processedDemographics.filter((d: any) => d.gender === "male").reduce((sum: number, d: any) => sum + parseFloat(d.percentage || 0), 0);
  
  const genderData = [
    { name: "Female", value: femalePercentage, color: "#E91E63" },
    { name: "Male", value: malePercentage, color: "#4A90E2" },
  ];

  return (
    <Card className="bg-white shadow-sm border">
      <CardContent className="p-6">
        <div className="mb-6">
          <h3 className="font-semibold text-navy mb-1">Audience Demographics</h3>
          <p className="text-sm text-gray-600">Based on data from all social platforms</p>
        </div>
        <div className="grid grid-cols-2 gap-4">
          <div>
            <h4 className="text-sm font-medium text-gray-700 mb-3">Age Distribution</h4>
            <div className="h-48">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={ageData} margin={{ top: 10, right: 10, left: 10, bottom: 10 }}>
                  <XAxis 
                    dataKey="age" 
                    axisLine={false}
                    tickLine={false}
                    className="text-xs text-gray-500"
                  />
                  <YAxis hide />
                  <Tooltip 
                    contentStyle={{
                      backgroundColor: 'white',
                      border: '1px solid #e2e8f0',
                      borderRadius: '8px',
                      boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)'
                    }}
                  />
                  <Bar 
                    dataKey="value" 
                    fill="hsl(var(--navy))"
                    radius={[3, 3, 0, 0]}
                  />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
          <div>
            <h4 className="text-sm font-medium text-gray-700 mb-3">Gender Distribution</h4>
            <div className="h-48 flex items-center justify-center">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={genderData}
                    cx="50%"
                    cy="50%"
                    innerRadius={40}
                    outerRadius={70}
                    dataKey="value"
                  >
                    {genderData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip 
                    contentStyle={{
                      backgroundColor: 'white',
                      border: '1px solid #e2e8f0',
                      borderRadius: '8px',
                      boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)'
                    }}
                  />
                </PieChart>
              </ResponsiveContainer>
            </div>
            <div className="flex justify-center mt-2 space-x-4">
              {genderData.map((item) => (
                <div key={item.name} className="flex items-center text-xs">
                  <div 
                    className="w-3 h-3 rounded-full mr-1" 
                    style={{ backgroundColor: item.color }}
                  ></div>
                  <span>{item.name}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
