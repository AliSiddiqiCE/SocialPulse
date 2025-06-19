import { LoginForm } from "@/components/login-form";

export default function Landing() {
  return (
    <div className="min-h-screen flex">
      {/* Left Panel */}
      <div className="w-1/2 bg-navy text-white p-12 flex flex-col justify-center">
        <div className="max-w-md">
          <h1 className="text-4xl font-bold text-orange mb-2">EXL</h1>
          <h2 className="text-2xl font-semibold mb-6">Social Media Analytics Platform</h2>
          <p className="text-lg mb-8 text-gray-300">
            Track and analyze your social media performance across platforms. Get insights on engagement, 
            audience demographics, and competitor analysis.
          </p>
          
          <div className="space-y-4">
            <div className="flex items-center">
              <div className="w-2 h-2 bg-orange rounded-full mr-3"></div>
              <span>Comprehensive social media dashboards</span>
            </div>
            <div className="flex items-center">
              <div className="w-2 h-2 bg-orange rounded-full mr-3"></div>
              <span>In-depth competitor analysis</span>
            </div>
            <div className="flex items-center">
              <div className="w-2 h-2 bg-orange rounded-full mr-3"></div>
              <span>Real-time data visualization</span>
            </div>
            <div className="flex items-center">
              <div className="w-2 h-2 bg-orange rounded-full mr-3"></div>
              <span>Sentiment analysis and trend detection</span>
            </div>
          </div>
        </div>
      </div>

      {/* Right Panel */}
      <div className="w-1/2 bg-white flex flex-col justify-center p-12">
        <div className="max-w-md mx-auto w-full">
          <LoginForm />
        </div>
      </div>
    </div>
  );
}
