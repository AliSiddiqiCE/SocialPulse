import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useState } from "react";

export function LoginForm() {
  const [isLogin, setIsLogin] = useState(true);

  const handleLogin = () => {
    window.location.href = "/api/login";
  };

  return (
    <>
      <h2 className="text-2xl font-semibold text-navy mb-2">
        Welcome to <span className="text-orange">EXL</span>
      </h2>
      <p className="text-gray-600 mb-8">Enter your credentials to access your account</p>

      <div className="flex mb-6">
        <button 
          className={`px-6 py-2 rounded-l-lg font-medium flex-1 transition-colors ${
            isLogin 
              ? 'bg-orange text-white' 
              : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
          }`}
          onClick={() => setIsLogin(true)}
        >
          Login
        </button>
        <button 
          className={`px-6 py-2 rounded-r-lg font-medium flex-1 transition-colors ${
            !isLogin 
              ? 'bg-orange text-white' 
              : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
          }`}
          onClick={() => setIsLogin(false)}
        >
          Register
        </button>
      </div>

      <form className="space-y-6">
        <div>
          <Label className="block text-sm font-medium text-gray-700 mb-2">Username</Label>
          <Input 
            type="text" 
            className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-orange focus:border-orange outline-none" 
            placeholder="Enter your username" 
          />
        </div>

        <div>
          <Label className="block text-sm font-medium text-gray-700 mb-2">Password</Label>
          <Input 
            type="password" 
            className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-orange focus:border-orange outline-none" 
            placeholder="••••••" 
          />
        </div>

        <Button 
          type="button"
          onClick={handleLogin}
          className="w-full bg-orange text-white py-3 rounded-lg font-medium hover:bg-orange-600 transition-colors"
        >
          Sign In
        </Button>
      </form>

      <p className="text-xs text-gray-500 mt-6">
        By continuing, you agree to our Terms of Service and Privacy Policy.
      </p>
    </>
  );
}
