import { useAuth } from "@/hooks/useAuth";
import { Avatar, AvatarImage, AvatarFallback } from "@/components/ui/avatar";
import { queryClient } from "@/lib/queryClient";
import { Link, useLocation } from "wouter";
import { 
  BarChart3, 
  Heart, 
  Users, 
  Settings, 
  User, 
  Download, 
  LogOut,
  TrendingUp
} from "lucide-react";

export function Sidebar() {
  const { user } = useAuth();
  const [location] = useLocation();

  const handleLogout = async () => {
    try {
      await fetch("/api/logout", { method: "POST" });
      // Clear all cached queries to reset auth state
      queryClient.clear();
      // Force a full page reload to clear all cached auth state
      window.location.replace("/auth");
    } catch (error) {
      console.error("Logout error:", error);
      queryClient.clear();
      window.location.replace("/auth");
    }
  };

  const navigation = [
    { name: "Dashboard", icon: TrendingUp, href: "/" },
    { name: "Analytics", icon: BarChart3, href: "/analytics" },
    { name: "Competitors", icon: Users, href: "/competitors" },
    { name: "Profile", icon: User, href: "/profile" },
    { name: "Settings", icon: Settings, href: "/settings" },
  ];

  return (
    <div className="w-60 bg-navy text-white flex flex-col h-screen">
      {/* Logo */}
      <div className="p-4">
        <h1 className="text-orange font-bold text-center text-[26px]">EXL</h1>
      </div>
      {/* Navigation */}
      <nav className="flex-1 px-4 overflow-y-auto">
        <ul className="space-y-1">
          {navigation.map((item) => (
            <li key={item.name}>
              <Link 
                href={item.href}
                className={`flex items-center px-3 py-2 rounded-lg transition-colors ${
                  location === item.href 
                    ? 'bg-orange text-white' 
                    : 'text-gray-300 hover:bg-gray-700'
                }`}
              >
                <item.icon size={16} className="mr-3" />
                {item.name}
              </Link>
            </li>
          ))}
        </ul>
      </nav>
      {/* User Profile */}
      <div className="p-3 border-t border-gray-600 mt-auto">
        <div className="flex items-center mb-4">
          <Avatar className="w-10 h-10 mr-3">
            <AvatarImage src={user?.profileImageUrl || ""} alt="Profile" />
            <AvatarFallback className="bg-orange text-white">
              {user?.firstName?.[0] || user?.email?.[0] || "U"}
            </AvatarFallback>
          </Avatar>
          <div className="flex-1 min-w-0">
            <p className="font-medium truncate">
              {user?.firstName && user?.lastName 
                ? `${user.firstName} ${user.lastName}` 
                : user?.email || "User"
              }
            </p>
            <p className="text-sm text-gray-400 truncate">{user?.email}</p>
          </div>
        </div>
        <ul className="space-y-1 text-sm">
          <li>
            <a href="#" className="flex items-center px-2 py-1 text-gray-300 hover:text-white transition-colors">
              <User size={16} className="mr-2" />
              Your Profile
            </a>
          </li>
          <li>
            <a href="#" className="flex items-center px-2 py-1 text-gray-300 hover:text-white transition-colors">
              <Download size={16} className="mr-2" />
              Download Project
            </a>
          </li>
          <li>
            <button 
              onClick={handleLogout}
              className="flex items-center px-2 py-1 text-gray-300 hover:text-white transition-colors w-full text-left"
            >
              <LogOut size={16} className="mr-2" />
              Sign out
            </button>
          </li>
        </ul>
      </div>
    </div>
  );
}
