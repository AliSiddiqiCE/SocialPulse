import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Avatar, AvatarImage, AvatarFallback } from "@/components/ui/avatar";
import { TopNav } from "@/components/top-nav";
import { Sidebar } from "@/components/sidebar";
import { useAuth } from "@/hooks/useAuth";
import { 
  User, 
  Mail, 
  Calendar,
  MapPin,
  Building,
  Phone,
  Globe,
  Camera,
  Edit3
} from "lucide-react";

export default function Profile() {
  const { user } = useAuth();
  const [selectedPlatform, setSelectedPlatform] = useState("all");
  const [selectedDateRange, setSelectedDateRange] = useState("30days");
  const [isEditing, setIsEditing] = useState(false);

  // Profile data from authenticated user
  const [profileData, setProfileData] = useState({
    fullName: user ? `${user.firstName} ${user.lastName}` : "",
    email: user?.email || "",
    phone: user?.phone || "",
    company: user?.company || "",
    department: user?.department || "",
    role: user?.role || "",
    location: user?.location || "",
    website: user?.website || "",
    bio: user?.bio || "",
    joinDate: user?.createdAt ? new Date(user.createdAt).toLocaleDateString('en-US', { month: 'long', year: 'numeric' }) : "",
  });

  // Update profile data when user data changes
  useEffect(() => {
    if (user) {
      setProfileData({
        fullName: `${user.firstName} ${user.lastName}`,
        email: user.email,
        phone: user.phone || "",
        company: user.company || "",
        department: user.department || "",
        role: user.role || "",
        location: user.location || "",
        website: user.website || "",
        bio: user.bio || "",
        joinDate: user.createdAt ? new Date(user.createdAt).toLocaleDateString('en-US', { month: 'long', year: 'numeric' }) : "",
      });
    }
  }, [user]);

  const handleSave = () => {
    // Handle save logic here
    setIsEditing(false);
    console.log("Profile saved:", profileData);
  };

  const handleCancel = () => {
    setIsEditing(false);
    // Reset changes if needed
  };

  return (
    <div className="flex h-screen bg-gray-50 dark:bg-gray-900">
      <Sidebar />
      <div className="flex-1 flex flex-col overflow-hidden">
        <TopNav 
          selectedBrand="marks-spencer"
          onBrandChange={() => {}}
          selectedPlatform={selectedPlatform}
          onPlatformChange={setSelectedPlatform}
          selectedDateRange={selectedDateRange}
          onDateRangeChange={setSelectedDateRange}
        />
        <main className="flex-1 overflow-y-auto p-6 space-y-6">
          {/* Header */}
          <div className="flex justify-between items-start">
            <div>
              <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Profile</h1>
              <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                Manage your personal information and preferences.
              </p>
            </div>
            <Button 
              onClick={() => setIsEditing(!isEditing)}
              variant={isEditing ? "outline" : "default"}
            >
              <Edit3 className="h-4 w-4 mr-2" />
              {isEditing ? "Cancel" : "Edit Profile"}
            </Button>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            {/* Profile Card */}
            <Card className="lg:col-span-1">
              <CardContent className="p-6">
                <div className="flex flex-col items-center space-y-4">
                  <div className="relative">
                    <Avatar className="h-32 w-32">
                      <AvatarImage src="/api/placeholder/128/128" alt="Profile" />
                      <AvatarFallback className="text-2xl">
                        {profileData.fullName.split(' ').map(n => n[0]).join('')}
                      </AvatarFallback>
                    </Avatar>
                    {isEditing && (
                      <Button 
                        size="sm" 
                        className="absolute bottom-0 right-0 rounded-full h-8 w-8 p-0"
                      >
                        <Camera className="h-4 w-4" />
                      </Button>
                    )}
                  </div>
                  
                  <div className="text-center space-y-2">
                    <h2 className="text-xl font-semibold">{profileData.fullName}</h2>
                    <p className="text-gray-500">{profileData.role}</p>
                    <Badge variant="secondary">{profileData.department}</Badge>
                  </div>

                  <div className="w-full space-y-3 text-sm">
                    <div className="flex items-center space-x-2">
                      <Mail className="h-4 w-4 text-gray-400" />
                      <span className="truncate">{profileData.email}</span>
                    </div>
                    <div className="flex items-center space-x-2">
                      <Building className="h-4 w-4 text-gray-400" />
                      <span>{profileData.company}</span>
                    </div>
                    <div className="flex items-center space-x-2">
                      <MapPin className="h-4 w-4 text-gray-400" />
                      <span>{profileData.location}</span>
                    </div>
                    <div className="flex items-center space-x-2">
                      <Calendar className="h-4 w-4 text-gray-400" />
                      <span>Joined {profileData.joinDate}</span>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Profile Details */}
            <Card className="lg:col-span-2">
              <CardHeader>
                <CardTitle className="flex items-center space-x-2">
                  <User className="h-5 w-5" />
                  <span>Personal Information</span>
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="fullName">Full Name</Label>
                    <Input 
                      id="fullName"
                      value={profileData.fullName}
                      onChange={(e) => setProfileData({...profileData, fullName: e.target.value})}
                      disabled={!isEditing}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="email">Email Address</Label>
                    <Input 
                      id="email"
                      type="email"
                      value={profileData.email}
                      onChange={(e) => setProfileData({...profileData, email: e.target.value})}
                      disabled={!isEditing}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="phone">Phone Number</Label>
                    <Input 
                      id="phone"
                      value={profileData.phone}
                      onChange={(e) => setProfileData({...profileData, phone: e.target.value})}
                      disabled={!isEditing}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="company">Company</Label>
                    <Input 
                      id="company"
                      value={profileData.company}
                      onChange={(e) => setProfileData({...profileData, company: e.target.value})}
                      disabled={!isEditing}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="department">Department</Label>
                    <Input 
                      id="department"
                      value={profileData.department}
                      onChange={(e) => setProfileData({...profileData, department: e.target.value})}
                      disabled={!isEditing}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="role">Job Title</Label>
                    <Input 
                      id="role"
                      value={profileData.role}
                      onChange={(e) => setProfileData({...profileData, role: e.target.value})}
                      disabled={!isEditing}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="location">Location</Label>
                    <Input 
                      id="location"
                      value={profileData.location}
                      onChange={(e) => setProfileData({...profileData, location: e.target.value})}
                      disabled={!isEditing}
                    />
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="website">Website</Label>
                    <Input 
                      id="website"
                      value={profileData.website}
                      onChange={(e) => setProfileData({...profileData, website: e.target.value})}
                      disabled={!isEditing}
                    />
                  </div>
                </div>

                <div className="space-y-2">
                  <Label htmlFor="bio">Bio</Label>
                  <textarea 
                    id="bio"
                    className="w-full p-3 border border-gray-300 rounded-md resize-none"
                    rows={4}
                    value={profileData.bio}
                    onChange={(e) => setProfileData({...profileData, bio: e.target.value})}
                    disabled={!isEditing}
                  />
                </div>

                {isEditing && (
                  <div className="flex space-x-3 pt-4">
                    <Button onClick={handleSave} className="flex-1">
                      Save Changes
                    </Button>
                    <Button onClick={handleCancel} variant="outline" className="flex-1">
                      Cancel
                    </Button>
                  </div>
                )}
              </CardContent>
            </Card>
          </div>

          {/* Activity Statistics */}
          <Card>
            <CardHeader>
              <CardTitle>Account Activity</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div className="text-center p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
                  <div className="text-2xl font-bold text-blue-600">247</div>
                  <div className="text-sm text-gray-500">Reports Generated</div>
                </div>
                <div className="text-center p-4 bg-green-50 dark:bg-green-900/20 rounded-lg">
                  <div className="text-2xl font-bold text-green-600">18</div>
                  <div className="text-sm text-gray-500">Dashboards Created</div>
                </div>
                <div className="text-center p-4 bg-orange-50 dark:bg-orange-900/20 rounded-lg">
                  <div className="text-2xl font-bold text-orange-600">3.2K</div>
                  <div className="text-sm text-gray-500">Data Points Analyzed</div>
                </div>
                <div className="text-center p-4 bg-purple-50 dark:bg-purple-900/20 rounded-lg">
                  <div className="text-2xl font-bold text-purple-600">156</div>
                  <div className="text-sm text-gray-500">Insights Shared</div>
                </div>
              </div>
            </CardContent>
          </Card>
        </main>
      </div>
    </div>
  );
}