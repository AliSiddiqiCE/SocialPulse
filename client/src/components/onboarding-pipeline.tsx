import { useState } from "react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useLocation } from "wouter";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Badge } from "@/components/ui/badge";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog";
import { useToast } from "@/hooks/use-toast";
import { apiRequest } from "@/lib/queryClient";
import { X, Plus, Check } from "lucide-react";

const pipelineSchema = z.object({
  brandName: z.string().min(1, "Brand name is required"),
  hashtags: z.array(z.string()),
  competitors: z.array(z.string()),
  platforms: z.array(z.string()).min(1, "Select at least one platform"),
});

type PipelineData = z.infer<typeof pipelineSchema>;

interface OnboardingPipelineProps {
  onComplete: () => void;
}

export function OnboardingPipeline({ onComplete }: OnboardingPipelineProps) {
  const [currentStep, setCurrentStep] = useState(1);
  const [showConfirmation, setShowConfirmation] = useState(false);
  const [, setLocation] = useLocation();
  const { toast } = useToast();

  const form = useForm<PipelineData>({
    resolver: zodResolver(pipelineSchema),
    defaultValues: {
      brandName: "",
      hashtags: [],
      competitors: [],
      platforms: [],
    },
  });

  const [customHashtag, setCustomHashtag] = useState("");
  const [competitorInput, setCompetitorInput] = useState("");

  // Fetch suggested hashtags based on brand name
  const { data: suggestedHashtags } = useQuery({
    queryKey: ["/api/hashtags/suggestions", form.watch("brandName")],
    queryFn: async () => {
      const brandName = form.getValues("brandName");
      if (!brandName.trim()) return [];
      
      const response = await fetch(`/api/hashtags/suggestions?brand=${encodeURIComponent(brandName)}`, {
        credentials: "include"
      });
      if (!response.ok) return [];
      return response.json();
    },
    enabled: !!form.watch("brandName"),
  });

  const savePipelineData = useMutation({
    mutationFn: async (data: PipelineData) => {
      const res = await apiRequest("POST", "/api/user/onboarding", data);
      return res.json();
    },
    onSuccess: () => {
      toast({
        title: "Setup Complete!",
        description: "Your monitoring pipeline has been configured successfully.",
      });
      onComplete();
      setLocation("/");
    },
    onError: (error: Error) => {
      toast({
        title: "Setup Failed",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const steps = [
    { number: 1, title: "Brand", active: currentStep === 1 },
    { number: 2, title: "Hashtags", active: currentStep === 2 },
    { number: 3, title: "Competitors", active: currentStep === 3 },
    { number: 4, title: "Platforms", active: currentStep === 4 },
    { number: 5, title: "Confirm", active: currentStep === 5 },
  ];

  const addHashtag = (hashtag: string) => {
    const currentHashtags = form.getValues("hashtags");
    if (!currentHashtags.includes(hashtag)) {
      form.setValue("hashtags", [...currentHashtags, hashtag]);
    }
  };

  const removeHashtag = (hashtag: string) => {
    const currentHashtags = form.getValues("hashtags");
    form.setValue("hashtags", currentHashtags.filter(h => h !== hashtag));
  };

  const addCustomHashtag = () => {
    if (customHashtag.trim()) {
      const hashtag = customHashtag.startsWith("#") ? customHashtag : `#${customHashtag}`;
      addHashtag(hashtag);
      setCustomHashtag("");
    }
  };

  const parseCompetitors = () => {
    const competitors = competitorInput
      .split(",")
      .map(c => c.trim())
      .filter(c => c.length > 0);
    form.setValue("competitors", competitors);
    setCompetitorInput("");
  };

  const togglePlatform = (platform: string) => {
    const currentPlatforms = form.getValues("platforms");
    if (currentPlatforms.includes(platform)) {
      form.setValue("platforms", currentPlatforms.filter(p => p !== platform));
    } else {
      form.setValue("platforms", [...currentPlatforms, platform]);
    }
  };

  const nextStep = () => {
    if (currentStep < 5) {
      setCurrentStep(currentStep + 1);
    } else {
      setShowConfirmation(true);
    }
  };

  const prevStep = () => {
    if (currentStep > 1) {
      setCurrentStep(currentStep - 1);
    }
  };

  const handleConfirm = () => {
    const data = form.getValues();
    savePipelineData.mutate(data);
  };

  const renderStepContent = () => {
    switch (currentStep) {
      case 1:
        return (
          <div className="space-y-6">
            <div className="text-center mb-8">
              <h2 className="text-2xl font-bold text-gray-900 mb-2">
                What brand would you like to monitor?
              </h2>
              <p className="text-gray-600">
                Enter your main brand name. We'll use this to generate relevant
                keywords and hashtags to track across social media platforms.
              </p>
            </div>
            
            <div className="space-y-4">
              <Label htmlFor="brandName" className="text-base font-medium">
                Brand Name
              </Label>
              <Input
                id="brandName"
                placeholder="e.g., Myntra"
                className="text-lg p-4"
                {...form.register("brandName")}
              />
              <p className="text-sm text-gray-500">
                Examples: Nike, Starbucks, Apple Inc., Tesla
              </p>
              {form.formState.errors.brandName && (
                <p className="text-sm text-red-600">
                  {form.formState.errors.brandName.message}
                </p>
              )}
            </div>
          </div>
        );

      case 2:
        return (
          <div className="space-y-6">
            <div className="text-center mb-8">
              <h2 className="text-2xl font-bold text-gray-900 mb-2">
                Select your hashtags and keywords
              </h2>
              <p className="text-gray-600">
                We've suggested some hashtags based on your brand. You can edit, add, or remove hashtags before continuing.
              </p>
            </div>

            <div className="space-y-6">
              {/* Suggested Hashtags */}
              {suggestedHashtags && suggestedHashtags.length > 0 && (
                <div>
                  <Label className="text-base font-medium mb-3 block">
                    Suggested Hashtags
                  </Label>
                  <div className="flex flex-wrap gap-2">
                    {suggestedHashtags.map((hashtag: any) => (
                      <Button
                        key={hashtag.tag}
                        variant={form.watch("hashtags").includes(hashtag.tag) ? "default" : "outline"}
                        size="sm"
                        onClick={() => {
                          if (form.watch("hashtags").includes(hashtag.tag)) {
                            removeHashtag(hashtag.tag);
                          } else {
                            addHashtag(hashtag.tag);
                          }
                        }}
                      >
                        {hashtag.tag}
                        {form.watch("hashtags").includes(hashtag.tag) && (
                          <Check className="ml-1 h-3 w-3" />
                        )}
                      </Button>
                    ))}
                  </div>
                </div>
              )}

              {/* Selected Hashtags */}
              <div>
                <Label className="text-base font-medium mb-3 block">
                  Selected Hashtags ({form.watch("hashtags").length})
                </Label>
                <div className="flex flex-wrap gap-2 min-h-[40px] p-3 border rounded-lg bg-gray-50">
                  {form.watch("hashtags").map((hashtag) => (
                    <Badge key={hashtag} variant="secondary" className="bg-orange-100 text-orange-600">
                      {hashtag}
                      <X
                        className="ml-1 h-3 w-3 cursor-pointer"
                        onClick={() => removeHashtag(hashtag)}
                      />
                    </Badge>
                  ))}
                  {form.watch("hashtags").length === 0 && (
                    <span className="text-gray-500 text-sm">No hashtags selected</span>
                  )}
                </div>
              </div>

              {/* Add Custom Hashtag */}
              <div>
                <Label className="text-base font-medium mb-3 block">
                  Add Custom Hashtag
                </Label>
                <div className="flex gap-2">
                  <Input
                    placeholder="Enter custom hashtag"
                    value={customHashtag}
                    onChange={(e) => setCustomHashtag(e.target.value)}
                    onKeyPress={(e) => e.key === "Enter" && addCustomHashtag()}
                  />
                  <Button onClick={addCustomHashtag} variant="outline">
                    <Plus className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            </div>
          </div>
        );

      case 3:
        return (
          <div className="space-y-6">
            <div className="text-center mb-8">
              <h2 className="text-2xl font-bold text-gray-900 mb-2">
                Who are your competitors?
              </h2>
              <p className="text-gray-600">
                Enter competitor brand names (comma-separated) to track their social media performance alongside yours.
              </p>
            </div>

            <div className="space-y-4">
              <Label className="text-base font-medium">
                Competitor Brand Names
              </Label>
              <Input
                placeholder="e.g., Nike, Adidas, Puma"
                value={competitorInput}
                onChange={(e) => setCompetitorInput(e.target.value)}
                onBlur={parseCompetitors}
              />
              <p className="text-sm text-gray-500">
                Separate multiple competitors with commas
              </p>

              {/* Selected Competitors */}
              {form.watch("competitors").length > 0 && (
                <div>
                  <Label className="text-base font-medium mb-3 block">
                    Selected Competitors ({form.watch("competitors").length})
                  </Label>
                  <div className="flex flex-wrap gap-2">
                    {form.watch("competitors").map((competitor, index) => (
                      <Badge key={index} variant="secondary" className="bg-blue-100 text-blue-600">
                        {competitor}
                        <X
                          className="ml-1 h-3 w-3 cursor-pointer"
                          onClick={() => {
                            const current = form.getValues("competitors");
                            form.setValue("competitors", current.filter((_, i) => i !== index));
                          }}
                        />
                      </Badge>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        );

      case 4:
        return (
          <div className="space-y-6">
            <div className="text-center mb-8">
              <h2 className="text-2xl font-bold text-gray-900 mb-2">
                Which platforms do you want to monitor?
              </h2>
              <p className="text-gray-600">
                Select the social media platforms you'd like to track for brand mentions and engagement.
              </p>
            </div>

            <div className="space-y-4">
              {[
                { id: "instagram", label: "Instagram", description: "Track posts, stories, and engagement" },
                { id: "tiktok", label: "TikTok", description: "Monitor viral content and trends" },
                { id: "youtube", label: "YouTube", description: "Analyze video content and comments" },
              ].map((platform) => (
                <div
                  key={platform.id}
                  className={`p-4 border rounded-lg cursor-pointer transition-colors ${
                    form.watch("platforms").includes(platform.id)
                      ? "border-orange-500 bg-orange-50"
                      : "border-gray-200 hover:border-gray-300"
                  }`}
                  onClick={() => togglePlatform(platform.id)}
                >
                  <div className="flex items-center space-x-3">
                    <Checkbox
                      checked={form.watch("platforms").includes(platform.id)}
                      onChange={() => togglePlatform(platform.id)}
                    />
                    <div>
                      <h3 className="font-medium text-gray-900">{platform.label}</h3>
                      <p className="text-sm text-gray-600">{platform.description}</p>
                    </div>
                  </div>
                </div>
              ))}
              {form.formState.errors.platforms && (
                <p className="text-sm text-red-600">
                  {form.formState.errors.platforms.message}
                </p>
              )}
            </div>
          </div>
        );

      case 5:
        return (
          <div className="space-y-6">
            <div className="text-center mb-8">
              <h2 className="text-2xl font-bold text-gray-900 mb-2">
                Review your setup
              </h2>
              <p className="text-gray-600">
                Please review your monitoring configuration before we set everything up.
              </p>
            </div>

            <div className="space-y-6">
              <Card>
                <CardHeader>
                  <CardTitle className="text-lg">Brand Information</CardTitle>
                </CardHeader>
                <CardContent>
                  <p className="font-medium">{form.watch("brandName")}</p>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="text-lg">Hashtags & Keywords ({form.watch("hashtags").length})</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="flex flex-wrap gap-2">
                    {form.watch("hashtags").map((hashtag) => (
                      <Badge key={hashtag} variant="secondary" className="bg-orange-100 text-orange-600">
                        {hashtag}
                      </Badge>
                    ))}
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="text-lg">Competitors ({form.watch("competitors").length})</CardTitle>
                </CardHeader>
                <CardContent>
                  {form.watch("competitors").length > 0 ? (
                    <div className="flex flex-wrap gap-2">
                      {form.watch("competitors").map((competitor, index) => (
                        <Badge key={index} variant="secondary" className="bg-blue-100 text-blue-600">
                          {competitor}
                        </Badge>
                      ))}
                    </div>
                  ) : (
                    <p className="text-gray-500">No competitors selected</p>
                  )}
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="text-lg">Platforms ({form.watch("platforms").length})</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="flex flex-wrap gap-2">
                    {form.watch("platforms").map((platform) => (
                      <Badge key={platform} variant="secondary" className="bg-green-100 text-green-600">
                        {platform.charAt(0).toUpperCase() + platform.slice(1)}
                      </Badge>
                    ))}
                  </div>
                </CardContent>
              </Card>
            </div>
          </div>
        );

      default:
        return null;
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-orange-50 to-orange-100 flex items-center justify-center p-4">
      <div className="w-full max-w-4xl">
        {/* Step Indicator */}
        <div className="flex justify-center mb-8">
          <div className="flex items-center space-x-4">
            {steps.map((step, index) => (
              <div key={step.number} className="flex items-center">
                <div
                  className={`w-10 h-10 rounded-full flex items-center justify-center font-medium ${
                    step.active
                      ? "bg-orange-500 text-white"
                      : currentStep > step.number
                      ? "bg-green-500 text-white"
                      : "bg-gray-300 text-gray-600"
                  }`}
                >
                  {currentStep > step.number ? (
                    <Check className="h-5 w-5" />
                  ) : (
                    step.number
                  )}
                </div>
                {index < steps.length - 1 && (
                  <div className="w-8 h-0.5 bg-gray-300 mx-2" />
                )}
              </div>
            ))}
          </div>
        </div>

        {/* Step Content */}
        <Card className="bg-white shadow-lg">
          <CardContent className="p-8">
            <div className="mb-8">
              <div className="w-16 h-16 bg-orange-100 rounded-lg flex items-center justify-center mb-4">
                <span className="text-orange-500 font-bold text-lg">
                  {steps[currentStep - 1]?.title}
                </span>
              </div>
            </div>

            {renderStepContent()}

            {/* Navigation */}
            <div className="flex justify-between mt-8">
              <Button
                variant="outline"
                onClick={prevStep}
                disabled={currentStep === 1}
              >
                Previous
              </Button>
              <Button
                onClick={nextStep}
                className="bg-orange-500 hover:bg-orange-600"
                disabled={
                  (currentStep === 1 && !form.watch("brandName")) ||
                  (currentStep === 4 && form.watch("platforms").length === 0)
                }
              >
                {currentStep === 5 ? "Complete Setup" : "Next"}
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Confirmation Modal */}
        <Dialog open={showConfirmation} onOpenChange={setShowConfirmation}>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Confirm Your Setup</DialogTitle>
            </DialogHeader>
            <div className="py-4">
              <p className="text-gray-600 mb-4">
                Are you ready to start monitoring your brand across social media platforms?
              </p>
              <div className="space-y-2 text-sm">
                <p><strong>Brand:</strong> {form.watch("brandName")}</p>
                <p><strong>Hashtags:</strong> {form.watch("hashtags").length} selected</p>
                <p><strong>Competitors:</strong> {form.watch("competitors").length} selected</p>
                <p><strong>Platforms:</strong> {form.watch("platforms").join(", ")}</p>
              </div>
            </div>
            <DialogFooter>
              <Button variant="outline" onClick={() => setShowConfirmation(false)}>
                Go Back
              </Button>
              <Button
                onClick={handleConfirm}
                className="bg-orange-500 hover:bg-orange-600"
                disabled={savePipelineData.isPending}
              >
                {savePipelineData.isPending ? "Setting up..." : "Start Monitoring"}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>
    </div>
  );
}