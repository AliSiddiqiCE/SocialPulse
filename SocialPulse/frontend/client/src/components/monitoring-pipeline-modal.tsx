import { useState } from "react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Badge } from "@/components/ui/badge";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog";
import { useToast } from "@/hooks/use-toast";
import { apiRequest } from "@/lib/queryClient";
import { X, Plus, Check, ArrowLeft, ArrowRight } from "lucide-react";

const pipelineSchema = z.object({
  brandName: z.string().min(1, "Brand name is required"),
  hashtags: z.array(z.string()),
  competitors: z.array(z.string()),
  platforms: z.array(z.string()).min(1, "Select at least one platform"),
});

type PipelineData = z.infer<typeof pipelineSchema>;

interface MonitoringPipelineModalProps {
  isOpen: boolean;
  onClose: () => void;
  onComplete?: () => void;
}

export function MonitoringPipelineModal({ isOpen, onClose, onComplete }: MonitoringPipelineModalProps) {
  const [currentStep, setCurrentStep] = useState(1);
  const [showConfirmation, setShowConfirmation] = useState(false);
  const { toast } = useToast();
  const queryClient = useQueryClient();

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
    queryKey: ["/api/hashtags/industry"],
    queryFn: async () => {
      const response = await fetch("/api/hashtags/industry", {
        credentials: "include"
      });
      if (!response.ok) return [];
      return response.json();
    },
  });

  // Create monitoring pipeline
  const createPipeline = useMutation({
    mutationFn: async (data: PipelineData) => {
      const response = await fetch("/api/brands", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        credentials: "include",
        body: JSON.stringify({
          name: data.brandName,
          hashtags: data.hashtags,
          competitors: data.competitors,
          platforms: data.platforms,
        }),
      });
      if (!response.ok) throw new Error("Failed to create brand");
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/brands"] });
      setShowConfirmation(true);
      toast({
        title: "Success!",
        description: "Brand monitoring pipeline created successfully.",
      });
    },
    onError: (error) => {
      toast({
        title: "Error",
        description: "Failed to create monitoring pipeline. Please try again.",
        variant: "destructive",
      });
    },
  });

  const resetForm = () => {
    form.reset();
    setCurrentStep(1);
    setShowConfirmation(false);
    setCustomHashtag("");
    setCompetitorInput("");
  };

  const handleClose = () => {
    resetForm();
    onClose();
  };

  const handleNext = () => {
    if (currentStep < 4) {
      setCurrentStep(currentStep + 1);
    }
  };

  const handlePrevious = () => {
    if (currentStep > 1) {
      setCurrentStep(currentStep - 1);
    }
  };

  const addCustomHashtag = () => {
    if (customHashtag.trim()) {
      const current = form.getValues("hashtags");
      const hashtag = customHashtag.startsWith("#") ? customHashtag : `#${customHashtag}`;
      if (!current.includes(hashtag)) {
        form.setValue("hashtags", [...current, hashtag]);
      }
      setCustomHashtag("");
    }
  };

  const removeHashtag = (hashtag: string) => {
    const current = form.getValues("hashtags");
    form.setValue("hashtags", current.filter(h => h !== hashtag));
  };

  const addCompetitor = () => {
    if (competitorInput.trim()) {
      const current = form.getValues("competitors");
      if (!current.includes(competitorInput.trim())) {
        form.setValue("competitors", [...current, competitorInput.trim()]);
      }
      setCompetitorInput("");
    }
  };

  const removeCompetitor = (competitor: string) => {
    const current = form.getValues("competitors");
    form.setValue("competitors", current.filter(c => c !== competitor));
  };

  const toggleHashtag = (hashtag: string) => {
    const current = form.getValues("hashtags");
    const tag = hashtag.startsWith("#") ? hashtag : `#${hashtag}`;
    if (current.includes(tag)) {
      form.setValue("hashtags", current.filter(h => h !== tag));
    } else {
      form.setValue("hashtags", [...current, tag]);
    }
  };

  const togglePlatform = (platform: string) => {
    const current = form.getValues("platforms");
    if (current.includes(platform)) {
      form.setValue("platforms", current.filter(p => p !== platform));
    } else {
      form.setValue("platforms", [...current, platform]);
    }
  };

  const onSubmit = (data: PipelineData) => {
    createPipeline.mutate(data);
  };

  const selectedHashtags = form.watch("hashtags");
  const selectedCompetitors = form.watch("competitors");
  const selectedPlatforms = form.watch("platforms");

  if (showConfirmation) {
    return (
      <Dialog open={isOpen} onOpenChange={handleClose}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle className="text-center text-navy">
              <Check className="w-12 h-12 text-green-500 mx-auto mb-4" />
              Pipeline Created Successfully!
            </DialogTitle>
          </DialogHeader>
          <div className="text-center py-4">
            <p className="text-gray-600 mb-4">
              Your brand monitoring pipeline has been set up successfully. You can now track your brand performance across all selected platforms.
            </p>
            <Button 
              onClick={() => {
                handleClose();
                onComplete?.();
              }}
              className="bg-orange text-white hover:bg-orange/90"
            >
              Start Monitoring
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    );
  }

  return (
    <Dialog open={isOpen} onOpenChange={handleClose}>
      <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="text-navy">Set Up Brand Monitoring Pipeline</DialogTitle>
          <div className="flex items-center space-x-2 mt-4">
            {[1, 2, 3, 4].map((step) => (
              <div key={step} className="flex items-center">
                <div className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-medium ${
                  step <= currentStep ? 'bg-orange text-white' : 'bg-gray-200 text-gray-600'
                }`}>
                  {step}
                </div>
                {step < 4 && <div className={`w-8 h-0.5 ${step < currentStep ? 'bg-orange' : 'bg-gray-200'}`} />}
              </div>
            ))}
          </div>
        </DialogHeader>

        <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-6">
          {currentStep === 1 && (
            <Card>
              <CardHeader>
                <CardTitle className="text-lg text-navy">Brand Information</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div>
                  <Label htmlFor="brandName">Brand Name</Label>
                  <Input
                    id="brandName"
                    placeholder="Enter your brand name"
                    {...form.register("brandName")}
                    className="mt-1"
                  />
                  {form.formState.errors.brandName && (
                    <p className="text-red-500 text-sm mt-1">{form.formState.errors.brandName.message}</p>
                  )}
                </div>
              </CardContent>
            </Card>
          )}

          {currentStep === 2 && (
            <Card>
              <CardHeader>
                <CardTitle className="text-lg text-navy">Hashtags & Keywords</CardTitle>
                <p className="text-sm text-gray-600">Select relevant hashtags to monitor mentions and engagement</p>
              </CardHeader>
              <CardContent className="space-y-4">
                <div>
                  <Label>Add Custom Hashtag</Label>
                  <div className="flex space-x-2 mt-1">
                    <Input
                      value={customHashtag}
                      onChange={(e) => setCustomHashtag(e.target.value)}
                      placeholder="#yourhashtag"
                      onKeyPress={(e) => e.key === 'Enter' && (e.preventDefault(), addCustomHashtag())}
                    />
                    <Button type="button" onClick={addCustomHashtag} variant="outline" size="sm">
                      <Plus size={16} />
                    </Button>
                  </div>
                </div>

                {suggestedHashtags && suggestedHashtags.length > 0 && (
                  <div>
                    <Label>Suggested Industry Hashtags</Label>
                    <div className="flex flex-wrap gap-2 mt-2">
                      {suggestedHashtags.slice(0, 12).map((item: any) => {
                        const hashtag = item.tag.startsWith("#") ? item.tag : `#${item.tag}`;
                        const isSelected = selectedHashtags.includes(hashtag);
                        return (
                          <Badge
                            key={hashtag}
                            variant={isSelected ? "default" : "outline"}
                            className={`cursor-pointer ${isSelected ? 'bg-orange text-white' : 'hover:bg-gray-100'}`}
                            onClick={() => toggleHashtag(hashtag)}
                          >
                            {hashtag}
                          </Badge>
                        );
                      })}
                    </div>
                  </div>
                )}

                {selectedHashtags.length > 0 && (
                  <div>
                    <Label>Selected Hashtags ({selectedHashtags.length})</Label>
                    <div className="flex flex-wrap gap-2 mt-2">
                      {selectedHashtags.map((hashtag) => (
                        <Badge key={hashtag} className="bg-orange text-white">
                          {hashtag}
                          <X 
                            size={14} 
                            className="ml-1 cursor-pointer hover:bg-orange/20 rounded"
                            onClick={() => removeHashtag(hashtag)}
                          />
                        </Badge>
                      ))}
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>
          )}

          {currentStep === 3 && (
            <Card>
              <CardHeader>
                <CardTitle className="text-lg text-navy">Competitor Analysis</CardTitle>
                <p className="text-sm text-gray-600">Add competitors to track and compare performance</p>
              </CardHeader>
              <CardContent className="space-y-4">
                <div>
                  <Label>Add Competitor</Label>
                  <div className="flex space-x-2 mt-1">
                    <Input
                      value={competitorInput}
                      onChange={(e) => setCompetitorInput(e.target.value)}
                      placeholder="Competitor brand name"
                      onKeyPress={(e) => e.key === 'Enter' && (e.preventDefault(), addCompetitor())}
                    />
                    <Button type="button" onClick={addCompetitor} variant="outline" size="sm">
                      <Plus size={16} />
                    </Button>
                  </div>
                </div>

                {selectedCompetitors.length > 0 && (
                  <div>
                    <Label>Selected Competitors ({selectedCompetitors.length})</Label>
                    <div className="flex flex-wrap gap-2 mt-2">
                      {selectedCompetitors.map((competitor) => (
                        <Badge key={competitor} className="bg-navy text-white">
                          {competitor}
                          <X 
                            size={14} 
                            className="ml-1 cursor-pointer hover:bg-navy/20 rounded"
                            onClick={() => removeCompetitor(competitor)}
                          />
                        </Badge>
                      ))}
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>
          )}

          {currentStep === 4 && (
            <Card>
              <CardHeader>
                <CardTitle className="text-lg text-navy">Platform Selection</CardTitle>
                <p className="text-sm text-gray-600">Choose which platforms to monitor</p>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  {[
                    { id: "instagram", name: "Instagram", description: "Photos, Stories, Reels" },
                    { id: "tiktok", name: "TikTok", description: "Short-form videos" },
                    { id: "youtube", name: "YouTube", description: "Long-form videos" },
                    { id: "twitter", name: "Twitter", description: "Real-time updates" },
                  ].map((platform) => (
                    <div key={platform.id} className="border rounded-lg p-4 cursor-pointer hover:border-orange"
                         onClick={() => togglePlatform(platform.id)}>
                      <div className="flex items-center space-x-3">
                        <Checkbox
                          checked={selectedPlatforms.includes(platform.id)}
                          onChange={() => togglePlatform(platform.id)}
                        />
                        <div>
                          <div className="font-medium">{platform.name}</div>
                          <div className="text-sm text-gray-500">{platform.description}</div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
                {form.formState.errors.platforms && (
                  <p className="text-red-500 text-sm">{form.formState.errors.platforms.message}</p>
                )}
              </CardContent>
            </Card>
          )}

          <DialogFooter className="flex justify-between">
            <div className="flex space-x-2">
              {currentStep > 1 && (
                <Button type="button" variant="outline" onClick={handlePrevious}>
                  <ArrowLeft size={16} className="mr-2" />
                  Previous
                </Button>
              )}
            </div>
            <div className="flex space-x-2">
              {currentStep < 4 ? (
                <Button type="button" onClick={handleNext} className="bg-orange text-white hover:bg-orange/90">
                  Next
                  <ArrowRight size={16} className="ml-2" />
                </Button>
              ) : (
                <Button 
                  type="submit" 
                  disabled={createPipeline.isPending}
                  className="bg-orange text-white hover:bg-orange/90"
                >
                  {createPipeline.isPending ? "Creating..." : "Create Pipeline"}
                </Button>
              )}
            </div>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}