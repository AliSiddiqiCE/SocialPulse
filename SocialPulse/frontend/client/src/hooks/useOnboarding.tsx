import { useState, useEffect } from 'react';
import { useAuth } from './useAuth';

export function useOnboarding() {
  const [showOnboarding, setShowOnboarding] = useState(false);
  const [hasCompletedOnboarding, setHasCompletedOnboarding] = useState(false);
  const { user, isAuthenticated } = useAuth();

  useEffect(() => {
    if (isAuthenticated && user && (user as any).id) {
      // Check if user has completed onboarding
      const onboardingKey = `onboarding_completed_${(user as any).id}`;
      const completed = localStorage.getItem(onboardingKey);
      
      if (!completed) {
        setShowOnboarding(true);
        setHasCompletedOnboarding(false);
      } else {
        setHasCompletedOnboarding(true);
      }
    }
  }, [isAuthenticated, user]);

  const completeOnboarding = () => {
    if (user && (user as any).id) {
      const onboardingKey = `onboarding_completed_${(user as any).id}`;
      localStorage.setItem(onboardingKey, 'true');
      setHasCompletedOnboarding(true);
      setShowOnboarding(false);
    }
  };

  const resetOnboarding = () => {
    if (user && (user as any).id) {
      const onboardingKey = `onboarding_completed_${(user as any).id}`;
      localStorage.removeItem(onboardingKey);
      setHasCompletedOnboarding(false);
      setShowOnboarding(true);
    }
  };

  return {
    showOnboarding,
    hasCompletedOnboarding,
    completeOnboarding,
    resetOnboarding,
  };
}