// CSV-only mode: No database required
// This file is kept for compatibility but doesn't actually connect to any database

export const pool = null;
export const db = null;

// Mock database functions for compatibility
export const mockDb = {
  execute: async (query: string) => {
    console.log('Mock DB execute called with:', query);
    return { rows: [] };
  }
};