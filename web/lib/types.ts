export type Profession = "tailoring" | "enchanting" | "unknown";

export type Opportunity = {
  alertedAt: string;
  observedAt: string;
  itemId: number;
  itemName: string;
  source: string;
  direction: "buy" | "sell";
  recipeId: number | null;
  recipeName: string | null;
  profession: Profession;
  craftCost: number | null;
  saleValue: number | null;
  expectedProfit: number | null;
  marginPct: number | null;
  craftConfidence: number | null;
  reagentBreakdown: Array<{
    itemId: number;
    name: string;
    quantity: number;
    unitPrice: number;
    totalCost: number;
    source: string;
  }>;
};

export type OpportunityResponse = {
  rows: Opportunity[];
  source: "supabase" | "sample";
  filters: {
    profession: Profession | "all";
    minProfitGold: number;
    minMarginPct: number;
    direction: "buy" | "both";
  };
};
