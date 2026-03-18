export type Profession =
  | "tailoring"
  | "enchanting"
  | "inscription"
  | "leatherworking"
  | "alchemy"
  | "blacksmithing"
  | "engineering"
  | "jewelcrafting"
  | "cooking"
  | "unknown";

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
  profitHistory: Array<{
    alertedAt: string;
    expectedProfit: number;
    saleValue: number | null;
    craftCost: number | null;
    marginPct: number | null;
    craftConfidence: number | null;
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

export type PredictionSignal = {
  observedAt: string;
  itemId: number;
  itemName: string;
  source: string;
  metricName: string;
  predictedDirection: "up" | "down" | "flat";
  confidence: number;
  predictedReturnPct: number;
  currentValue: number;
  reason: string;
  shortMean: number;
  mediumMean: number;
  longMean: number;
  priceVsLongPct: number;
  shortVsMediumPct: number;
  quantityVsLongPct: number;
  listingsVsLongPct: number;
  cooldownActive: boolean;
};

export type PredictionResponse = {
  rows: PredictionSignal[];
  source: "supabase" | "sample";
  filters: {
    direction: "both" | "up" | "down";
    minConfidence: number;
  };
};
