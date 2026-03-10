import type { OpportunityResponse } from "./types";

export const sampleResponse: OpportunityResponse = {
  source: "sample",
  filters: {
    profession: "all",
    minProfitGold: 50,
    minMarginPct: 10,
    direction: "both"
  },
  rows: [
    {
      alertedAt: "2026-03-09T15:00:00Z",
      observedAt: "2026-03-09T14:59:00Z",
      itemId: 238523,
      itemName: "Carving Canine",
      source: "commodity:region",
      direction: "buy",
      recipeId: 101001,
      recipeName: "Embroider Gilded Spellthread",
      profession: "tailoring",
      craftCost: 18220000,
      saleValue: 22340000,
      expectedProfit: 4120000,
      marginPct: 0.226
    },
    {
      alertedAt: "2026-03-09T15:00:00Z",
      observedAt: "2026-03-09T14:59:00Z",
      itemId: 245881,
      itemName: "Lexicologist's Vellum",
      source: "commodity:region",
      direction: "buy",
      recipeId: 101002,
      recipeName: "Bind Radiant Matrix",
      profession: "enchanting",
      craftCost: 7400000,
      saleValue: 8610000,
      expectedProfit: 1210000,
      marginPct: 0.164
    },
    {
      alertedAt: "2026-03-09T15:00:00Z",
      observedAt: "2026-03-09T14:59:00Z",
      itemId: 237366,
      itemName: "Dazzling Thorium",
      source: "commodity:region",
      direction: "sell",
      recipeId: 101003,
      recipeName: "Weave Shimmering Bolt",
      profession: "tailoring",
      craftCost: 5900000,
      saleValue: 5360000,
      expectedProfit: -540000,
      marginPct: -0.092
    }
  ]
};
