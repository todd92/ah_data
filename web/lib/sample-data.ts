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
      marginPct: 0.226,
      craftConfidence: 82,
      reagentBreakdown: [
        { itemId: 251665, name: "Silverleaf Thread", quantity: 3, unitPrice: 110735, totalCost: 332205, source: "commodity:region" },
        { itemId: 251691, name: "Embroidery Floss", quantity: 2, unitPrice: 20342, totalCost: 40684, source: "commodity:region" }
      ]
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
      marginPct: 0.164,
      craftConfidence: 67,
      reagentBreakdown: [
        { itemId: 245881, name: "Lexicologist's Vellum", quantity: 3, unitPrice: 1200000, totalCost: 3600000, source: "commodity:region" },
        { itemId: 236950, name: "Mote of Mana", quantity: 1, unitPrice: 1800000, totalCost: 1800000, source: "commodity:region" }
      ]
    }
  ]
};
