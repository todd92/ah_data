import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { sampleResponse } from "@/lib/sample-data";
import type { Opportunity, OpportunityResponse, Profession } from "@/lib/types";

type AlertRow = {
  alerted_at: string;
  observed_at: string;
  item_id: number;
  item_name: string;
  source: string;
  direction: "buy" | "sell";
  profession: string | null;
  recipe_id: number | null;
  recipe_name: string | null;
  craft_cost: number | null;
  sale_value: number | null;
  expected_profit: number | null;
  margin_pct: number | null;
  craft_confidence: number | null;
  reagent_breakdown:
    | Array<{
        item_id: number;
        name: string;
        quantity: number;
        unit_price: number;
        total_cost: number;
        source: string;
      }>
    | string
    | null;
};

function toProfession(raw: string | undefined): Profession {
  const lowered = (raw || "").toLowerCase();
  if (lowered.includes("tailor")) return "tailoring";
  if (lowered.includes("enchant")) return "enchanting";
  if (lowered.includes("inscript")) return "inscription";
  if (lowered.includes("leather")) return "leatherworking";
  return "unknown";
}

function copperFromGold(gold: number): number {
  return Math.round(gold * 10000);
}

function parseFloatOr(v: string | null, fallback: number): number {
  const n = Number.parseFloat(v || "");
  return Number.isFinite(n) ? n : fallback;
}

function mapRows(rows: AlertRow[]): Opportunity[] {
  const historyByKey = new Map<string, Opportunity["profitHistory"]>();
  for (const r of rows) {
    const key = `${r.item_id}:${r.recipe_id || 0}:${r.source}`;
    const history = historyByKey.get(key) || [];
    history.push({
      alertedAt: r.alerted_at,
      expectedProfit: r.expected_profit || 0,
      saleValue: r.sale_value,
      craftCost: r.craft_cost,
      marginPct: r.margin_pct,
      craftConfidence: r.craft_confidence
    });
    historyByKey.set(key, history);
  }

  return rows.map((r) => ({
    alertedAt: r.alerted_at,
    observedAt: r.observed_at,
    itemId: r.item_id,
    itemName: r.item_name,
    source: r.source,
    direction: r.direction,
    recipeId: r.recipe_id,
    recipeName: r.recipe_name,
    profession: toProfession(r.profession || undefined),
    craftCost: r.craft_cost,
    saleValue: r.sale_value,
    expectedProfit: r.expected_profit,
    marginPct: r.margin_pct,
    craftConfidence: r.craft_confidence,
    reagentBreakdown: Array.isArray(r.reagent_breakdown)
      ? r.reagent_breakdown.map((entry) => ({
          itemId: entry.item_id,
          name: entry.name,
          quantity: entry.quantity,
          unitPrice: entry.unit_price,
          totalCost: entry.total_cost,
          source: entry.source
        }))
      : typeof r.reagent_breakdown === "string"
        ? (() => {
            try {
              const parsed = JSON.parse(r.reagent_breakdown);
              return Array.isArray(parsed)
                ? parsed.map((entry) => ({
                    itemId: entry.item_id,
                    name: entry.name,
                    quantity: entry.quantity,
                    unitPrice: entry.unit_price,
                    totalCost: entry.total_cost,
                    source: entry.source
                  }))
                : [];
            } catch {
              return [];
            }
          })()
        : [],
    profitHistory: (historyByKey.get(`${r.item_id}:${r.recipe_id || 0}:${r.source}`) || [])
      .slice(0, 12)
      .reverse()
  }));
}

export async function GET(request: NextRequest) {
  const params = request.nextUrl.searchParams;
  const profession = (params.get("profession") || "all") as Profession | "all";
  const direction = (params.get("direction") || "both") as "buy" | "both";
  const minProfitGold = parseFloatOr(params.get("min_profit_gold"), 50);
  const minMarginPct = parseFloatOr(params.get("min_margin_pct"), 10);
  const limit = Math.max(1, Math.min(200, Number.parseInt(params.get("limit") || "50", 10) || 50));

  const minProfitCopper = copperFromGold(minProfitGold);
  const minMarginRatio = minMarginPct / 100.0;
  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseAnonKey = process.env.SUPABASE_ANON_KEY;
  if (!supabaseUrl || !supabaseAnonKey) {
    const sample = {
      ...sampleResponse,
      filters: { profession, minProfitGold, minMarginPct, direction }
    };
    return NextResponse.json(sample);
  }

  const supabase = createClient(supabaseUrl, supabaseAnonKey, { auth: { persistSession: false } });
  let query = supabase
    .from("alerts")
    .select(
      "alerted_at, observed_at, item_id, item_name, source, direction, profession, recipe_id, recipe_name, craft_cost, sale_value, expected_profit, margin_pct, craft_confidence, reagent_breakdown"
    )
    .eq("alert_kind", "craft_arbitrage")
    .order("alerted_at", { ascending: false })
    .limit(500);

  if (direction !== "both") query = query.eq("direction", direction);

  const { data, error } = await query;
  if (error) {
    const sample = {
      ...sampleResponse,
      filters: { profession, minProfitGold, minMarginPct, direction }
    };
    return NextResponse.json(sample, { status: 200 });
  }

  let rows = mapRows((data || []) as AlertRow[]);
  rows = rows.filter((r) => {
    const profit = r.expectedProfit || 0;
    const margin = r.marginPct || 0;
    if (direction === "buy") return profit >= minProfitCopper && margin >= minMarginRatio;
    return profit >= minProfitCopper && margin >= minMarginRatio;
  });
  if (profession !== "all") {
    rows = rows.filter((r) => r.profession === profession);
  }
  rows = rows.slice(0, limit);

  const payload: OpportunityResponse = {
    source: "supabase",
    filters: { profession, minProfitGold, minMarginPct, direction },
    rows
  };
  return NextResponse.json(payload);
}
