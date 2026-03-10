import { readFile } from "node:fs/promises";
import path from "node:path";
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
  recipe_id: number | null;
  recipe_name: string | null;
  craft_cost: number | null;
  sale_value: number | null;
  expected_profit: number | null;
  margin_pct: number | null;
};

type RecipeMeta = {
  recipe_id: number;
  profession?: string;
};

function toProfession(raw: string | undefined): Profession {
  const lowered = (raw || "").toLowerCase();
  if (lowered.includes("tailor")) return "tailoring";
  if (lowered.includes("enchant")) return "enchanting";
  return "unknown";
}

async function loadProfessionByRecipeId(): Promise<Map<number, Profession>> {
  const map = new Map<number, Profession>();
  const configured = process.env.WATCHLIST_FILE || "../targets_midnight_tailoring_enchanting.json";
  const filePath = path.resolve(process.cwd(), configured);
  try {
    const txt = await readFile(filePath, "utf8");
    const parsed = JSON.parse(txt) as { recipes?: RecipeMeta[] };
    const recipes = Array.isArray(parsed.recipes) ? parsed.recipes : [];
    for (const row of recipes) {
      if (typeof row.recipe_id === "number") {
        map.set(row.recipe_id, toProfession(row.profession));
      }
    }
  } catch {
    return map;
  }
  return map;
}

function copperFromGold(gold: number): number {
  return Math.round(gold * 10000);
}

function parseFloatOr(v: string | null, fallback: number): number {
  const n = Number.parseFloat(v || "");
  return Number.isFinite(n) ? n : fallback;
}

function mapRows(rows: AlertRow[], professionByRecipeId: Map<number, Profession>): Opportunity[] {
  return rows.map((r) => ({
    alertedAt: r.alerted_at,
    observedAt: r.observed_at,
    itemId: r.item_id,
    itemName: r.item_name,
    source: r.source,
    direction: r.direction,
    recipeId: r.recipe_id,
    recipeName: r.recipe_name,
    profession: r.recipe_id ? professionByRecipeId.get(r.recipe_id) || "unknown" : "unknown",
    craftCost: r.craft_cost,
    saleValue: r.sale_value,
    expectedProfit: r.expected_profit,
    marginPct: r.margin_pct
  }));
}

export async function GET(request: NextRequest) {
  const params = request.nextUrl.searchParams;
  const profession = (params.get("profession") || "all") as Profession | "all";
  const direction = (params.get("direction") || "both") as "buy" | "sell" | "both";
  const minProfitGold = parseFloatOr(params.get("min_profit_gold"), 50);
  const minMarginPct = parseFloatOr(params.get("min_margin_pct"), 10);
  const limit = Math.max(1, Math.min(200, Number.parseInt(params.get("limit") || "50", 10) || 50));

  const minProfitCopper = copperFromGold(minProfitGold);
  const minMarginRatio = minMarginPct / 100.0;
  const professionByRecipeId = await loadProfessionByRecipeId();

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
      "alerted_at, observed_at, item_id, item_name, source, direction, recipe_id, recipe_name, craft_cost, sale_value, expected_profit, margin_pct"
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

  let rows = mapRows((data || []) as AlertRow[], professionByRecipeId);
  rows = rows.filter((r) => {
    const profit = r.expectedProfit || 0;
    const margin = r.marginPct || 0;
    if (direction === "buy") return profit >= minProfitCopper && margin >= minMarginRatio;
    if (direction === "sell") return profit <= -minProfitCopper && margin <= -minMarginRatio;
    return (profit >= minProfitCopper && margin >= minMarginRatio) || (profit <= -minProfitCopper && margin <= -minMarginRatio);
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
