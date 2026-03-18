import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { samplePredictionResponse } from "@/lib/sample-data";
import type { PredictionResponse, PredictionSignal } from "@/lib/types";

type PredictionRow = {
  observed_at: string;
  item_id: number;
  item_name: string;
  source: string;
  metric_name: string;
  current_value: number;
  predicted_direction: "up" | "down" | "flat";
  confidence: number;
  predicted_return_pct: number;
  short_mean: number;
  medium_mean: number;
  long_mean: number;
  price_vs_long_pct: number;
  short_vs_medium_pct: number;
  quantity_vs_long_pct: number;
  listings_vs_long_pct: number;
  reason: string;
};

type CooldownRow = {
  item_id: number;
  source: string;
  metric_name: string;
  predicted_direction: "up" | "down";
  cooldown_until: string;
};

function parseFloatOr(v: string | null, fallback: number): number {
  const n = Number.parseFloat(v || "");
  return Number.isFinite(n) ? n : fallback;
}

function mapRows(rows: PredictionRow[], cooldowns: CooldownRow[]): PredictionSignal[] {
  const activeCooldownKeys = new Set(
    cooldowns.map((c) => `${c.item_id}:${c.source}:${c.metric_name}:${c.predicted_direction}`)
  );
  return rows.map((row) => ({
    observedAt: row.observed_at,
    itemId: row.item_id,
    itemName: row.item_name,
    source: row.source,
    metricName: row.metric_name,
    predictedDirection: row.predicted_direction,
    confidence: row.confidence,
    predictedReturnPct: row.predicted_return_pct,
    currentValue: row.current_value,
    reason: row.reason,
    shortMean: row.short_mean,
    mediumMean: row.medium_mean,
    longMean: row.long_mean,
    priceVsLongPct: row.price_vs_long_pct,
    shortVsMediumPct: row.short_vs_medium_pct,
    quantityVsLongPct: row.quantity_vs_long_pct,
    listingsVsLongPct: row.listings_vs_long_pct,
    cooldownActive: activeCooldownKeys.has(
      `${row.item_id}:${row.source}:${row.metric_name}:${row.predicted_direction}`
    )
  }));
}

export async function GET(request: NextRequest) {
  const params = request.nextUrl.searchParams;
  const direction = (params.get("direction") || "both") as "both" | "up" | "down";
  const minConfidence = parseFloatOr(params.get("min_confidence"), 0.8);
  const limit = Math.max(1, Math.min(200, Number.parseInt(params.get("limit") || "50", 10) || 50));
  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseAnonKey = process.env.SUPABASE_ANON_KEY;

  if (!supabaseUrl || !supabaseAnonKey) {
    return NextResponse.json({
      ...samplePredictionResponse,
      filters: { direction, minConfidence }
    });
  }

  const supabase = createClient(supabaseUrl, supabaseAnonKey, { auth: { persistSession: false } });
  let query = supabase
    .from("predictions")
    .select(
      "observed_at, item_id, item_name, source, metric_name, current_value, predicted_direction, confidence, predicted_return_pct, short_mean, medium_mean, long_mean, price_vs_long_pct, short_vs_medium_pct, quantity_vs_long_pct, listings_vs_long_pct, reason"
    )
    .gte("confidence", minConfidence)
    .neq("predicted_direction", "flat")
    .order("observed_at", { ascending: false })
    .order("confidence", { ascending: false })
    .limit(limit);

  if (direction !== "both") query = query.eq("predicted_direction", direction);

  const { data, error } = await query;
  if (error) {
    return NextResponse.json(
      {
        ...samplePredictionResponse,
        filters: { direction, minConfidence }
      },
      { status: 200 }
    );
  }

  const latestObservedAt = (data && data[0]?.observed_at) || null;
  let cooldowns: CooldownRow[] = [];
  if (latestObservedAt) {
    const { data: cooldownData } = await supabase
      .from("prediction_cooldowns")
      .select("item_id, source, metric_name, predicted_direction, cooldown_until")
      .gt("cooldown_until", latestObservedAt);
    cooldowns = (cooldownData || []) as CooldownRow[];
  }

  const payload: PredictionResponse = {
    source: "supabase",
    filters: { direction, minConfidence },
    rows: mapRows((data || []) as PredictionRow[], cooldowns)
  };
  return NextResponse.json(payload);
}
