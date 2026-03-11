"use client";

import { useEffect, useMemo, useState } from "react";
import type { Opportunity, OpportunityResponse } from "@/lib/types";
import { sampleResponse } from "@/lib/sample-data";

type Filters = {
  profession: "all" | "tailoring" | "enchanting" | "inscription" | "leatherworking" | "alchemy" | "blacksmithing" | "engineering";
  direction: "both" | "buy";
  minProfitGold: number;
  minMarginPct: number;
};

const defaultFilters: Filters = {
  profession: "all",
  direction: "both",
  minProfitGold: 50,
  minMarginPct: 10
};

function moneyFromCopper(copper: number | null): string {
  if (copper === null) return "-";
  const gold = Math.floor(copper / 10000);
  const silver = Math.floor((copper % 10000) / 100);
  const c = copper % 100;
  return `${gold}g ${silver}s ${c}c`;
}

function pct(v: number | null): string {
  if (v === null) return "-";
  return `${(v * 100).toFixed(1)}%`;
}

function confidenceLabel(v: number | null): string {
  if (v === null) return "-";
  if (v >= 75) return `High (${v})`;
  if (v >= 50) return `Medium (${v})`;
  return `Low (${v})`;
}

function Sparkline({ values }: { values: number[] }) {
  if (values.length === 0) return <span className="small">-</span>;
  const width = 120;
  const height = 36;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = Math.max(max - min, 1);
  const points = values
    .map((value, index) => {
      const x = values.length === 1 ? width / 2 : (index / (values.length - 1)) * width;
      const y = height - (((value - min) / range) * (height - 6) + 3);
      return `${x},${y}`;
    })
    .join(" ");
  const trendUp = values[values.length - 1] >= values[0];
  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Profit history">
      <polyline
        fill="none"
        stroke={trendUp ? "currentColor" : "#b45309"}
        strokeWidth="2"
        points={points}
      />
    </svg>
  );
}

function useStats(rows: Opportunity[]) {
  return useMemo(() => {
    const profitable = rows.filter((r) => (r.expectedProfit || 0) > 0);
    const bestProfit = Math.max(...rows.map((r) => r.expectedProfit || 0), 0);
    const avgMargin = rows.length > 0 ? rows.reduce((sum, r) => sum + (r.marginPct || 0), 0) / rows.length : 0;
    return { profitableCount: profitable.length, bestProfit, avgMargin };
  }, [rows]);
}

export default function HomePage() {
  const [filters, setFilters] = useState<Filters>(defaultFilters);
  const [rows, setRows] = useState<Opportunity[]>(sampleResponse.rows);
  const [source, setSource] = useState<"supabase" | "sample">("sample");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>("");

  const stats = useStats(rows);

  function craftDirectionLabel(direction: "buy" | "sell") {
    return direction === "buy" ? "CRAFT" : "SELL_MATS";
  }

  async function analyze() {
    setLoading(true);
    setError("");
    try {
      const qs = new URLSearchParams({
        profession: filters.profession,
        direction: filters.direction,
        min_profit_gold: String(filters.minProfitGold),
        min_margin_pct: String(filters.minMarginPct),
        limit: "100"
      });
      const resp = await fetch(`/api/opportunities?${qs.toString()}`, { cache: "no-store" });
      if (!resp.ok) throw new Error(`API error ${resp.status}`);
      const data = (await resp.json()) as OpportunityResponse;
      setRows(data.rows);
      setSource(data.source);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Unexpected error");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void analyze();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <main className="wrap">
      <section className="hero">
        <h1>AH Crafting Radar</h1>
        <p>Choose a profession and rank crafts by expected profit, margin, and current market conditions.</p>
      </section>

      <section className="card">
        <div className="filters">
          <div>
            <label htmlFor="profession">Profession</label>
            <select
              id="profession"
              value={filters.profession}
              onChange={(e) => setFilters((x) => ({ ...x, profession: e.target.value as Filters["profession"] }))}
            >
              <option value="all">All</option>
              <option value="tailoring">Tailoring</option>
              <option value="enchanting">Enchanting</option>
              <option value="inscription">Inscription</option>
              <option value="leatherworking">Leatherworking</option>
              <option value="alchemy">Alchemy</option>
              <option value="blacksmithing">Blacksmithing</option>
              <option value="engineering">Engineering</option>
            </select>
          </div>
          <div>
            <label htmlFor="direction">Signal</label>
            <select
              id="direction"
              value={filters.direction}
              onChange={(e) => setFilters((x) => ({ ...x, direction: e.target.value as Filters["direction"] }))}
            >
              <option value="both">All craft alerts</option>
              <option value="buy">Craft only</option>
            </select>
          </div>
          <div>
            <label htmlFor="profit">Min Profit (gold)</label>
            <input
              id="profit"
              type="number"
              min={0}
              value={filters.minProfitGold}
              onChange={(e) => setFilters((x) => ({ ...x, minProfitGold: Number(e.target.value || 0) }))}
            />
          </div>
          <div>
            <label htmlFor="margin">Min Margin (%)</label>
            <input
              id="margin"
              type="number"
              min={0}
              value={filters.minMarginPct}
              onChange={(e) => setFilters((x) => ({ ...x, minMarginPct: Number(e.target.value || 0) }))}
            />
          </div>
          <div>
            <button type="button" onClick={analyze} disabled={loading}>
              {loading ? "Analyzing..." : "Analyze"}
            </button>
          </div>
        </div>
        <div className="meta">
          Data source: <strong>{source}</strong> {error ? `| Error: ${error}` : ""}
        </div>
      </section>

      <section className="card">
        <div className="stats">
          <div className="stat">
            <div className="stat-k">Results</div>
            <div className="stat-v">{rows.length}</div>
          </div>
          <div className="stat">
            <div className="stat-k">Profitable Crafts</div>
            <div className="stat-v">{stats.profitableCount}</div>
          </div>
          <div className="stat">
            <div className="stat-k">Best Profit</div>
            <div className="stat-v">{moneyFromCopper(stats.bestProfit)}</div>
          </div>
          <div className="stat">
            <div className="stat-k">Average Margin</div>
            <div className="stat-v">{(stats.avgMargin * 100).toFixed(1)}%</div>
          </div>
        </div>
      </section>

      <section className="card">
        {rows.length === 0 ? (
          <div className="empty">No matching opportunities for these filters.</div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Craft</th>
                <th>Recipe</th>
                <th>Profession</th>
                <th>Craft Cost</th>
                <th>Sale Value</th>
                <th>Profit</th>
                <th>Margin</th>
                <th>Confidence</th>
                <th>Signal</th>
                <th>Trend</th>
                <th>Reagents</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => {
                const p = row.expectedProfit || 0;
                const positive = p >= 0;
                return (
                  <tr key={`${row.alertedAt}-${row.itemId}-${row.recipeId || "none"}`}>
                    <td>
                      <strong>{row.itemName}</strong>
                      <div className="small">{row.source}</div>
                    </td>
                    <td>{row.recipeName || `Recipe #${row.recipeId || "?"}`}</td>
                    <td style={{ textTransform: "capitalize" }}>{row.profession}</td>
                    <td>{moneyFromCopper(row.craftCost)}</td>
                    <td>{moneyFromCopper(row.saleValue)}</td>
                    <td className={positive ? "good" : "bad"}>{moneyFromCopper(row.expectedProfit)}</td>
                    <td className={positive ? "good" : "bad"}>{pct(row.marginPct)}</td>
                    <td>{confidenceLabel(row.craftConfidence)}</td>
                    <td>
                      <span className={`pill ${row.direction === "sell" ? "bad" : ""}`}>{craftDirectionLabel(row.direction)}</span>
                    </td>
                    <td>
                      <div className="small">
                        <Sparkline values={row.profitHistory.map((point) => point.expectedProfit)} />
                        <div>{row.profitHistory.length} pts</div>
                      </div>
                    </td>
                    <td>
                      {row.reagentBreakdown.length === 0 ? (
                        <span className="small">-</span>
                      ) : (
                        <details>
                          <summary>{row.reagentBreakdown.length} mats</summary>
                          <div className="small">
                            {row.reagentBreakdown.map((reagent) => (
                              <div key={`${row.itemId}-${reagent.itemId}`}>
                                {reagent.quantity}x {reagent.name}: {moneyFromCopper(reagent.totalCost)}
                              </div>
                            ))}
                          </div>
                        </details>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </section>
    </main>
  );
}
