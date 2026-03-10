"use client";

import { useEffect, useMemo, useState } from "react";
import type { Opportunity, OpportunityResponse } from "@/lib/types";
import { sampleResponse } from "@/lib/sample-data";

type Filters = {
  profession: "all" | "tailoring" | "enchanting";
  direction: "both" | "buy" | "sell";
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
            </select>
          </div>
          <div>
            <label htmlFor="direction">Signal</label>
            <select
              id="direction"
              value={filters.direction}
              onChange={(e) => setFilters((x) => ({ ...x, direction: e.target.value as Filters["direction"] }))}
            >
              <option value="both">Buy + Sell</option>
              <option value="buy">Buy only</option>
              <option value="sell">Sell only</option>
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
                <th>Signal</th>
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
                    <td>
                      <span className={`pill ${row.direction === "sell" ? "bad" : ""}`}>{row.direction.toUpperCase()}</span>
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
