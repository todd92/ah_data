"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import type { PredictionResponse, PredictionSignal } from "@/lib/types";
import { samplePredictionResponse } from "@/lib/sample-data";

type Filters = {
  direction: "both" | "up" | "down";
  minConfidence: number;
};

const defaultFilters: Filters = {
  direction: "both",
  minConfidence: 0.8
};

function moneyFromCopper(copper: number): string {
  const gold = Math.floor(copper / 10000);
  const silver = Math.floor((copper % 10000) / 100);
  const c = copper % 100;
  return `${gold}g ${silver}s ${c}c`;
}

function pct(v: number): string {
  return `${(v * 100).toFixed(1)}%`;
}

function sourceLabel(source: string): string {
  if (source === "commodity:region") return "Regional commodity AH";
  return source;
}

function renderReagentName(name: string) {
  if (name.endsWith(" [Gold]")) {
    const baseName = name.slice(0, -7);
    return (
      <span>
        {baseName}{" "}
        <span style={{ color: "#f59e0b", fontWeight: "bold" }}>[Gold]</span>
      </span>
    );
  }
  if (name.endsWith(" [Silver]")) {
    const baseName = name.slice(0, -9);
    return (
      <span>
        {baseName}{" "}
        <span style={{ color: "#9ca3af", fontWeight: "bold" }}>[Silver]</span>
      </span>
    );
  }
  return <span>{name}</span>;
}

function useStats(rows: PredictionSignal[]) {
  return useMemo(() => {
    const up = rows.filter((r) => r.predictedDirection === "up").length;
    const down = rows.filter((r) => r.predictedDirection === "down").length;
    const avgConfidence = rows.length > 0 ? rows.reduce((sum, r) => sum + r.confidence, 0) / rows.length : 0;
    const cooldownCount = rows.filter((r) => r.cooldownActive).length;
    return { up, down, avgConfidence, cooldownCount };
  }, [rows]);
}

export default function PredictionsPage() {
  const [filters, setFilters] = useState<Filters>(defaultFilters);
  const [rows, setRows] = useState<PredictionSignal[]>(samplePredictionResponse.rows);
  const [source, setSource] = useState<"supabase" | "sample">("sample");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const stats = useStats(rows);

  async function analyze() {
    setLoading(true);
    setError("");
    try {
      const qs = new URLSearchParams({
        direction: filters.direction,
        min_confidence: String(filters.minConfidence),
        limit: "100"
      });
      const resp = await fetch(`/api/predictions?${qs.toString()}`, { cache: "no-store" });
      if (!resp.ok) throw new Error(`API error ${resp.status}`);
      const data = (await resp.json()) as PredictionResponse;
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
      <nav className="topnav">
        <Link href="/">Craft Alerts</Link>
        <Link href="/predictions" className="active">
          Predictions
        </Link>
      </nav>

      <section className="hero">
        <h1>AH Prediction Signals</h1>
        <p>High-confidence directional calls from the prediction pipeline, with cooldown-aware suppression and source context.</p>
      </section>

      <section className="card">
        <div className="filters">
          <div>
            <label htmlFor="direction">Direction</label>
            <select
              id="direction"
              value={filters.direction}
              onChange={(e) => setFilters((x) => ({ ...x, direction: e.target.value as Filters["direction"] }))}
            >
              <option value="both">All predictions</option>
              <option value="up">Up only</option>
              <option value="down">Down only</option>
            </select>
          </div>
          <div>
            <label htmlFor="confidence">Min Confidence</label>
            <input
              id="confidence"
              type="number"
              min={0}
              max={1}
              step="0.05"
              value={filters.minConfidence}
              onChange={(e) => setFilters((x) => ({ ...x, minConfidence: Number(e.target.value || 0) }))}
            />
          </div>
          <div>
            <button type="button" onClick={analyze} disabled={loading}>
              {loading ? "Loading..." : "Refresh"}
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
            <div className="stat-k">Rows</div>
            <div className="stat-v">{rows.length}</div>
          </div>
          <div className="stat">
            <div className="stat-k">Up Signals</div>
            <div className="stat-v">{stats.up}</div>
          </div>
          <div className="stat">
            <div className="stat-k">Down Signals</div>
            <div className="stat-v">{stats.down}</div>
          </div>
          <div className="stat">
            <div className="stat-k">Avg Confidence</div>
            <div className="stat-v">{(stats.avgConfidence * 100).toFixed(1)}%</div>
          </div>
          <div className="stat">
            <div className="stat-k">Cooldown Active</div>
            <div className="stat-v">{stats.cooldownCount}</div>
          </div>
        </div>
      </section>

      <section className="card">
        {rows.length === 0 ? (
          <div className="empty">No prediction rows matched these filters.</div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Item</th>
                <th>Direction</th>
                <th>Confidence</th>
                <th>Expected Move</th>
                <th>Current Price</th>
                <th>Observed</th>
                <th>Cooldown</th>
                <th>Reason</th>
                <th>Source</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={`${row.observedAt}:${row.itemId}:${row.source}:${row.predictedDirection}`}>
                  <td>
                    <strong>{renderReagentName(row.itemName)}</strong>
                    <div className="small">#{row.itemId}</div>
                  </td>
                  <td>
                    <span className={`pill ${row.predictedDirection === "down" ? "bad" : ""}`}>
                      {row.predictedDirection.toUpperCase()}
                    </span>
                  </td>
                  <td>{(row.confidence * 100).toFixed(1)}%</td>
                  <td className={row.predictedDirection === "down" ? "bad" : "good"}>{pct(row.predictedReturnPct)}</td>
                  <td>{moneyFromCopper(row.currentValue)}</td>
                  <td>{new Date(row.observedAt).toLocaleString()}</td>
                  <td>{row.cooldownActive ? <span className="pill bad">ACTIVE</span> : <span className="small">No</span>}</td>
                  <td style={{ minWidth: 260 }}>{row.reason}</td>
                  <td>{sourceLabel(row.source)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </main>
  );
}
