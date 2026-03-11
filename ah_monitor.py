#!/usr/bin/env python3
import argparse
import json
import math
import os
import sqlite3
import subprocess
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


@dataclass
class Observation:
    observed_at: str
    item_id: int
    item_name: str
    source: str
    metric_name: str
    metric_value: int
    listing_count: int
    total_quantity: int
    min_unit_price: Optional[int]
    max_unit_price: Optional[int]
    avg_unit_price: Optional[int]
    median_unit_price: Optional[int]
    p25_unit_price: Optional[int]
    weighted_avg_unit_price: Optional[int]


@dataclass
class Alert:
    observed_at: str
    item_id: int
    item_name: str
    source: str
    metric_name: str
    current_value: int
    mean_value: float
    stddev_value: float
    z_score: float
    direction: str
    recent_avg_value: float
    history_count: int
    abs_move: int
    alert_kind: str = "price_sigma"
    profession: Optional[str] = None
    recipe_id: Optional[int] = None
    recipe_name: Optional[str] = None
    craft_cost: Optional[int] = None
    sale_value: Optional[int] = None
    expected_profit: Optional[int] = None
    margin_pct: Optional[float] = None
    craft_confidence: Optional[int] = None
    reagent_breakdown: Optional[str] = None


@dataclass
class RecipeReagent:
    item_id: int
    name: str
    quantity: int


@dataclass
class RecipeDefinition:
    recipe_id: int
    recipe_name: str
    profession: str
    crafted_item_id: int
    crafted_item_name: str
    crafted_quantity: int
    reagents: List[RecipeReagent]


@dataclass
class AlertDiagnostics:
    total_rows: int = 0
    blocked_liquidity: int = 0
    blocked_min_history: int = 0
    blocked_zero_stddev: int = 0
    blocked_abs_move: int = 0
    blocked_sigma: int = 0
    blocked_signal_direction: int = 0
    blocked_trend_history: int = 0
    blocked_trend_guard: int = 0


@dataclass
class CraftAlertDiagnostics:
    total_rows: int = 0
    matched_crafted_rows: int = 0
    blocked_output_liquidity: int = 0
    blocked_missing_recipe: int = 0
    blocked_missing_reagent_price: int = 0
    blocked_non_positive_cost: int = 0
    blocked_profit_threshold: int = 0
    blocked_low_confidence: int = 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run AH scrape, store snapshots in SQLite, and alert on 7-day 2-sigma anomalies.")
    p.add_argument("--config", default="config.json", help="Path to scraper config")
    p.add_argument("--report", default="report.json", help="Path for generated report JSON")
    p.add_argument("--db", default="ah_prices.sqlite3", help="SQLite DB path")
    p.add_argument("--database-url", default="", help="Database URL (Supabase/Neon Postgres). Overrides --db")
    p.add_argument(
        "--metric",
        default="weighted_avg_unit_price",
        choices=["min_unit_price", "avg_unit_price", "weighted_avg_unit_price"],
        help="Metric used for anomaly detection",
    )
    p.add_argument("--window-hours", type=int, default=168, help="History window size for baseline (default 168 = 7 days)")
    p.add_argument("--sigma", type=float, default=2.0, help="Sigma threshold for alerting")
    p.add_argument("--min-history", type=int, default=24, help="Minimum historical points before alerting")
    p.add_argument("--trend-hours", type=int, default=48, help="Short trend window in hours for confirmation")
    p.add_argument("--min-trend-history", type=int, default=6, help="Minimum points required in trend window")
    p.add_argument(
        "--buy-recovery-ratio",
        type=float,
        default=0.98,
        help="BUY filter: require current >= recent_avg * ratio (avoids strong downtrends)",
    )
    p.add_argument(
        "--sell-strength-ratio",
        type=float,
        default=1.00,
        help="SELL filter: require current >= recent_avg * ratio",
    )
    p.add_argument(
        "--signal-direction",
        default="both",
        choices=["both", "buy", "sell"],
        help="Filter emitted signals by direction",
    )
    p.add_argument(
        "--min-listings-commodity",
        type=int,
        default=8,
        help="Liquidity filter for commodity sources",
    )
    p.add_argument(
        "--min-quantity-commodity",
        type=int,
        default=200,
        help="Liquidity filter for commodity sources",
    )
    p.add_argument(
        "--min-listings-crafted",
        type=int,
        default=2,
        help="Liquidity filter for non-commodity sources",
    )
    p.add_argument(
        "--min-quantity-crafted",
        type=int,
        default=1,
        help="Liquidity filter for non-commodity sources",
    )
    p.add_argument(
        "--craft-min-listings-output",
        type=int,
        default=5,
        help="Minimum listing count required for crafted outputs in craft arbitrage alerts",
    )
    p.add_argument(
        "--craft-min-quantity-output",
        type=int,
        default=3,
        help="Minimum quantity required for crafted outputs in craft arbitrage alerts",
    )
    p.add_argument(
        "--min-abs-move-gold-commodity",
        type=float,
        default=20.0,
        help="Minimum absolute move vs 7-day mean for commodity alerts (gold)",
    )
    p.add_argument(
        "--min-abs-move-gold-crafted",
        type=float,
        default=100.0,
        help="Minimum absolute move vs 7-day mean for crafted/non-commodity alerts (gold)",
    )
    p.add_argument(
        "--retention-days-observations",
        type=int,
        default=30,
        help="Delete observations older than this many days (0 disables pruning)",
    )
    p.add_argument(
        "--retention-days-alerts",
        type=int,
        default=90,
        help="Delete alerts older than this many days (0 disables pruning)",
    )
    p.add_argument(
        "--enable-craft-alerts",
        action="store_true",
        help="Emit crafting arbitrage BUY/SELL alerts from recipe definitions",
    )
    p.add_argument(
        "--craft-ah-cut-rate",
        type=float,
        default=0.05,
        help="Auction house cut used for craft profit estimates",
    )
    p.add_argument(
        "--craft-min-profit-gold",
        type=float,
        default=50.0,
        help="Minimum profit (gold) for BUY craft alerts and minimum loss for SELL alerts",
    )
    p.add_argument(
        "--craft-min-margin-pct",
        type=float,
        default=0.10,
        help="Minimum absolute margin ratio for crafting arbitrage alerts",
    )
    p.add_argument(
        "--craft-min-confidence",
        type=int,
        default=40,
        help="Minimum confidence score required for crafting arbitrage alerts",
    )
    p.add_argument("--webhook-url", default="", help="Optional webhook URL for alerts")
    p.add_argument(
        "--webhook-format",
        default="slack",
        choices=["slack", "discord"],
        help="Payload format for webhook alerts",
    )
    p.add_argument("--refresh-watchlist", action="store_true", help="Refresh targets file before scraping")
    p.add_argument("--expansion-keyword", default="midnight", help="Expansion keyword for watchlist refresh")
    p.add_argument(
        "--professions",
        default="tailoring,enchanting,inscription,leatherworking",
        help="Comma-separated professions for watchlist refresh",
    )
    p.add_argument("--include-reagents", action="store_true", help="Include reagents while refreshing watchlist")
    p.add_argument("--watchlist-output", default="targets_midnight_tailoring_enchanting.json", help="Watchlist output file")
    p.add_argument("--watchlist-debug-dir", default="", help="Optional directory for watchlist debug artifacts")
    p.add_argument("--ingest-only", action="store_true", help="Skip scraping and just ingest existing report")
    return p.parse_args()


def sqlite_schema_sql() -> str:
    return """
    PRAGMA journal_mode=WAL;

    CREATE TABLE IF NOT EXISTS observations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      observed_at TEXT NOT NULL,
      item_id INTEGER NOT NULL,
      item_name TEXT NOT NULL,
      source TEXT NOT NULL,
      metric_name TEXT NOT NULL,
      metric_value INTEGER NOT NULL,
      listing_count INTEGER NOT NULL,
      total_quantity INTEGER NOT NULL,
      min_unit_price INTEGER,
      max_unit_price INTEGER,
      avg_unit_price INTEGER,
      median_unit_price INTEGER,
      p25_unit_price INTEGER,
      weighted_avg_unit_price INTEGER
    );

    CREATE INDEX IF NOT EXISTS idx_obs_item_source_time
      ON observations(item_id, source, metric_name, observed_at);

    CREATE TABLE IF NOT EXISTS alerts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      alerted_at TEXT NOT NULL,
      observed_at TEXT NOT NULL,
      item_id INTEGER NOT NULL,
      item_name TEXT NOT NULL,
      source TEXT NOT NULL,
      metric_name TEXT NOT NULL,
      current_value INTEGER NOT NULL,
      mean_value REAL NOT NULL,
      stddev_value REAL NOT NULL,
      z_score REAL NOT NULL,
      direction TEXT NOT NULL,
      alert_kind TEXT NOT NULL DEFAULT 'price_sigma',
      profession TEXT,
      recipe_id INTEGER,
      recipe_name TEXT,
      craft_cost INTEGER,
      sale_value INTEGER,
      expected_profit INTEGER,
      margin_pct REAL,
      craft_confidence INTEGER,
      reagent_breakdown TEXT
    );
    """


def postgres_schema_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS observations (
      id BIGSERIAL PRIMARY KEY,
      observed_at TIMESTAMPTZ NOT NULL,
      item_id INTEGER NOT NULL,
      item_name TEXT NOT NULL,
      source TEXT NOT NULL,
      metric_name TEXT NOT NULL,
      metric_value BIGINT NOT NULL,
      listing_count INTEGER NOT NULL,
      total_quantity BIGINT NOT NULL,
      min_unit_price BIGINT,
      max_unit_price BIGINT,
      avg_unit_price BIGINT,
      median_unit_price BIGINT,
      p25_unit_price BIGINT,
      weighted_avg_unit_price BIGINT
    );

    CREATE INDEX IF NOT EXISTS idx_obs_item_source_time
      ON observations(item_id, source, metric_name, observed_at);

    CREATE TABLE IF NOT EXISTS alerts (
      id BIGSERIAL PRIMARY KEY,
      alerted_at TIMESTAMPTZ NOT NULL,
      observed_at TIMESTAMPTZ NOT NULL,
      item_id INTEGER NOT NULL,
      item_name TEXT NOT NULL,
      source TEXT NOT NULL,
      metric_name TEXT NOT NULL,
      current_value BIGINT NOT NULL,
      mean_value DOUBLE PRECISION NOT NULL,
      stddev_value DOUBLE PRECISION NOT NULL,
      z_score DOUBLE PRECISION NOT NULL,
      direction TEXT NOT NULL,
      alert_kind TEXT NOT NULL DEFAULT 'price_sigma',
      profession TEXT,
      recipe_id INTEGER,
      recipe_name TEXT,
      craft_cost BIGINT,
      sale_value BIGINT,
      expected_profit BIGINT,
      margin_pct DOUBLE PRECISION,
      craft_confidence INTEGER,
      reagent_breakdown JSONB
    );
    """


def run_cmd(cmd: Sequence[str]) -> None:
    proc = subprocess.run(cmd, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}")


def refresh_watchlist(args: argparse.Namespace) -> None:
    cmd = [
        sys.executable,
        "build_profession_watchlist.py",
        "--config",
        args.config,
        "--expansion-keyword",
        args.expansion_keyword,
        "--professions",
        args.professions,
        "--output",
        args.watchlist_output,
    ]
    if args.include_reagents:
        cmd.append("--include-reagents")
    if args.watchlist_debug_dir:
        cmd.extend(["--debug-dir", args.watchlist_debug_dir])
    run_cmd(cmd)


def run_scraper(args: argparse.Namespace) -> None:
    run_cmd([sys.executable, "wow_ah_scraper.py", "--config", args.config, "--output", args.report])


def parse_observations(report: Dict[str, Any], metric_name: str, observed_at: str) -> List[Observation]:
    out: List[Observation] = []
    for target in report.get("targets", []):
        item_id = int(target["item_id"])
        item_name = str(target.get("name", f"item-{item_id}"))
        for source_entry in target.get("sources", []):
            source = str(source_entry.get("source", "unknown"))
            summary = source_entry.get("summary")
            if not summary:
                continue
            metric_value = summary.get(metric_name)
            if not isinstance(metric_value, int):
                continue
            out.append(
                Observation(
                    observed_at=observed_at,
                    item_id=item_id,
                    item_name=item_name,
                    source=source,
                    metric_name=metric_name,
                    metric_value=metric_value,
                    listing_count=int(summary.get("listing_count") or 0),
                    total_quantity=int(summary.get("total_quantity") or 0),
                    min_unit_price=summary.get("min_unit_price"),
                    max_unit_price=summary.get("max_unit_price"),
                    avg_unit_price=summary.get("avg_unit_price"),
                    median_unit_price=summary.get("median_unit_price"),
                    p25_unit_price=summary.get("p25_unit_price"),
                    weighted_avg_unit_price=summary.get("weighted_avg_unit_price"),
                )
            )
    return out


def ts_for_db(ts_iso: str) -> str:
    return ts_iso.replace("Z", "+00:00")


class DBClient:
    def init(self) -> None:
        raise NotImplementedError

    def insert_observations(self, rows: List[Observation]) -> None:
        raise NotImplementedError

    def history_values(self, row: Observation, start_iso: str, end_iso: str) -> List[int]:
        raise NotImplementedError

    def insert_alerts(self, alerts: List[Alert], alerted_at: str) -> None:
        raise NotImplementedError

    def prune_old_rows(
        self,
        observations_before_iso: Optional[str],
        alerts_before_iso: Optional[str],
    ) -> Tuple[int, int]:
        raise NotImplementedError

    def commit(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


class SQLiteClient(DBClient):
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path)

    def init(self) -> None:
        self.conn.executescript(sqlite_schema_sql())
        self._migrate_observation_columns_if_needed()
        self._migrate_alert_columns_if_needed()

    def _migrate_observation_columns_if_needed(self) -> None:
        rows = self.conn.execute("PRAGMA table_info(observations)").fetchall()
        existing = {str(r[1]) for r in rows}
        for name, decl in [("median_unit_price", "INTEGER"), ("p25_unit_price", "INTEGER")]:
            if name not in existing:
                self.conn.execute(f"ALTER TABLE observations ADD COLUMN {name} {decl}")

    def _migrate_alert_columns_if_needed(self) -> None:
        rows = self.conn.execute("PRAGMA table_info(alerts)").fetchall()
        existing = {str(r[1]) for r in rows}
        targets = [
            ("alert_kind", "TEXT NOT NULL DEFAULT 'price_sigma'"),
            ("profession", "TEXT"),
            ("recipe_id", "INTEGER"),
            ("recipe_name", "TEXT"),
            ("craft_cost", "INTEGER"),
            ("sale_value", "INTEGER"),
            ("expected_profit", "INTEGER"),
            ("margin_pct", "REAL"),
            ("craft_confidence", "INTEGER"),
            ("reagent_breakdown", "TEXT"),
        ]
        for name, decl in targets:
            if name not in existing:
                self.conn.execute(f"ALTER TABLE alerts ADD COLUMN {name} {decl}")

    def insert_observations(self, rows: List[Observation]) -> None:
        self.conn.executemany(
            """
            INSERT INTO observations (
              observed_at, item_id, item_name, source, metric_name, metric_value,
              listing_count, total_quantity, min_unit_price, max_unit_price,
              avg_unit_price, median_unit_price, p25_unit_price, weighted_avg_unit_price
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    r.observed_at,
                    r.item_id,
                    r.item_name,
                    r.source,
                    r.metric_name,
                    r.metric_value,
                    r.listing_count,
                    r.total_quantity,
                    r.min_unit_price,
                    r.max_unit_price,
                    r.avg_unit_price,
                    r.median_unit_price,
                    r.p25_unit_price,
                    r.weighted_avg_unit_price,
                )
                for r in rows
            ],
        )

    def history_values(self, row: Observation, start_iso: str, end_iso: str) -> List[int]:
        rows = self.conn.execute(
            """
            SELECT metric_value
            FROM observations
            WHERE item_id = ?
              AND source = ?
              AND metric_name = ?
              AND observed_at >= ?
              AND observed_at < ?
            ORDER BY observed_at ASC
            """,
            (row.item_id, row.source, row.metric_name, start_iso, end_iso),
        ).fetchall()
        return [int(v[0]) for v in rows]

    def insert_alerts(self, alerts: List[Alert], alerted_at: str) -> None:
        self.conn.executemany(
            """
                INSERT INTO alerts (
                  alerted_at, observed_at, item_id, item_name, source, metric_name,
                  current_value, mean_value, stddev_value, z_score, direction,
                  alert_kind, profession, recipe_id, recipe_name, craft_cost, sale_value, expected_profit, margin_pct,
                  craft_confidence, reagent_breakdown
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    alerted_at,
                    a.observed_at,
                    a.item_id,
                    a.item_name,
                    a.source,
                    a.metric_name,
                    a.current_value,
                    a.mean_value,
                    a.stddev_value,
                    a.z_score,
                    a.direction,
                    a.alert_kind,
                    a.profession,
                    a.recipe_id,
                    a.recipe_name,
                    a.craft_cost,
                    a.sale_value,
                    a.expected_profit,
                    a.margin_pct,
                    a.craft_confidence,
                    a.reagent_breakdown,
                )
                for a in alerts
            ],
        )

    def prune_old_rows(
        self,
        observations_before_iso: Optional[str],
        alerts_before_iso: Optional[str],
    ) -> Tuple[int, int]:
        deleted_observations = 0
        deleted_alerts = 0
        if observations_before_iso:
            cur = self.conn.execute(
                "DELETE FROM observations WHERE observed_at < ?",
                (observations_before_iso,),
            )
            deleted_observations = max(int(cur.rowcount), 0)
        if alerts_before_iso:
            cur = self.conn.execute(
                "DELETE FROM alerts WHERE alerted_at < ?",
                (alerts_before_iso,),
            )
            deleted_alerts = max(int(cur.rowcount), 0)
        return deleted_observations, deleted_alerts

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


class PostgresClient(DBClient):
    def __init__(self, url: str):
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError("Postgres backend requires 'psycopg'. Install with: pip install psycopg[binary]") from exc
        self._psycopg = psycopg
        self.conn = psycopg.connect(url, prepare_threshold=None)
        
    def init(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(postgres_schema_sql())
        self._migrate_observation_columns_if_needed()
        self._migrate_int_to_bigint_if_needed()
        self._migrate_alert_columns_if_needed()

    def _migrate_observation_columns_if_needed(self) -> None:
        targets = [
            ("median_unit_price", "BIGINT"),
            ("p25_unit_price", "BIGINT"),
        ]
        with self.conn.cursor() as cur:
            for column_name, ddl in targets:
                cur.execute(
                    """
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'observations'
                      AND column_name = %s
                    """,
                    (column_name,),
                )
                row = cur.fetchone()
                if not row:
                    cur.execute(f"ALTER TABLE observations ADD COLUMN {column_name} {ddl}")

    def _migrate_int_to_bigint_if_needed(self) -> None:
        targets = [
            ("observations", "metric_value"),
            ("observations", "total_quantity"),
            ("observations", "min_unit_price"),
            ("observations", "max_unit_price"),
            ("observations", "avg_unit_price"),
            ("observations", "median_unit_price"),
            ("observations", "p25_unit_price"),
            ("observations", "weighted_avg_unit_price"),
            ("alerts", "current_value"),
        ]
        with self.conn.cursor() as cur:
            for table_name, column_name in targets:
                cur.execute(
                    """
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = %s
                      AND column_name = %s
                    """,
                    (table_name, column_name),
                )
                row = cur.fetchone()
                if not row:
                    continue
                if row[0] == "integer":
                    cur.execute(
                        f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE BIGINT USING {column_name}::BIGINT"
                    )

    def _migrate_alert_columns_if_needed(self) -> None:
        targets = [
            ("alert_kind", "TEXT NOT NULL DEFAULT 'price_sigma'"),
            ("profession", "TEXT"),
            ("recipe_id", "INTEGER"),
            ("recipe_name", "TEXT"),
            ("craft_cost", "BIGINT"),
            ("sale_value", "BIGINT"),
            ("expected_profit", "BIGINT"),
            ("margin_pct", "DOUBLE PRECISION"),
            ("craft_confidence", "INTEGER"),
            ("reagent_breakdown", "JSONB"),
        ]
        with self.conn.cursor() as cur:
            for column_name, ddl in targets:
                cur.execute(
                    """
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'alerts'
                      AND column_name = %s
                    """,
                    (column_name,),
                )
                row = cur.fetchone()
                if not row:
                    cur.execute(f"ALTER TABLE alerts ADD COLUMN {column_name} {ddl}")

    def insert_observations(self, rows: List[Observation]) -> None:
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO observations (
                  observed_at, item_id, item_name, source, metric_name, metric_value,
                  listing_count, total_quantity, min_unit_price, max_unit_price,
                  avg_unit_price, median_unit_price, p25_unit_price, weighted_avg_unit_price
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        ts_for_db(r.observed_at),
                        r.item_id,
                        r.item_name,
                        r.source,
                        r.metric_name,
                        r.metric_value,
                        r.listing_count,
                        r.total_quantity,
                        r.min_unit_price,
                        r.max_unit_price,
                        r.avg_unit_price,
                        r.median_unit_price,
                        r.p25_unit_price,
                        r.weighted_avg_unit_price,
                    )
                    for r in rows
                ],
            )

    def history_values(self, row: Observation, start_iso: str, end_iso: str) -> List[int]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT metric_value
                FROM observations
                WHERE item_id = %s
                  AND source = %s
                  AND metric_name = %s
                  AND observed_at >= %s
                  AND observed_at < %s
                ORDER BY observed_at ASC
                """,
                (row.item_id, row.source, row.metric_name, ts_for_db(start_iso), ts_for_db(end_iso)),
            )
            rows = cur.fetchall()
        return [int(v[0]) for v in rows]

    def insert_alerts(self, alerts: List[Alert], alerted_at: str) -> None:
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO alerts (
                  alerted_at, observed_at, item_id, item_name, source, metric_name,
                  current_value, mean_value, stddev_value, z_score, direction,
                  alert_kind, profession, recipe_id, recipe_name, craft_cost, sale_value, expected_profit, margin_pct,
                  craft_confidence, reagent_breakdown
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        ts_for_db(alerted_at),
                        ts_for_db(a.observed_at),
                        a.item_id,
                        a.item_name,
                        a.source,
                        a.metric_name,
                        a.current_value,
                        a.mean_value,
                        a.stddev_value,
                        a.z_score,
                        a.direction,
                        a.alert_kind,
                        a.profession,
                        a.recipe_id,
                        a.recipe_name,
                        a.craft_cost,
                        a.sale_value,
                        a.expected_profit,
                        a.margin_pct,
                        a.craft_confidence,
                        self._psycopg.types.json.Jsonb(json.loads(a.reagent_breakdown)) if a.reagent_breakdown else None,
                    )
                    for a in alerts
                ],
            )

    def prune_old_rows(
        self,
        observations_before_iso: Optional[str],
        alerts_before_iso: Optional[str],
    ) -> Tuple[int, int]:
        deleted_observations = 0
        deleted_alerts = 0
        with self.conn.cursor() as cur:
            if observations_before_iso:
                cur.execute(
                    "DELETE FROM observations WHERE observed_at < %s",
                    (ts_for_db(observations_before_iso),),
                )
                deleted_observations = max(int(cur.rowcount), 0)
            if alerts_before_iso:
                cur.execute(
                    "DELETE FROM alerts WHERE alerted_at < %s",
                    (ts_for_db(alerts_before_iso),),
                )
                deleted_alerts = max(int(cur.rowcount), 0)
        return deleted_observations, deleted_alerts

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


def mean_stddev(values: List[int]) -> Tuple[float, float]:
    n = len(values)
    mean = float(sum(values)) / float(n)
    var = sum((v - mean) ** 2 for v in values) / float(n)
    return mean, math.sqrt(var)


def format_money_copper(value_copper: int) -> str:
    gold = value_copper // 10000
    remainder = value_copper % 10000
    silver = remainder // 100
    copper = remainder % 100
    return f"{gold}g {silver}s {copper}c"


def is_commodity_source(source: str) -> bool:
    return source.startswith("commodity:")


def min_abs_move_copper(row: Observation, args: argparse.Namespace) -> int:
    if is_commodity_source(row.source):
        return int(args.min_abs_move_gold_commodity * 10000)
    return int(args.min_abs_move_gold_crafted * 10000)


def passes_liquidity(row: Observation, args: argparse.Namespace) -> bool:
    if is_commodity_source(row.source):
        return row.listing_count >= args.min_listings_commodity and row.total_quantity >= args.min_quantity_commodity
    return row.listing_count >= args.min_listings_crafted and row.total_quantity >= args.min_quantity_crafted


def passes_craft_output_liquidity(row: Observation, args: argparse.Namespace) -> bool:
    if is_commodity_source(row.source):
        return passes_liquidity(row, args)
    return row.listing_count >= args.craft_min_listings_output and row.total_quantity >= args.craft_min_quantity_output


def conservative_craft_sale_unit_price(row: Observation) -> Optional[int]:
    if isinstance(row.p25_unit_price, int) and row.p25_unit_price > 0:
        return row.p25_unit_price
    if isinstance(row.median_unit_price, int) and row.median_unit_price > 0 and isinstance(row.avg_unit_price, int) and row.avg_unit_price > 0:
        return min(row.median_unit_price, row.avg_unit_price)
    if isinstance(row.median_unit_price, int) and row.median_unit_price > 0:
        return row.median_unit_price
    if isinstance(row.avg_unit_price, int) and row.avg_unit_price > 0:
        return row.avg_unit_price
    if isinstance(row.weighted_avg_unit_price, int) and row.weighted_avg_unit_price > 0:
        return row.weighted_avg_unit_price
    if isinstance(row.min_unit_price, int) and row.min_unit_price > 0:
        return row.min_unit_price
    if row.metric_value > 0:
        return row.metric_value
    return None


def clamp_ratio(numerator: Optional[int], denominator: Optional[int]) -> Optional[float]:
    if not isinstance(numerator, int) or not isinstance(denominator, int) or numerator <= 0 or denominator <= 0:
        return None
    return min(float(numerator), float(denominator)) / max(float(numerator), float(denominator))


def craft_confidence_score(
    row: Observation,
    sale_unit_price: int,
    margin_pct: float,
    recent_sale_avg: Optional[float],
    args: argparse.Namespace,
) -> int:
    listing_score = min(float(row.listing_count) / float(max(args.craft_min_listings_output * 2, 1)), 1.0)
    quantity_score = min(float(row.total_quantity) / float(max(args.craft_min_quantity_output * 3, 1)), 1.0)
    spread_parts = [
        clamp_ratio(row.p25_unit_price, row.median_unit_price),
        clamp_ratio(row.median_unit_price, row.avg_unit_price),
        clamp_ratio(row.min_unit_price, row.p25_unit_price),
    ]
    spread_values = [v for v in spread_parts if v is not None]
    spread_score = sum(spread_values) / float(len(spread_values)) if spread_values else 0.5
    recent_score = 0.5
    if isinstance(recent_sale_avg, float) and recent_sale_avg > 0:
        recent_score = min(float(sale_unit_price), recent_sale_avg) / max(float(sale_unit_price), recent_sale_avg)
    confidence = int(round(100.0 * ((listing_score * 0.30) + (quantity_score * 0.25) + (spread_score * 0.20) + (recent_score * 0.25))))
    penalty = 0
    if margin_pct >= 1.5:
        penalty += min(int((margin_pct - 1.5) * 8.0), 20)
    if margin_pct >= 3.0 and (row.listing_count < 10 or row.total_quantity < 8):
        penalty += 20
    if isinstance(recent_sale_avg, float) and recent_sale_avg > 0:
        if sale_unit_price > recent_sale_avg * 1.5:
            penalty += 15
        if sale_unit_price > recent_sale_avg * 2.0:
            penalty += 15
    confidence -= penalty
    return max(0, min(confidence, 100))


def detect_alerts(
    db: DBClient,
    rows: List[Observation],
    args: argparse.Namespace,
) -> Tuple[List[Alert], AlertDiagnostics]:
    alerts: List[Alert] = []
    diagnostics = AlertDiagnostics(total_rows=len(rows))
    for row in rows:
        if not passes_liquidity(row, args):
            diagnostics.blocked_liquidity += 1
            continue

        current_ts = datetime.fromisoformat(row.observed_at.replace("Z", "+00:00"))
        start_ts = current_ts - timedelta(hours=args.window_hours)

        history = db.history_values(
            row=row,
            start_iso=start_ts.isoformat().replace("+00:00", "Z"),
            end_iso=row.observed_at,
        )
        if len(history) < args.min_history:
            diagnostics.blocked_min_history += 1
            continue

        mean, stddev = mean_stddev(history)
        if stddev <= 0:
            diagnostics.blocked_zero_stddev += 1
            continue

        delta = row.metric_value - mean
        if abs(delta) < min_abs_move_copper(row, args):
            diagnostics.blocked_abs_move += 1
            continue
        z = delta / stddev
        if abs(z) < args.sigma:
            diagnostics.blocked_sigma += 1
            continue

        direction = "below_mean" if z < 0 else "above_mean"
        if args.signal_direction == "buy" and direction != "below_mean":
            diagnostics.blocked_signal_direction += 1
            continue
        if args.signal_direction == "sell" and direction != "above_mean":
            diagnostics.blocked_signal_direction += 1
            continue
        trend_start_ts = current_ts - timedelta(hours=args.trend_hours)
        trend_values = db.history_values(
            row=row,
            start_iso=trend_start_ts.isoformat().replace("+00:00", "Z"),
            end_iso=row.observed_at,
        )
        if len(trend_values) < args.min_trend_history:
            diagnostics.blocked_trend_history += 1
            continue
        recent_avg = float(sum(trend_values)) / float(len(trend_values))

        if direction == "below_mean":
            # Avoid "falling knife" entries unless price is stabilizing vs recent trend.
            if row.metric_value < recent_avg * args.buy_recovery_ratio:
                diagnostics.blocked_trend_guard += 1
                continue
        else:
            # For exits, require current price to still show strength vs recent trend.
            if row.metric_value < recent_avg * args.sell_strength_ratio:
                diagnostics.blocked_trend_guard += 1
                continue

        alerts.append(
            Alert(
                observed_at=row.observed_at,
                item_id=row.item_id,
                item_name=row.item_name,
                source=row.source,
                metric_name=row.metric_name,
                current_value=row.metric_value,
                mean_value=mean,
                stddev_value=stddev,
                z_score=z,
                direction=direction,
                recent_avg_value=recent_avg,
                history_count=len(history),
                abs_move=abs(delta),
            )
        )
    return alerts, diagnostics


def load_recipe_definitions(config_path: Path) -> List[RecipeDefinition]:
    if not config_path.exists():
        return []
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    target_files: List[str] = []
    one = cfg.get("targets_file")
    if isinstance(one, str) and one.strip():
        target_files.append(one)
    many = cfg.get("targets_files")
    if isinstance(many, list):
        for v in many:
            if isinstance(v, str) and v.strip():
                target_files.append(v)
    out: List[RecipeDefinition] = []
    seen_recipe_ids: set[int] = set()
    for rel in target_files:
        path = (config_path.parent / rel).resolve()
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        recipes = payload.get("recipes", [])
        if not isinstance(recipes, list):
            continue
        for raw in recipes:
            if not isinstance(raw, dict):
                continue
            recipe_id = raw.get("recipe_id")
            crafted_item_id = raw.get("crafted_item_id")
            reagents_raw = raw.get("reagents")
            if not isinstance(recipe_id, int) or not isinstance(crafted_item_id, int) or not isinstance(reagents_raw, list):
                continue
            if recipe_id in seen_recipe_ids:
                continue
            reagents: List[RecipeReagent] = []
            for r in reagents_raw:
                if not isinstance(r, dict):
                    continue
                item_id = r.get("item_id")
                quantity = r.get("quantity")
                if not isinstance(item_id, int) or not isinstance(quantity, int) or quantity <= 0:
                    continue
                reagents.append(RecipeReagent(item_id=item_id, name=str(r.get("name", f"item-{item_id}")), quantity=quantity))
            if not reagents:
                continue
            out.append(
                RecipeDefinition(
                    recipe_id=recipe_id,
                    recipe_name=str(raw.get("recipe_name", f"recipe-{recipe_id}")),
                    profession=str(raw.get("profession", "unknown")),
                    crafted_item_id=crafted_item_id,
                    crafted_item_name=str(raw.get("crafted_item_name", f"item-{crafted_item_id}")),
                    crafted_quantity=max(int(raw.get("crafted_quantity", 1) or 1), 1),
                    reagents=reagents,
                )
            )
            seen_recipe_ids.add(recipe_id)
    return out


def pick_market_row(candidates: List[Observation], preferred_source: str) -> Optional[Observation]:
    if not candidates:
        return None
    by_source = {r.source: r for r in candidates}
    if preferred_source in by_source:
        return by_source[preferred_source]
    commodity = [r for r in candidates if is_commodity_source(r.source)]
    if commodity:
        return max(commodity, key=lambda r: (r.listing_count, r.total_quantity))
    return max(candidates, key=lambda r: (r.listing_count, r.total_quantity))


def detect_craft_alerts(
    db: DBClient,
    rows: List[Observation],
    recipes: List[RecipeDefinition],
    args: argparse.Namespace,
) -> Tuple[List[Alert], CraftAlertDiagnostics]:
    diagnostics = CraftAlertDiagnostics(total_rows=len(rows))
    if not recipes:
        diagnostics.blocked_missing_recipe = len(rows)
        return [], diagnostics
    rows_by_item: Dict[int, List[Observation]] = defaultdict(list)
    for row in rows:
        rows_by_item[row.item_id].append(row)
    recipes_by_output: Dict[int, List[RecipeDefinition]] = defaultdict(list)
    for recipe in recipes:
        recipes_by_output[recipe.crafted_item_id].append(recipe)

    min_profit_copper = int(args.craft_min_profit_gold * 10000)
    alerts: List[Alert] = []
    for crafted_item_id, crafted_rows in rows_by_item.items():
        recipe_list = recipes_by_output.get(crafted_item_id)
        if not recipe_list:
            diagnostics.blocked_missing_recipe += len(crafted_rows)
            continue
        for crafted_row in crafted_rows:
            diagnostics.matched_crafted_rows += 1
            if not passes_craft_output_liquidity(crafted_row, args):
                diagnostics.blocked_output_liquidity += 1
                continue
            for recipe in recipe_list:
                total_craft_cost = 0
                missing_price = False
                reagent_breakdown_rows: List[Dict[str, Any]] = []
                for reagent in recipe.reagents:
                    reagent_row = pick_market_row(rows_by_item.get(reagent.item_id, []), crafted_row.source)
                    if not reagent_row or not passes_liquidity(reagent_row, args):
                        missing_price = True
                        break
                    reagent_total_cost = reagent.quantity * reagent_row.metric_value
                    total_craft_cost += reagent_total_cost
                    reagent_breakdown_rows.append(
                        {
                            "item_id": reagent.item_id,
                            "name": reagent.name,
                            "quantity": reagent.quantity,
                            "unit_price": reagent_row.metric_value,
                            "total_cost": reagent_total_cost,
                            "source": reagent_row.source,
                        }
                    )
                if missing_price:
                    diagnostics.blocked_missing_reagent_price += 1
                    continue
                if total_craft_cost <= 0:
                    diagnostics.blocked_non_positive_cost += 1
                    continue
                sale_unit_price = conservative_craft_sale_unit_price(crafted_row)
                if not sale_unit_price or sale_unit_price <= 0:
                    diagnostics.blocked_profit_threshold += 1
                    continue
                sale_value = sale_unit_price * recipe.crafted_quantity
                net_sale = int(sale_value * (1.0 - args.craft_ah_cut_rate))
                expected_profit = net_sale - total_craft_cost
                margin_pct = expected_profit / float(total_craft_cost)
                if expected_profit >= min_profit_copper and margin_pct >= args.craft_min_margin_pct:
                    direction = "buy"
                else:
                    diagnostics.blocked_profit_threshold += 1
                    continue
                current_ts = datetime.fromisoformat(crafted_row.observed_at.replace("Z", "+00:00"))
                recent_history = db.history_values(
                    row=crafted_row,
                    start_iso=(current_ts - timedelta(hours=args.trend_hours)).isoformat().replace("+00:00", "Z"),
                    end_iso=crafted_row.observed_at,
                )
                recent_sale_avg = float(sum(recent_history)) / float(len(recent_history)) if recent_history else None
                confidence = craft_confidence_score(crafted_row, sale_unit_price, margin_pct, recent_sale_avg, args)
                if confidence < args.craft_min_confidence:
                    diagnostics.blocked_low_confidence += 1
                    continue
                alerts.append(
                    Alert(
                        observed_at=crafted_row.observed_at,
                        item_id=crafted_row.item_id,
                        item_name=crafted_row.item_name,
                        source=crafted_row.source,
                        metric_name="craft_profit",
                        current_value=sale_unit_price,
                        mean_value=0.0,
                        stddev_value=0.0,
                        z_score=0.0,
                        direction=direction,
                        recent_avg_value=0.0,
                        history_count=0,
                        abs_move=abs(expected_profit),
                        alert_kind="craft_arbitrage",
                        profession=recipe.profession,
                        recipe_id=recipe.recipe_id,
                        recipe_name=recipe.recipe_name,
                        craft_cost=total_craft_cost,
                        sale_value=sale_value,
                        expected_profit=expected_profit,
                        margin_pct=margin_pct,
                        craft_confidence=confidence,
                        reagent_breakdown=json.dumps(reagent_breakdown_rows),
                    )
                )
    return alerts, diagnostics


def format_alert_diagnostics(diag: AlertDiagnostics) -> str:
    return (
        "Sigma diagnostics: "
        f"rows={diag.total_rows}, "
        f"liquidity={diag.blocked_liquidity}, "
        f"min_history={diag.blocked_min_history}, "
        f"zero_stddev={diag.blocked_zero_stddev}, "
        f"abs_move={diag.blocked_abs_move}, "
        f"sigma={diag.blocked_sigma}, "
        f"signal_direction={diag.blocked_signal_direction}, "
        f"trend_history={diag.blocked_trend_history}, "
        f"trend_guard={diag.blocked_trend_guard}"
    )


def format_craft_alert_diagnostics(diag: CraftAlertDiagnostics, recipe_count: int) -> str:
    return (
        "Craft diagnostics: "
        f"rows={diag.total_rows}, "
        f"recipes={recipe_count}, "
        f"matched_outputs={diag.matched_crafted_rows}, "
        f"missing_recipe={diag.blocked_missing_recipe}, "
        f"output_liquidity={diag.blocked_output_liquidity}, "
        f"missing_reagent_price={diag.blocked_missing_reagent_price}, "
        f"non_positive_cost={diag.blocked_non_positive_cost}, "
        f"profit_threshold={diag.blocked_profit_threshold}, "
        f"low_confidence={diag.blocked_low_confidence}"
    )


def craft_action_label(direction: str) -> str:
    if direction == "buy":
        return "CRAFT"
    return direction.upper()


def format_alert_message(alerts: List[Alert], sigma: float, window_hours: int, craft_ah_cut_rate: float) -> str:
    sigma_alerts = [a for a in alerts if a.alert_kind == "price_sigma"]
    craft_alerts = [a for a in alerts if a.alert_kind == "craft_arbitrage"]
    buys = [a for a in sigma_alerts if a.direction == "below_mean"] + [a for a in craft_alerts if a.direction == "buy"]
    sells = [a for a in sigma_alerts if a.direction == "above_mean"] + [a for a in craft_alerts if a.direction == "sell"]
    lines = [f"WoW AH alerts: {len(alerts)} total (sigma={len(sigma_alerts)}, craft={len(craft_alerts)})"]
    lines.append(f"BUY: {len([a for a in sigma_alerts if a.direction == 'below_mean'])} | SELL: {len([a for a in sigma_alerts if a.direction == 'above_mean'])}")
    lines.append(f"CRAFT: {len(craft_alerts)}")

    sigma_buys = [a for a in sigma_alerts if a.direction == "below_mean"]
    if sigma_buys:
        lines.append("BUY signals:")
        for a in sigma_buys[:10]:
            lines.append(
                f"- {a.item_name} [{a.item_id}] {a.source}: {format_money_copper(a.current_value)} vs mean {format_money_copper(int(a.mean_value))} ({a.z_score:+.2f} sigma, n={a.history_count})"
            )

    sigma_sells = [a for a in sigma_alerts if a.direction == "above_mean"]
    if sigma_sells:
        lines.append("SELL signals:")
        for a in sigma_sells[:10]:
            lines.append(
                f"- {a.item_name} [{a.item_id}] {a.source}: {format_money_copper(a.current_value)} vs mean {format_money_copper(int(a.mean_value))} ({a.z_score:+.2f} sigma, n={a.history_count})"
            )

    if craft_alerts:
        lines.append("Crafting arbitrage:")
        for a in craft_alerts[:10]:
            profit = a.expected_profit or 0
            margin = (a.margin_pct or 0.0) * 100.0
            net_sale = int((a.sale_value or 0) * (1.0 - craft_ah_cut_rate))
            lines.append(
                f"- {craft_action_label(a.direction)} {a.item_name} [{a.item_id}] {a.recipe_name or f'r#{a.recipe_id}'}: "
                f"net sale {format_money_copper(net_sale)} vs mat cost {format_money_copper(a.craft_cost or 0)} "
                f"(profit {format_money_copper(profit)}, margin {margin:+.1f}%, confidence {a.craft_confidence or 0})"
            )

    if len(alerts) > 20:
        lines.append(f"... plus {len(alerts) - 20} more")
    return "\n".join(lines)


def send_webhook(url: str, message: str, fmt: str) -> None:
    if fmt == "discord":
        payload = {"content": message}
    else:
        payload = {"text": message}
    body = json.dumps(payload).encode("utf-8")
    
    # Add User-Agent header to avoid 403 Forbidden
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)" 
    }
    
    req = urllib.request.Request(url, data=body, method="POST", headers=headers)
    with urllib.request.urlopen(req, timeout=20) as resp:
        if resp.status >= 300:
            raise RuntimeError(f"Webhook failed with status {resp.status}")


def pick_db_client(args: argparse.Namespace) -> Tuple[DBClient, str]:
    db_url = args.database_url.strip() or os.environ.get("DATABASE_URL", "").strip()
    if not db_url:
        return SQLiteClient(args.db), f"sqlite:{args.db}"

    scheme = urllib.parse.urlparse(db_url).scheme.lower()
    if scheme in {"postgres", "postgresql"}:
        # Supabase pooler URLs can include "pgbouncer=true", which psycopg
        # doesn't accept as a valid libpq connection parameter in URI form.
        parsed = urllib.parse.urlparse(db_url)
        query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        filtered = [(k, v) for (k, v) in query if k.lower() != "pgbouncer"]
        cleaned_query = urllib.parse.urlencode(filtered)
        db_url = urllib.parse.urlunparse(
            (parsed.scheme, parsed.netloc, parsed.path, parsed.params, cleaned_query, parsed.fragment)
        )
        return PostgresClient(db_url), "postgres"
    if scheme in {"sqlite", "file"}:
        return SQLiteClient(args.db), f"sqlite:{args.db}"
    raise ValueError(f"Unsupported DATABASE_URL scheme '{scheme}'. Use postgres/postgresql for Supabase.")


def main() -> int:
    args = parse_args()
    webhook_url = args.webhook_url.strip() or os.environ.get("AH_ALERT_WEBHOOK_URL", "").strip()
    recipe_defs = load_recipe_definitions(Path(args.config)) if args.enable_craft_alerts else []

    if args.refresh_watchlist:
        refresh_watchlist(args)

    if not args.ingest_only:
        run_scraper(args)

    report_path = Path(args.report)
    if not report_path.exists():
        raise FileNotFoundError(f"Report file not found: {report_path}")

    report = json.loads(report_path.read_text(encoding="utf-8"))
    observed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    rows = parse_observations(report, args.metric, observed_at)
    if not rows:
        print("No usable observations in report; nothing inserted.")
        return 0

    db, db_label = pick_db_client(args)
    try:
        db.init()
        db.insert_observations(rows)
        sigma_alerts, sigma_diag = detect_alerts(
            db=db,
            rows=rows,
            args=args,
        )
        if args.enable_craft_alerts:
            craft_alerts, craft_diag = detect_craft_alerts(db=db, rows=rows, recipes=recipe_defs, args=args)
        else:
            craft_alerts, craft_diag = [], CraftAlertDiagnostics(total_rows=len(rows))
        alerts = sigma_alerts + craft_alerts
        alerted_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if alerts:
            db.insert_alerts(alerts, alerted_at)
        now_utc = datetime.now(timezone.utc).replace(microsecond=0)
        observations_before_iso = None
        alerts_before_iso = None
        if args.retention_days_observations > 0:
            observations_before_iso = (now_utc - timedelta(days=args.retention_days_observations)).isoformat().replace(
                "+00:00", "Z"
            )
        if args.retention_days_alerts > 0:
            alerts_before_iso = (now_utc - timedelta(days=args.retention_days_alerts)).isoformat().replace(
                "+00:00", "Z"
            )
        deleted_observations, deleted_alerts = db.prune_old_rows(observations_before_iso, alerts_before_iso)
        db.commit()
    finally:
        db.close()

    print(f"Inserted {len(rows)} observations into {db_label} at {observed_at}")
    print(format_alert_diagnostics(sigma_diag))
    if args.enable_craft_alerts:
        print(f"Craft recipe definitions loaded: {len(recipe_defs)}")
        print(format_craft_alert_diagnostics(craft_diag, len(recipe_defs)))
    if observations_before_iso or alerts_before_iso:
        print(
            "Retention prune:"
            f" deleted {deleted_observations} observation(s)"
            f" and {deleted_alerts} alert(s)."
        )

    if not alerts:
        print("No alerts this run.")
        return 0

    message = format_alert_message(alerts, args.sigma, args.window_hours, args.craft_ah_cut_rate)
    print(message)

    if webhook_url:
        send_webhook(webhook_url, message, args.webhook_format)
        print(f"Sent alert webhook ({args.webhook_format}).")
    else:
        print("No webhook configured; alerts printed to stdout only.")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
