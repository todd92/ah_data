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
    p.add_argument("--webhook-url", default="", help="Optional webhook URL for alerts")
    p.add_argument(
        "--webhook-format",
        default="slack",
        choices=["slack", "discord"],
        help="Payload format for webhook alerts",
    )
    p.add_argument("--refresh-watchlist", action="store_true", help="Refresh targets file before scraping")
    p.add_argument("--expansion-keyword", default="midnight", help="Expansion keyword for watchlist refresh")
    p.add_argument("--professions", default="tailoring,enchanting", help="Comma-separated professions for watchlist refresh")
    p.add_argument("--include-reagents", action="store_true", help="Include reagents while refreshing watchlist")
    p.add_argument("--watchlist-output", default="targets_midnight_tailoring_enchanting.json", help="Watchlist output file")
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
      direction TEXT NOT NULL
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
      direction TEXT NOT NULL
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

    def insert_observations(self, rows: List[Observation]) -> None:
        self.conn.executemany(
            """
            INSERT INTO observations (
              observed_at, item_id, item_name, source, metric_name, metric_value,
              listing_count, total_quantity, min_unit_price, max_unit_price,
              avg_unit_price, weighted_avg_unit_price
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
              current_value, mean_value, stddev_value, z_score, direction
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                )
                for a in alerts
            ],
        )

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
        self.conn = psycopg.connect(url)

    def init(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(postgres_schema_sql())
        self._migrate_int_to_bigint_if_needed()

    def _migrate_int_to_bigint_if_needed(self) -> None:
        targets = [
            ("observations", "metric_value"),
            ("observations", "total_quantity"),
            ("observations", "min_unit_price"),
            ("observations", "max_unit_price"),
            ("observations", "avg_unit_price"),
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

    def insert_observations(self, rows: List[Observation]) -> None:
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO observations (
                  observed_at, item_id, item_name, source, metric_name, metric_value,
                  listing_count, total_quantity, min_unit_price, max_unit_price,
                  avg_unit_price, weighted_avg_unit_price
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                  current_value, mean_value, stddev_value, z_score, direction
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    )
                    for a in alerts
                ],
            )

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


def mean_stddev(values: List[int]) -> Tuple[float, float]:
    n = len(values)
    mean = float(sum(values)) / float(n)
    var = sum((v - mean) ** 2 for v in values) / float(n)
    return mean, math.sqrt(var)


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


def detect_alerts(
    db: DBClient,
    rows: List[Observation],
    args: argparse.Namespace,
) -> List[Alert]:
    alerts: List[Alert] = []
    for row in rows:
        if not passes_liquidity(row, args):
            continue

        current_ts = datetime.fromisoformat(row.observed_at.replace("Z", "+00:00"))
        start_ts = current_ts - timedelta(hours=args.window_hours)

        history = db.history_values(
            row=row,
            start_iso=start_ts.isoformat().replace("+00:00", "Z"),
            end_iso=row.observed_at,
        )
        if len(history) < args.min_history:
            continue

        mean, stddev = mean_stddev(history)
        if stddev <= 0:
            continue

        delta = row.metric_value - mean
        if abs(delta) < min_abs_move_copper(row, args):
            continue
        z = delta / stddev
        if abs(z) < args.sigma:
            continue

        direction = "below_mean" if z < 0 else "above_mean"
        trend_start_ts = current_ts - timedelta(hours=args.trend_hours)
        trend_values = db.history_values(
            row=row,
            start_iso=trend_start_ts.isoformat().replace("+00:00", "Z"),
            end_iso=row.observed_at,
        )
        if len(trend_values) < args.min_trend_history:
            continue
        recent_avg = float(sum(trend_values)) / float(len(trend_values))

        if direction == "below_mean":
            # Avoid "falling knife" entries unless price is stabilizing vs recent trend.
            if row.metric_value < recent_avg * args.buy_recovery_ratio:
                continue
        else:
            # For exits, require current price to still show strength vs recent trend.
            if row.metric_value < recent_avg * args.sell_strength_ratio:
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
    return alerts


def format_alert_message(alerts: List[Alert], sigma: float, window_hours: int) -> str:
    lines = [
        f"WoW AH alerts: {len(alerts)} item(s) beyond {sigma:.2f} sigma vs last {window_hours}h",
    ]
    for a in alerts[:30]:
        direction = "BUY signal" if a.direction == "below_mean" else "SELL signal"
        lines.append(
            f"- {a.item_name} [{a.item_id}] {a.source}: {a.current_value} cp vs mean {int(a.mean_value)} cp ({a.z_score:+.2f} sigma, n={a.history_count}) {direction}"
        )
    if len(alerts) > 30:
        lines.append(f"... plus {len(alerts) - 30} more")
    return "\n".join(lines)


def send_webhook(url: str, message: str, fmt: str) -> None:
    if fmt == "discord":
        payload = {"content": message}
    else:
        payload = {"text": message}
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST", headers={"Content-Type": "application/json"})
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
        alerts = detect_alerts(
            db=db,
            rows=rows,
            args=args,
        )
        alerted_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if alerts:
            db.insert_alerts(alerts, alerted_at)
        db.commit()
    finally:
        db.close()

    print(f"Inserted {len(rows)} observations into {db_label} at {observed_at}")

    if not alerts:
        print("No sigma alerts this run.")
        return 0

    message = format_alert_message(alerts, args.sigma, args.window_hours)
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
