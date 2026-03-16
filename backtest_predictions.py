#!/usr/bin/env python3
import argparse
import sqlite3
from argparse import Namespace
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from ah_monitor import Observation, PredictionCooldown, SQLiteClient, detect_predictions


@dataclass
class PendingPrediction:
    observed_at: str
    resolve_start_at: str
    resolve_end_at: str
    item_id: int
    item_name: str
    source: str
    metric_name: str
    current_value: int
    predicted_direction: str
    confidence: float
    predicted_return_pct: float
    resolved: bool = False


class ReplayDB(SQLiteClient):
    def __init__(self, path: str):
        super().__init__(path)
        self.cooldowns: List[PredictionCooldown] = []

    def active_prediction_cooldown(
        self,
        row: Observation,
        predicted_direction: str,
        observed_at: str,
    ) -> Optional[PredictionCooldown]:
        current_ts = parse_ts(observed_at)
        hits = [
            c
            for c in self.cooldowns
            if c.item_id == row.item_id
            and c.source == row.source
            and c.metric_name == row.metric_name
            and c.predicted_direction == predicted_direction
            and parse_ts(c.cooldown_until) > current_ts
        ]
        if not hits:
            return None
        return max(hits, key=lambda c: c.cooldown_until)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backtest Phase 1 directional predictions against archived SQLite history.")
    p.add_argument("--db", default="data/ah_prices.sqlite3", help="SQLite DB path for historical archive")
    p.add_argument(
        "--metric",
        default="weighted_avg_unit_price",
        choices=["min_unit_price", "avg_unit_price", "weighted_avg_unit_price"],
        help="Metric used for prediction/backtest",
    )
    p.add_argument("--horizon-hours", type=int, default=12, help="Prediction horizon in hours")
    p.add_argument("--grace-hours", type=int, default=6, help="Allow future match within this many extra hours")
    p.add_argument("--min-actual-move-pct", type=float, default=2.0, help="Minimum realized move to count as up/down")
    p.add_argument("--min-confidence", type=float, default=0.80, help="Minimum confidence to include a prediction")
    p.add_argument("--limit-snapshots", type=int, default=0, help="Optional limit on evaluated snapshots from newest backwards")
    p.add_argument("--top-n", type=int, default=10, help="Number of strongest predictions to print")
    p.add_argument("--prediction-window-hours", type=int, default=168, help="Long history window for prediction features")
    p.add_argument("--prediction-short-window-hours", type=int, default=12, help="Short history window for momentum features")
    p.add_argument("--prediction-medium-window-hours", type=int, default=48, help="Medium history window for trend context")
    p.add_argument("--prediction-min-history", type=int, default=24, help="Minimum long-window history rows")
    p.add_argument("--prediction-min-short-history", type=int, default=6, help="Minimum short-window history rows")
    p.add_argument("--prediction-cooldown-hours", type=int, default=24, help="Cooldown duration after a strongly failed directional prediction")
    p.add_argument("--prediction-cooldown-horizon-hours", type=int, default=12, help="Resolution horizon used to judge whether a prior prediction failed")
    p.add_argument("--prediction-cooldown-grace-hours", type=int, default=6, help="Extra grace window for matching a matured prediction")
    p.add_argument("--prediction-cooldown-min-confidence", type=float, default=0.85, help="Minimum prior prediction confidence required to trigger cooldown")
    p.add_argument("--prediction-cooldown-loss-pct", type=float, default=20.0, help="Adverse realized move percent required to trigger cooldown")
    p.add_argument("--min-listings-commodity", type=int, default=8, help="Liquidity floor for commodities")
    p.add_argument("--min-quantity-commodity", type=int, default=200, help="Quantity floor for commodities")
    p.add_argument("--min-listings-crafted", type=int, default=2, help="Liquidity floor for crafted/non-commodity")
    p.add_argument("--min-quantity-crafted", type=int, default=1, help="Quantity floor for crafted/non-commodity")
    return p.parse_args()


def build_prediction_args(cli: argparse.Namespace) -> Namespace:
    return Namespace(
        prediction_window_hours=cli.prediction_window_hours,
        prediction_short_window_hours=cli.prediction_short_window_hours,
        prediction_medium_window_hours=cli.prediction_medium_window_hours,
        prediction_min_history=cli.prediction_min_history,
        prediction_min_short_history=cli.prediction_min_short_history,
        prediction_min_confidence=cli.min_confidence,
        prediction_cooldown_hours=cli.prediction_cooldown_hours,
        prediction_cooldown_horizon_hours=cli.prediction_cooldown_horizon_hours,
        prediction_cooldown_grace_hours=cli.prediction_cooldown_grace_hours,
        prediction_cooldown_min_confidence=cli.prediction_cooldown_min_confidence,
        prediction_cooldown_loss_pct=cli.prediction_cooldown_loss_pct,
        min_listings_commodity=cli.min_listings_commodity,
        min_quantity_commodity=cli.min_quantity_commodity,
        min_listings_crafted=cli.min_listings_crafted,
        min_quantity_crafted=cli.min_quantity_crafted,
    )


def parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def format_pct(value: float) -> str:
    return f"{value * 100.0:+.1f}%"


def load_snapshot_rows(conn: sqlite3.Connection, observed_at: str, metric_name: str) -> List[Observation]:
    cur = conn.execute(
        """
        SELECT observed_at, item_id, item_name, source, metric_name, metric_value,
               listing_count, total_quantity, min_unit_price, max_unit_price,
               avg_unit_price, median_unit_price, p25_unit_price, weighted_avg_unit_price
        FROM observations
        WHERE observed_at = ?
          AND metric_name = ?
        ORDER BY item_id, source
        """,
        (observed_at, metric_name),
    )
    return [
        Observation(
            observed_at=str(v[0]),
            item_id=int(v[1]),
            item_name=str(v[2]),
            source=str(v[3]),
            metric_name=str(v[4]),
            metric_value=int(v[5]),
            listing_count=int(v[6]),
            total_quantity=int(v[7]),
            min_unit_price=v[8],
            max_unit_price=v[9],
            avg_unit_price=v[10],
            median_unit_price=v[11],
            p25_unit_price=v[12],
            weighted_avg_unit_price=v[13],
        )
        for v in cur.fetchall()
    ]


def find_future_value(
    conn: sqlite3.Connection,
    row: Observation,
    horizon_hours: int,
    grace_hours: int,
) -> Optional[Tuple[str, int]]:
    current_ts = parse_ts(row.observed_at)
    start_iso = (current_ts + timedelta(hours=horizon_hours)).isoformat().replace("+00:00", "Z")
    end_iso = (current_ts + timedelta(hours=horizon_hours + grace_hours)).isoformat().replace("+00:00", "Z")
    cur = conn.execute(
        """
        SELECT observed_at, metric_value
        FROM observations
        WHERE item_id = ?
          AND source = ?
          AND metric_name = ?
          AND observed_at >= ?
          AND observed_at <= ?
        ORDER BY observed_at ASC
        LIMIT 1
        """,
        (row.item_id, row.source, row.metric_name, start_iso, end_iso),
    )
    result = cur.fetchone()
    if not result:
        return None
    return str(result[0]), int(result[1])


def resolve_pending_predictions(
    replay_db: ReplayDB,
    rows: List[Observation],
    pending: List[PendingPrediction],
    cli: argparse.Namespace,
) -> Tuple[List[PendingPrediction], int]:
    row_map = {(row.item_id, row.source, row.metric_name): row for row in rows}
    current_ts = parse_ts(rows[0].observed_at) if rows else None
    remaining: List[PendingPrediction] = []
    cooldowns_added = 0
    adverse_move = cli.prediction_cooldown_loss_pct / 100.0

    for pending_pred in pending:
        if pending_pred.resolved:
            continue
        resolve_start = parse_ts(pending_pred.resolve_start_at)
        resolve_end = parse_ts(pending_pred.resolve_end_at)
        if current_ts is None or current_ts < resolve_start:
            remaining.append(pending_pred)
            continue
        if current_ts > resolve_end:
            continue

        current_row = row_map.get((pending_pred.item_id, pending_pred.source, pending_pred.metric_name))
        if current_row is None or pending_pred.current_value <= 0:
            remaining.append(pending_pred)
            continue

        realized_return = (current_row.metric_value - pending_pred.current_value) / float(pending_pred.current_value)
        failed = (
            pending_pred.predicted_direction == "up" and realized_return <= -adverse_move
        ) or (
            pending_pred.predicted_direction == "down" and realized_return >= adverse_move
        )
        if failed and pending_pred.confidence >= cli.prediction_cooldown_min_confidence:
            cooldown_until = (current_ts + timedelta(hours=cli.prediction_cooldown_hours)).isoformat().replace("+00:00", "Z")
            replay_db.cooldowns.append(
                PredictionCooldown(
                    item_id=pending_pred.item_id,
                    item_name=pending_pred.item_name,
                    source=pending_pred.source,
                    metric_name=pending_pred.metric_name,
                    predicted_direction=pending_pred.predicted_direction,
                    cooldown_until=cooldown_until,
                    reason=(
                        f"failed {pending_pred.predicted_direction} call from {pending_pred.observed_at} "
                        f"({realized_return * 100.0:+.1f}% realized)"
                    ),
                    trigger_prediction_at=pending_pred.observed_at,
                    trigger_confidence=pending_pred.confidence,
                    trigger_return_pct=pending_pred.predicted_return_pct,
                    realized_return_pct=realized_return,
                )
            )
            cooldowns_added += 1
        pending_pred.resolved = True
    return remaining, cooldowns_added


def main() -> int:
    cli = parse_args()
    conn = sqlite3.connect(cli.db)
    db = ReplayDB(cli.db)
    db.init()
    prediction_args = build_prediction_args(cli)

    try:
        snapshot_rows = conn.execute(
            """
            SELECT observed_at, COUNT(*)
            FROM observations
            WHERE metric_name = ?
            GROUP BY observed_at
            ORDER BY observed_at ASC
            """,
            (cli.metric,),
        ).fetchall()
        snapshots = [str(v[0]) for v in snapshot_rows]
        if cli.limit_snapshots > 0:
            snapshots = snapshots[-cli.limit_snapshots :]

        total_predictions = 0
        total_scored = 0
        matched_predictions = 0
        correct_direction = 0
        predicted_return_sum = 0.0
        actual_return_sum = 0.0
        by_direction: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        strongest_hits: List[Tuple[float, str]] = []
        strongest_misses: List[Tuple[float, str]] = []
        move_threshold = cli.min_actual_move_pct / 100.0
        cooldowns_added = 0
        cooldown_blocks = 0
        pending: List[PendingPrediction] = []

        for observed_at in snapshots:
            rows = load_snapshot_rows(conn, observed_at, cli.metric)
            pending, newly_added_cooldowns = resolve_pending_predictions(db, rows, pending, cli)
            cooldowns_added += newly_added_cooldowns

            predictions, diag = detect_predictions(db=db, rows=rows, args=prediction_args)
            cooldown_blocks += diag.blocked_cooldown
            for pred in predictions:
                if pred.predicted_direction not in {"up", "down"}:
                    continue
                if pred.confidence < cli.min_confidence:
                    continue
                total_predictions += 1

                original_row = next((r for r in rows if r.item_id == pred.item_id and r.source == pred.source), None)
                if original_row is None:
                    continue

                future = find_future_value(conn, original_row, cli.horizon_hours, cli.grace_hours)
                if future is None:
                    continue

                matched_predictions += 1
                future_ts, future_value = future
                actual_return = (future_value - original_row.metric_value) / float(original_row.metric_value)
                predicted_return_sum += pred.predicted_return_pct
                actual_return_sum += actual_return

                if actual_return > move_threshold:
                    actual_direction = "up"
                elif actual_return < -move_threshold:
                    actual_direction = "down"
                else:
                    actual_direction = "flat"

                by_direction[pred.predicted_direction]["predicted"] += 1
                by_direction[pred.predicted_direction][actual_direction] += 1

                total_scored += 1
                correct = actual_direction == pred.predicted_direction
                if correct:
                    correct_direction += 1

                detail = (
                    f"{pred.predicted_direction.upper()} {pred.item_name} [{pred.item_id}] {pred.source} at {pred.observed_at} "
                    f"conf {pred.confidence:.2f}, pred {format_pct(pred.predicted_return_pct)}, actual {format_pct(actual_return)} by {future_ts}"
                )
                bucket = strongest_hits if correct else strongest_misses
                bucket.append((pred.confidence, detail))

                prediction_ts = parse_ts(pred.observed_at)
                pending.append(
                    PendingPrediction(
                        observed_at=pred.observed_at,
                        resolve_start_at=(prediction_ts + timedelta(hours=cli.prediction_cooldown_horizon_hours)).isoformat().replace("+00:00", "Z"),
                        resolve_end_at=(prediction_ts + timedelta(hours=cli.prediction_cooldown_horizon_hours + cli.prediction_cooldown_grace_hours)).isoformat().replace("+00:00", "Z"),
                        item_id=pred.item_id,
                        item_name=pred.item_name,
                        source=pred.source,
                        metric_name=pred.metric_name,
                        current_value=pred.current_value,
                        predicted_direction=pred.predicted_direction,
                        confidence=pred.confidence,
                        predicted_return_pct=pred.predicted_return_pct,
                    )
                )

        strongest_hits.sort(key=lambda v: v[0], reverse=True)
        strongest_misses.sort(key=lambda v: v[0], reverse=True)

        print(f"Backtest DB: {cli.db}")
        print(f"Snapshots evaluated: {len(snapshots)}")
        print(f"Predictions generated: {total_predictions}")
        print(f"Predictions with future match: {matched_predictions}")
        print(f"Cooldowns added during replay: {cooldowns_added}")
        print(f"Predictions blocked by cooldown: {cooldown_blocks}")
        if total_scored == 0:
            print("No scored predictions. Increase history depth, widen grace-hours, or lower confidence threshold.")
            return 0

        hit_rate = correct_direction / float(total_scored)
        avg_pred = predicted_return_sum / float(total_scored)
        avg_actual = actual_return_sum / float(total_scored)
        print(f"Directional accuracy: {hit_rate:.2%}")
        print(f"Average predicted return: {avg_pred:.2%}")
        print(f"Average actual return: {avg_actual:.2%}")

        for direction in ("up", "down"):
            counts = by_direction.get(direction, {})
            predicted = counts.get("predicted", 0)
            if predicted == 0:
                continue
            print(
                f"{direction.upper()} breakdown: predicted={predicted}, "
                f"actual_up={counts.get('up', 0)}, actual_down={counts.get('down', 0)}, actual_flat={counts.get('flat', 0)}"
            )

        print("Top hits:")
        for _, line in strongest_hits[: cli.top_n]:
            print(f"- {line}")

        print("Top misses:")
        for _, line in strongest_misses[: cli.top_n]:
            print(f"- {line}")
        return 0
    finally:
        db.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
