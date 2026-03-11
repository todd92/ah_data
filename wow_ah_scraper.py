#!/usr/bin/env python3
import argparse
import base64
import json
import math
import time
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


OAUTH_HOST = "oauth.battle.net"
API_HOSTS = {
    "us": "us.api.blizzard.com",
    "eu": "eu.api.blizzard.com",
    "kr": "kr.api.blizzard.com",
    "tw": "tw.api.blizzard.com",
}


@dataclass(frozen=True)
class SourceKey:
    source_type: str
    id_or_slug: str


class BlizzardAPI:
    def __init__(self, client_id: str, client_secret: str, region: str, locale: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.region = region
        self.locale = locale
        self.api_host = API_HOSTS.get(region)
        if not self.api_host:
            raise ValueError(f"Unsupported region '{region}'. Use one of: {', '.join(API_HOSTS)}")
        self._access_token: Optional[str] = None
        self._realm_index: Optional[List[Dict[str, Any]]] = None

    def _http_json(self, method: str, url: str, headers: Dict[str, str], body: Optional[bytes] = None) -> Any:
        retry_status = {429, 500, 502, 503, 504}
        attempts = 5
        for i in range(attempts):
            req = urllib.request.Request(url=url, method=method, headers=headers, data=body)
            try:
                with urllib.request.urlopen(req, timeout=45) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                details = exc.read().decode("utf-8", errors="ignore")
                if exc.code in retry_status and i < attempts - 1:
                    time.sleep(1.5 * (2**i))
                    continue
                raise RuntimeError(f"HTTP {exc.code} for {url}: {details}") from exc
            except urllib.error.URLError as exc:
                if i < attempts - 1:
                    time.sleep(1.5 * (2**i))
                    continue
                raise RuntimeError(f"URL error for {url}: {exc}") from exc
        raise RuntimeError(f"Request failed after retries: {url}")

    def access_token(self) -> str:
        if self._access_token:
            return self._access_token

        auth_raw = f"{self.client_id}:{self.client_secret}".encode("utf-8")
        auth_b64 = base64.b64encode(auth_raw).decode("ascii")
        data = urllib.parse.urlencode({"grant_type": "client_credentials"}).encode("utf-8")
        url = f"https://{OAUTH_HOST}/token"
        payload = self._http_json(
            method="POST",
            url=url,
            headers={
                "Authorization": f"Basic {auth_b64}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            body=data,
        )
        token = payload.get("access_token")
        if not token:
            raise RuntimeError(f"OAuth response missing access_token: {payload}")
        self._access_token = token
        return token

    def _api_get(self, path: str, namespace: str) -> Any:
        params = urllib.parse.urlencode({"locale": self.locale})
        url = f"https://{self.api_host}{path}?{params}"
        return self._http_json(
            method="GET",
            url=url,
            headers={
                "Authorization": f"Bearer {self.access_token()}",
                "Battlenet-Namespace": namespace,
            },
        )

    def realm_index(self) -> List[Dict[str, Any]]:
        if self._realm_index is None:
            data = self._api_get(path="/data/wow/realm/index", namespace=f"dynamic-{self.region}")
            self._realm_index = data.get("realms", [])
        return self._realm_index

    @staticmethod
    def _slugify_name(name: str) -> str:
        cleaned = "".join(ch if ch.isalnum() or ch == " " else " " for ch in name.lower())
        return "-".join(part for part in cleaned.split() if part)

    @staticmethod
    def _extract_id_from_href(href: str, path_fragment: str) -> Optional[int]:
        if path_fragment not in href:
            return None
        tail = href.split(path_fragment)[-1]
        value = tail.split("?")[0].strip("/")
        if value.isdigit():
            return int(value)
        return None

    def resolve_realm_to_connected_id(self, realm_slug: str) -> int:
        match_realm_id: Optional[int] = None
        query = realm_slug.strip().lower()
        realms = self.realm_index()
        for realm in realms:
            name_field = realm.get("name")
            name = name_field if isinstance(name_field, str) else str((name_field or {}).get(self.locale) or "")
            name_slug = self._slugify_name(name)
            href = str((realm.get("key") or {}).get("href", ""))
            realm_id = self._extract_id_from_href(href, "/data/wow/realm/")
            if realm_id is None:
                continue
            if query == name.lower() or query == name_slug:
                match_realm_id = realm_id
                break

        if match_realm_id is None:
            raise RuntimeError(f"Realm '{realm_slug}' not found in region '{self.region}' realm index")

        data = self._api_get(path=f"/data/wow/realm/{match_realm_id}", namespace=f"dynamic-{self.region}")
        href = (data.get("connected_realm") or {}).get("href", "")
        connected_id = self._extract_id_from_href(href, "/data/wow/connected-realm/")
        if connected_id is None:
            raise RuntimeError(f"Could not parse connected realm ID from href '{href}' for realm '{realm_slug}'")
        return connected_id

    def connected_realm_auctions(self, connected_id: int) -> List[Dict[str, Any]]:
        data = self._api_get(
            path=f"/data/wow/connected-realm/{connected_id}/auctions",
            namespace=f"dynamic-{self.region}",
        )
        return data.get("auctions", [])

    def commodity_auctions(self) -> List[Dict[str, Any]]:
        data = self._api_get(path="/data/wow/auctions/commodities", namespace=f"dynamic-{self.region}")
        return data.get("auctions", [])

    def item_details(self, item_id: int) -> Dict[str, Any]:
        return self._api_get(path=f"/data/wow/item/{item_id}", namespace=f"static-{self.region}")


@dataclass
class Target:
    name: str
    item_id: int
    sources: List[Dict[str, Any]]
    source_mode: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch and summarize WoW auction house prices for selected items.")
    p.add_argument("--config", default="config.json", help="Path to JSON config file")
    p.add_argument("--output", default="report.json", help="Output JSON summary path")
    return p.parse_args()


def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def parse_targets_list(raw_targets: List[Dict[str, Any]], label: str) -> List[Target]:
    targets: List[Target] = []
    for i, t in enumerate(raw_targets, 1):
        try:
            item_id = int(t["item_id"])
        except Exception as exc:
            raise ValueError(f"{label}[{i}] missing valid item_id") from exc
        name = str(t.get("name", f"item-{item_id}"))
        sources = t.get("sources", [])
        source_mode = str(t.get("source_mode", "manual"))
        if source_mode not in {"manual", "auto"}:
            raise ValueError(f"{label}[{i}] ('{name}') has invalid source_mode '{source_mode}'")
        if source_mode == "manual" and not sources:
            raise ValueError(f"{label}[{i}] ('{name}') must include at least one source when source_mode is manual")
        targets.append(Target(name=name, item_id=item_id, sources=sources, source_mode=source_mode))
    return targets


def build_targets(cfg: Dict[str, Any], config_dir: Path) -> List[Target]:
    all_targets: List[Target] = []
    raw_targets = cfg.get("targets", [])
    if raw_targets:
        all_targets.extend(parse_targets_list(raw_targets, "targets"))

    targets_files: List[str] = []
    one = cfg.get("targets_file")
    if isinstance(one, str) and one.strip():
        targets_files.append(one)
    many = cfg.get("targets_files")
    if isinstance(many, list):
        targets_files.extend(str(v) for v in many if str(v).strip())

    for p in targets_files:
        file_path = Path(p)
        if not file_path.is_absolute():
            file_path = config_dir / file_path
        if not file_path.exists():
            raise FileNotFoundError(f"Targets file not found: {file_path}")
        with file_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        file_targets = payload.get("targets", [])
        if not isinstance(file_targets, list):
            raise ValueError(f"Targets file has invalid 'targets' payload: {file_path}")
        all_targets.extend(parse_targets_list(file_targets, f"{file_path.name}:targets"))

    if not all_targets:
        raise ValueError("Config must include at least one target via 'targets' or 'targets_file(s)'")

    deduped: Dict[int, Target] = {}
    for t in all_targets:
        if t.item_id not in deduped:
            deduped[t.item_id] = t
    return list(deduped.values())


def unit_price_from_auction(a: Dict[str, Any]) -> Optional[int]:
    # Commodity auctions expose unit_price directly.
    if "unit_price" in a and isinstance(a["unit_price"], int):
        return a["unit_price"]

    qty = int(a.get("quantity", 1))
    if qty < 1:
        qty = 1

    # Item auctions expose buyout for the full stack.
    buyout = a.get("buyout")
    if isinstance(buyout, int) and buyout > 0:
        return buyout // qty

    return None


def percentile_value(sorted_values: List[int], fraction: float) -> Optional[int]:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    clamped = min(max(fraction, 0.0), 1.0)
    pos = clamped * (len(sorted_values) - 1)
    lower = int(math.floor(pos))
    upper = int(math.ceil(pos))
    if lower == upper:
        return sorted_values[lower]
    weight = pos - lower
    return int(sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * weight)


def summarize(auctions: List[Dict[str, Any]], item_ids: Set[int]) -> Dict[int, Dict[str, Any]]:
    by_item: Dict[int, List[Tuple[int, int]]] = defaultdict(list)

    for a in auctions:
        item = a.get("item") or {}
        item_id = item.get("id")
        if item_id not in item_ids:
            continue
        unit = unit_price_from_auction(a)
        if unit is None:
            continue
        qty = int(a.get("quantity", 1))
        by_item[item_id].append((unit, qty))

    result: Dict[int, Dict[str, Any]] = {}
    for item_id, samples in by_item.items():
        prices = sorted(p for p, _ in samples)
        qty_sum = sum(q for _, q in samples)
        weighted_sum = sum(p * q for p, q in samples)
        result[item_id] = {
            "listing_count": len(samples),
            "total_quantity": qty_sum,
            "min_unit_price": min(prices),
            "max_unit_price": max(prices),
            "avg_unit_price": int(sum(prices) / len(prices)),
            "median_unit_price": percentile_value(prices, 0.50),
            "p25_unit_price": percentile_value(prices, 0.25),
            "weighted_avg_unit_price": int(weighted_sum / qty_sum) if qty_sum else None,
        }
    return result


def source_key_and_label(source: Dict[str, Any], resolved_connected_id: Optional[int] = None) -> Tuple[SourceKey, str]:
    st = source.get("type")
    if st == "connected_realm":
        cid = int(source["id"])
        return SourceKey("connected_realm", str(cid)), f"connected_realm:{cid}"
    if st == "realm":
        slug = str(source["slug"])
        if resolved_connected_id is None:
            return SourceKey("realm", slug), f"realm:{slug}"
        return SourceKey("connected_realm", str(resolved_connected_id)), f"realm:{slug} (connected_realm:{resolved_connected_id})"
    if st == "commodity":
        return SourceKey("commodity", "region"), "commodity:region"
    raise ValueError(f"Unknown source type '{st}'. Expected connected_realm, realm, or commodity.")


def is_reagent_or_trade_good(item_details: Dict[str, Any]) -> bool:
    item_class = item_details.get("item_class") or {}
    class_id = item_class.get("id")
    if class_id == 7:
        return True

    # Fallback when class IDs change or are missing in unexpected payloads.
    name_field = item_class.get("name")
    if isinstance(name_field, dict):
        name = str(name_field.get("en_US", "")).lower()
    else:
        name = str(name_field or "").lower()
    return "trade goods" in name or "reagent" in name


def main() -> int:
    args = parse_args()
    config_path = Path(args.config)
    cfg = load_config(config_path)

    region = str(cfg.get("region", "us")).lower()
    locale = str(cfg.get("locale", "en_US"))
    client_id = str(cfg.get("client_id", "")).strip()
    client_secret = str(cfg.get("client_secret", "")).strip()
    if not client_id or not client_secret:
        raise ValueError("Config must include client_id and client_secret")

    api = BlizzardAPI(client_id=client_id, client_secret=client_secret, region=region, locale=locale)
    targets = build_targets(cfg, config_path.parent)
    target_item_ids = {t.item_id for t in targets}
    default_realm_slug = str(cfg.get("default_realm_slug", "")).strip()
    if any(t.source_mode == "auto" for t in targets) and not default_realm_slug:
        raise ValueError("Config must include default_realm_slug when any target uses source_mode 'auto'")

    auto_target_sources: Dict[int, List[Dict[str, Any]]] = {}
    auto_target_reason: Dict[int, str] = {}
    if default_realm_slug:
        default_connected_id = api.resolve_realm_to_connected_id(default_realm_slug)
    else:
        default_connected_id = None

    for t in targets:
        if t.source_mode != "auto":
            continue
        details = api.item_details(t.item_id)
        if is_reagent_or_trade_good(details):
            # Blizzard exposes reagent-like commodity data through the commodities endpoint.
            auto_target_sources[t.item_id] = [{"type": "commodity"}]
            auto_target_reason[t.item_id] = "auto: reagent/trade goods -> commodity:region"
        else:
            auto_target_sources[t.item_id] = [{"type": "realm", "slug": default_realm_slug}]
            auto_target_reason[t.item_id] = (
                f"auto: non-reagent -> realm:{default_realm_slug} (connected_realm:{default_connected_id})"
            )

    realm_slug_to_connected: Dict[str, int] = {}
    fetch_plan: Dict[SourceKey, str] = {}

    # Resolve all realm sources once, then dedupe fetching by concrete source.
    for t in targets:
        effective_sources = auto_target_sources.get(t.item_id, t.sources)
        for source in effective_sources:
            st = source.get("type")
            if st == "realm":
                slug = str(source["slug"])
                if slug not in realm_slug_to_connected:
                    realm_slug_to_connected[slug] = api.resolve_realm_to_connected_id(slug)
                key, label = source_key_and_label(source, realm_slug_to_connected[slug])
            else:
                key, label = source_key_and_label(source)
            fetch_plan[key] = label

    source_item_summary: Dict[SourceKey, Dict[int, Dict[str, Any]]] = {}
    for key in fetch_plan:
        if key.source_type == "commodity":
            auctions = api.commodity_auctions()
        else:
            auctions = api.connected_realm_auctions(int(key.id_or_slug))
        source_item_summary[key] = summarize(auctions, target_item_ids)

    report: Dict[str, Any] = {
        "region": region,
        "locale": locale,
        "targets": [],
    }

    for t in targets:
        entry = {
            "name": t.name,
            "item_id": t.item_id,
            "sources": [],
        }
        if t.item_id in auto_target_reason:
            entry["source_mode"] = "auto"
            entry["source_mode_reason"] = auto_target_reason[t.item_id]

        effective_sources = auto_target_sources.get(t.item_id, t.sources)
        for source in effective_sources:
            st = source.get("type")
            if st == "realm":
                slug = str(source["slug"])
                key, label = source_key_and_label(source, realm_slug_to_connected[slug])
            else:
                key, label = source_key_and_label(source)
            summary = source_item_summary.get(key, {}).get(t.item_id)
            entry["sources"].append(
                {
                    "source": label,
                    "summary": summary,
                }
            )
        report["targets"].append(entry)

    out_path = Path(args.output)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"Wrote summary for {len(targets)} target item(s) to {out_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
