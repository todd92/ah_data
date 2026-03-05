#!/usr/bin/env python3
import argparse
import base64
import json
import time
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

OAUTH_HOST = "oauth.battle.net"
API_HOSTS = {
    "us": "us.api.blizzard.com",
    "eu": "eu.api.blizzard.com",
    "kr": "kr.api.blizzard.com",
    "tw": "tw.api.blizzard.com",
}


def text_value(v: Any, locale: str) -> str:
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        return str(v.get(locale) or v.get("en_US") or next(iter(v.values()), ""))
    return ""


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

    def api_get(self, path: str, namespace: str) -> Any:
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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build AH target list from profession skill tiers for an expansion keyword.")
    p.add_argument("--config", default="config.json", help="Config JSON with region/locale/client_id/client_secret")
    p.add_argument("--expansion-keyword", default="midnight", help="Case-insensitive tier-name keyword")
    p.add_argument(
        "--professions",
        default="tailoring,enchanting",
        help="Comma-separated profession names to include (ex: tailoring,enchanting)",
    )
    p.add_argument("--output", default="targets_midnight_tailoring_enchanting.json", help="Output targets JSON file")
    p.add_argument(
        "--include-reagents",
        action="store_true",
        help="Include recipe reagent items in addition to crafted outputs",
    )
    return p.parse_args()


def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def add_item(store: Dict[int, str], item_obj: Dict[str, Any], locale: str):
    item_id = item_obj.get("id")
    if not isinstance(item_id, int):
        return
    name = text_value(item_obj.get("name"), locale) or f"item-{item_id}"
    if item_id not in store:
        store[item_id] = name


def main() -> int:
    args = parse_args()
    cfg = load_config(Path(args.config))

    region = str(cfg.get("region", "us")).lower()
    locale = str(cfg.get("locale", "en_US"))
    client_id = str(cfg.get("client_id", "")).strip()
    client_secret = str(cfg.get("client_secret", "")).strip()
    if not client_id or not client_secret:
        raise ValueError("Config must include client_id and client_secret")

    prof_wanted: Set[str] = {p.strip().lower() for p in args.professions.split(",") if p.strip()}
    keyword = args.expansion_keyword.strip().lower()
    if not prof_wanted:
        raise ValueError("--professions must include at least one value")

    api = BlizzardAPI(client_id=client_id, client_secret=client_secret, region=region, locale=locale)

    prof_index = api.api_get("/data/wow/profession/index", namespace=f"static-{region}")
    professions = prof_index.get("professions", [])

    selected_professions: List[Dict[str, Any]] = []
    for p in professions:
        pname = text_value(p.get("name"), locale).lower()
        if pname in prof_wanted:
            selected_professions.append(p)

    if not selected_professions:
        raise RuntimeError(f"No matching professions found for: {sorted(prof_wanted)}")

    items: Dict[int, str] = {}
    recipe_count = 0

    for p in selected_professions:
        pid = p.get("id")
        if not isinstance(pid, int):
            continue
        prof_name = text_value(p.get("name"), locale) or f"profession-{pid}"

        pdetail = api.api_get(f"/data/wow/profession/{pid}", namespace=f"static-{region}")
        tiers = pdetail.get("skill_tiers", [])
        failed_recipe_count = 0

        matching_tiers = []
        for t in tiers:
            tname = text_value(t.get("name"), locale)
            if keyword in tname.lower():
                matching_tiers.append(t)

        for tier in matching_tiers:
            tid = tier.get("id")
            if not isinstance(tid, int):
                continue

            tdetail = api.api_get(f"/data/wow/profession/{pid}/skill-tier/{tid}", namespace=f"static-{region}")
            categories = tdetail.get("categories", [])
            recipe_ids: Set[int] = set()
            for c in categories:
                for r in c.get("recipes", []):
                    rid = r.get("id")
                    if isinstance(rid, int):
                        recipe_ids.add(rid)

            for rid in recipe_ids:
                try:
                    rdetail = api.api_get(f"/data/wow/recipe/{rid}", namespace=f"static-{region}")
                except Exception as exc:
                    failed_recipe_count += 1
                    print(f"WARN: skipping recipe {rid}: {exc}", file=sys.stderr)
                    continue
                recipe_count += 1
                crafted = rdetail.get("crafted_item")
                if isinstance(crafted, dict):
                    add_item(items, crafted, locale)

                if args.include_reagents:
                    for reagent in rdetail.get("reagents", []):
                        reagent_item = reagent.get("reagent")
                        if isinstance(reagent_item, dict):
                            add_item(items, reagent_item, locale)

        print(
            f"Processed profession '{prof_name}' (id={pid}), matching tiers={len(matching_tiers)}, failed recipes={failed_recipe_count}"
        )

    targets = [
        {
            "name": name,
            "item_id": item_id,
            "source_mode": "auto",
        }
        for item_id, name in sorted(items.items(), key=lambda kv: kv[1].lower())
    ]

    payload = {
        "meta": {
            "region": region,
            "locale": locale,
            "expansion_keyword": args.expansion_keyword,
            "professions": sorted(prof_wanted),
            "include_reagents": args.include_reagents,
            "recipe_count": recipe_count,
            "item_count": len(targets),
        },
        "targets": targets,
    }

    out = Path(args.output)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {len(targets)} items to {out}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
