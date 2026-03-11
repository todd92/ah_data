#!/usr/bin/env python3
import argparse
import base64
import json
import re
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
WOWHEAD_SEARCH_URL = "https://www.wowhead.com/search"
WOWHEAD_ITEM_RE = re.compile(r"/item=(\d+)/(?:[^\"'<>]+)")
WOWHEAD_SPELL_RE = re.compile(r"/spell=(\d+)/(?:[^\"'<>]+)")
WOWHEAD_RESULT_RE = re.compile(r'href=\"(/spell=\d+/[^"]+)\"[^>]*>([^<]+)</a>', re.IGNORECASE)


def text_value(v: Any, locale: str) -> str:
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        return str(v.get(locale) or v.get("en_US") or next(iter(v.values()), ""))
    return ""


def extract_id_from_href(href: str, path_fragment: str) -> Optional[int]:
    if path_fragment not in href:
        return None
    tail = href.split(path_fragment)[-1]
    value = tail.split("?")[0].strip("/")
    if value.isdigit():
        return int(value)
    return None


def item_id_from_ref(item_obj: Any) -> Optional[int]:
    if not isinstance(item_obj, dict):
        return None
    item_id = item_obj.get("id")
    if isinstance(item_id, int):
        return item_id
    href_candidates = [
        str((item_obj.get("key") or {}).get("href", "")),
        str(item_obj.get("href", "")),
    ]
    for href in href_candidates:
        item_id = extract_id_from_href(href, "/data/wow/item/")
        if item_id is not None:
            return item_id
    return None


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


class WowheadClient:
    def __init__(self):
        self._opener = urllib.request.build_opener()
        self._headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }

    def _http_text(self, url: str) -> str:
        req = urllib.request.Request(url=url, headers=self._headers)
        with self._opener.open(req, timeout=45) as resp:
            return resp.read().decode("utf-8", errors="ignore")

    def search_spell_url(self, recipe_name: str) -> Optional[str]:
        params = urllib.parse.urlencode({"q": recipe_name})
        html = self._http_text(f"{WOWHEAD_SEARCH_URL}?{params}")

        lowered_name = recipe_name.strip().lower()
        for href, label in WOWHEAD_RESULT_RE.findall(html):
            if label.strip().lower() == lowered_name:
                return urllib.parse.urljoin("https://www.wowhead.com", href)
        for href, _label in WOWHEAD_RESULT_RE.findall(html):
            return urllib.parse.urljoin("https://www.wowhead.com", href)
        return None

    def parse_recipe_page(self, url: str) -> Optional[Dict[str, Any]]:
        html = self._http_text(url)
        spell_match = WOWHEAD_SPELL_RE.search(html)
        crafted_match = WOWHEAD_ITEM_RE.search(html)
        if not crafted_match:
            return None

        item_id = int(crafted_match.group(1))
        title_match = re.search(r"<title>([^<]+?) - Spell - World of Warcraft</title>", html, re.IGNORECASE)
        recipe_name = title_match.group(1).strip() if title_match else ""
        item_name = ""
        crafted_title = re.search(r'item=(\d+)/([^"\'<>]+)', html, re.IGNORECASE)
        if crafted_title:
            item_name = urllib.parse.unquote(crafted_title.group(2)).replace("-", " ").strip().title()

        return {
            "wowhead_spell_id": int(spell_match.group(1)) if spell_match else None,
            "crafted_item_id": item_id,
            "crafted_item_name": item_name or f"item-{item_id}",
            "recipe_name": recipe_name,
            "wowhead_url": url,
        }


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
    p.add_argument(
        "--wowhead-cache",
        default="wowhead_recipe_cache.json",
        help="JSON cache file for Wowhead recipe lookups",
    )
    return p.parse_args()


def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def add_item(store: Dict[int, str], item_obj: Dict[str, Any], locale: str):
    item_id = item_id_from_ref(item_obj)
    if item_id is None:
        return
    name = text_value(item_obj.get("name"), locale) or f"item-{item_id}"
    if item_id not in store:
        store[item_id] = name


def crafted_quantity_value(recipe: Dict[str, Any]) -> int:
    raw = recipe.get("crafted_quantity")
    if isinstance(raw, int) and raw > 0:
        return raw
    if isinstance(raw, dict):
        for k in ("value", "minimum", "max", "maximum"):
            v = raw.get(k)
            if isinstance(v, int) and v > 0:
                return v
    return 1


def load_cache(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for k, v in payload.items():
        if isinstance(k, str) and isinstance(v, dict):
            out[k] = v
    return out


def save_cache(path: Path, cache: Dict[str, Dict[str, Any]]) -> None:
    path.write_text(json.dumps(cache, indent=2, sort_keys=True), encoding="utf-8")


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
    wowhead = WowheadClient()
    cache_path = Path(args.wowhead_cache)
    if not cache_path.is_absolute():
        cache_path = Path(args.config).parent / cache_path
    wowhead_cache = load_cache(cache_path)
    wowhead_hits = 0
    wowhead_misses = 0

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
    recipe_defs: List[Dict[str, Any]] = []

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
                crafted_id = item_id_from_ref(crafted)
                recipe_name = text_value(rdetail.get("name"), locale) or f"recipe-{rid}"
                if not isinstance(crafted_id, int):
                    cached = wowhead_cache.get(recipe_name)
                    if cached and isinstance(cached.get("crafted_item_id"), int):
                        crafted_id = int(cached["crafted_item_id"])
                    else:
                        try:
                            wowhead_url = wowhead.search_spell_url(recipe_name)
                            parsed = wowhead.parse_recipe_page(wowhead_url) if wowhead_url else None
                        except Exception as exc:
                            print(f"WARN: Wowhead lookup failed for '{recipe_name}': {exc}", file=sys.stderr)
                            parsed = None
                        if parsed and isinstance(parsed.get("crafted_item_id"), int):
                            wowhead_cache[recipe_name] = parsed
                            crafted_id = int(parsed["crafted_item_id"])
                            wowhead_hits += 1
                        else:
                            wowhead_misses += 1

                if isinstance(crafted_id, int):
                    recipe_entry: Dict[str, Any] = {
                        "recipe_id": rid,
                        "recipe_name": recipe_name,
                        "profession": prof_name,
                        "profession_id": pid,
                        "crafted_item_id": crafted_id,
                        "crafted_item_name": (
                            text_value(crafted.get("name"), locale)
                            if isinstance(crafted, dict)
                            else ""
                        )
                        or str(wowhead_cache.get(recipe_name, {}).get("crafted_item_name") or f"item-{crafted_id}"),
                        "crafted_quantity": crafted_quantity_value(rdetail),
                        "reagents": [],
                    }
                    for reagent in rdetail.get("reagents", []):
                        if not isinstance(reagent, dict):
                            continue
                        reagent_item = reagent.get("reagent")
                        if not isinstance(reagent_item, dict):
                            continue
                        reagent_id = item_id_from_ref(reagent_item)
                        reagent_qty = reagent.get("quantity")
                        if not isinstance(reagent_id, int) or not isinstance(reagent_qty, int) or reagent_qty <= 0:
                            continue
                        recipe_entry["reagents"].append(
                            {
                                "item_id": reagent_id,
                                "name": text_value(reagent_item.get("name"), locale) or f"item-{reagent_id}",
                                "quantity": reagent_qty,
                            }
                        )
                    if recipe_entry["reagents"]:
                        recipe_defs.append(recipe_entry)

                if args.include_reagents:
                    for reagent in rdetail.get("reagents", []):
                        reagent_item = reagent.get("reagent")
                        if isinstance(reagent_item, dict):
                            add_item(items, reagent_item, locale)

        print(
            f"Processed profession '{prof_name}' (id={pid}), matching tiers={len(matching_tiers)}, failed recipes={failed_recipe_count}"
        )

    save_cache(cache_path, wowhead_cache)

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
            "recipe_definition_count": len(recipe_defs),
            "item_count": len(targets),
            "wowhead_cache_entries": len(wowhead_cache),
            "wowhead_hits": wowhead_hits,
            "wowhead_misses": wowhead_misses,
        },
        "targets": targets,
        "recipes": recipe_defs,
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
