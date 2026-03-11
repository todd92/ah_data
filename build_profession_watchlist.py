#!/usr/bin/env python3
import argparse
import base64
import json
import html
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
WIKI_API_URL = "https://warcraft.wiki.gg/api.php"
WIKI_ITEM_ID_RE = re.compile(r"(?:Item ID|ID)\s*:?\s*(\d+)", re.IGNORECASE)
WIKI_REAGENT_LINE_RE = re.compile(r"(\d+)x\s+([A-Za-z0-9'&: -]+)")


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


class WarcraftWikiClient:
    def __init__(self):
        self._opener = urllib.request.build_opener()
        self._headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }

    def _http_json(self, params: Dict[str, str]) -> Any:
        query = urllib.parse.urlencode(params)
        req = urllib.request.Request(url=f"{WIKI_API_URL}?{query}", headers=self._headers)
        with self._opener.open(req, timeout=45) as resp:
            return json.loads(resp.read().decode("utf-8", errors="ignore"))

    def page_html(self, title: str) -> Optional[str]:
        try:
            payload = self._http_json(
                {
                    "action": "parse",
                    "page": title,
                    "prop": "text",
                    "format": "json",
                    "redirects": "1",
                }
            )
        except Exception:
            return None
        return str((((payload.get("parse") or {}).get("text") or {}).get("*")) or "")

    def parse_item_page(self, title: str) -> Optional[Dict[str, Any]]:
        page_html = self.page_html(title)
        if not page_html:
            return None
        text = html.unescape(re.sub(r"<[^>]+>", " ", page_html))
        text = re.sub(r"\s+", " ", text).strip()
        item_id_match = WIKI_ITEM_ID_RE.search(text)
        if not item_id_match:
            item_id_match = re.search(r"/item=(\d+)", page_html)
        if not item_id_match:
            return None

        item_id = int(item_id_match.group(1))
        item_name = title.replace("_", " ")
        profession_match = re.search(r"created with Midnight ([A-Za-z]+)", text, re.IGNORECASE)
        profession = profession_match.group(1).lower() if profession_match else None

        reagents: List[Dict[str, Any]] = []
        reagents_match = re.search(
            r"Reagents:\s*(.*?)\s*(?:Crafting reagent for|Patch changes|External links|Retrieved from)",
            text,
            re.IGNORECASE,
        )
        if reagents_match:
            reagent_blob = reagents_match.group(1)
            for qty_text, reagent_name in WIKI_REAGENT_LINE_RE.findall(reagent_blob):
                reagents.append(
                    {
                        "name": reagent_name.strip(),
                        "quantity": int(qty_text),
                    }
                )

        return {
            "crafted_item_id": item_id,
            "crafted_item_name": item_name,
            "recipe_name": item_name,
            "profession": profession,
            "reagents": reagents,
            "wiki_title": title,
        }


def normalize_name(name: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", name.lower())
    return " ".join(cleaned.split())


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
        "--recipe-cache",
        default="recipe_lookup_cache.json",
        help="JSON cache file for external recipe lookups",
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
    wiki = WarcraftWikiClient()
    cache_path = Path(args.recipe_cache)
    if not cache_path.is_absolute():
        cache_path = Path(args.config).parent / cache_path
    recipe_cache = load_cache(cache_path)
    external_hits = 0
    external_misses = 0

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
                        or f"item-{crafted_id}",
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

    name_to_id = {normalize_name(name): item_id for item_id, name in items.items()}
    existing_recipe_outputs = {int(r["crafted_item_id"]) for r in recipe_defs if isinstance(r.get("crafted_item_id"), int)}
    synthetic_recipe_id = 900000000
    for item_id, item_name in sorted(items.items(), key=lambda kv: kv[1].lower()):
        if item_id in existing_recipe_outputs:
            continue
        cached = recipe_cache.get(item_name)
        if cached and isinstance(cached.get("crafted_item_id"), int):
            parsed = cached
        else:
            try:
                parsed = wiki.parse_item_page(item_name.replace(" ", "_"))
            except Exception as exc:
                print(f"WARN: external lookup failed for '{item_name}': {exc}", file=sys.stderr)
                parsed = None
            if parsed:
                recipe_cache[item_name] = parsed

        if not parsed:
            external_misses += 1
            continue
        external_hits += 1
        profession = str(parsed.get("profession") or "").lower()
        if profession not in prof_wanted:
            continue
        if int(parsed.get("crafted_item_id", 0) or 0) != item_id:
            continue
        reagents_raw = parsed.get("reagents") or []
        resolved_reagents: List[Dict[str, Any]] = []
        for reagent in reagents_raw:
            reagent_name = str(reagent.get("name") or "").strip()
            reagent_qty = reagent.get("quantity")
            reagent_id = name_to_id.get(normalize_name(reagent_name))
            if reagent_id is None or not isinstance(reagent_qty, int) or reagent_qty <= 0:
                resolved_reagents = []
                break
            resolved_reagents.append(
                {
                    "item_id": reagent_id,
                    "name": items[reagent_id],
                    "quantity": reagent_qty,
                }
            )
        if not resolved_reagents:
            continue
        recipe_defs.append(
            {
                "recipe_id": synthetic_recipe_id + item_id,
                "recipe_name": item_name,
                "profession": profession,
                "profession_id": None,
                "crafted_item_id": item_id,
                "crafted_item_name": item_name,
                "crafted_quantity": 1,
                "reagents": resolved_reagents,
            }
        )

    save_cache(cache_path, recipe_cache)

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
            "external_cache_entries": len(recipe_cache),
            "external_hits": external_hits,
            "external_misses": external_misses,
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
