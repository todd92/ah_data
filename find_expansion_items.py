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

class BlizzardAPI:
    def __init__(self, client_id: str, client_secret: str, region: str, locale: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.region = region
        self.locale = locale
        self.api_host = API_HOSTS.get(region)
        self._access_token: Optional[str] = None

    def _http_json(self, url: str, headers: Dict[str, str]) -> Any:
        req = urllib.request.Request(url=url, headers=headers)
        with urllib.request.urlopen(req, timeout=45) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def access_token(self) -> str:
        if self._access_token: return self._access_token
        auth = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        data = urllib.parse.urlencode({"grant_type": "client_credentials"}).encode()
        req = urllib.request.Request(f"https://{OAUTH_HOST}/token", data=data, headers={"Authorization": f"Basic {auth}"})
        with urllib.request.urlopen(req) as resp:
            payload = json.loads(resp.read().decode())
            self._access_token = payload["access_token"]
            return self._access_token

    def search_items(self, query_params: Dict[str, str]) -> List[Dict[str, Any]]:
        all_results = []
        page = 1
        headers = {"Authorization": f"Bearer {self.access_token()}", "Battlenet-Namespace": f"static-{self.region}"}
        
        while True:
            params = {**query_params, "locale": self.locale, "_page": page, "_pageSize": 1000}
            url = f"https://{self.api_host}/data/wow/search/item?{urllib.parse.urlencode(params)}"
            print(f"Fetching page {page}...", file=sys.stderr)
            data = self._http_json(url, headers)
            results = data.get("results", [])
            all_results.extend([r["data"] for r in results])
            
            if page >= data.get("pageCount", 0):
                break
            page += 1
            time.sleep(0.5)
        return all_results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--expansion-id", type=int, default=11) # Midnight is typically Exp 11
    parser.add_argument("--keyword", default="Midnight")
    parser.add_argument("--output", default="expansion_world_items.json")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = json.load(f)

    api = BlizzardAPI(cfg["client_id"], cfg["client_secret"], cfg.get("region", "us"), cfg.get("locale", "en_US"))
    
    print(f"Searching for items in Expansion {args.expansion_id}...", file=sys.stderr)
    # Search by expansion ID
    items = api.search_items({"expansion.id": str(args.expansion_id)})
    
    # Also search by keyword just in case metadata is missing expansion ID
    print(f"Searching for items with keyword '{args.keyword}'...", file=sys.stderr)
    keyword_items = api.search_items({f"name.{api.locale}": args.keyword})
    
    # Merge and dedupe
    seen_ids = set()
    final_items = []
    for item in items + keyword_items:
        if item["id"] not in seen_ids:
            seen_ids.add(item["id"])
            final_items.append({
                "item_id": item["id"],
                "name": item["name"][api.locale],
                "source_mode": "auto"
            })

    with open(args.output, "w") as f:
        json.dump({"targets": final_items}, f, indent=2)
    
    print(f"Found {len(final_items)} total expansion items.")

if __name__ == "__main__":
    main()
