#!/usr/bin/env python3
import argparse
import json
import re
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional


SKILL_TO_PROFESSION = {
    197: "tailoring",
    333: "enchanting",
    2909: "enchanting",
    2913: "inscription",
    2915: "leatherworking",
    2918: "tailoring",
}

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build local Tailoring/Enchanting recipe mappings from Wowhead reagent pages.")
    p.add_argument("--targets-file", default="targets_midnight_tailoring_enchanting.json", help="Targets/watchlist JSON file")
    p.add_argument("--output", default="wowhead_recipe_mappings.json", help="Output mapping JSON file")
    return p.parse_args()


def http_text(url: str) -> str:
    req = urllib.request.Request(
        url=url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    with urllib.request.urlopen(req, timeout=45) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def load_targets(path: Path) -> Dict[int, str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    targets = payload.get("targets", [])
    out: Dict[int, str] = {}
    for row in targets:
        if isinstance(row, dict) and isinstance(row.get("item_id"), int):
            out[int(row["item_id"])] = str(row.get("name") or f"item-{row['item_id']}")
    return out


def extract_reagent_for_spells(page_html: str) -> List[Dict[str, Any]]:
    marker = "id: 'reagent-for'"
    idx = page_html.find(marker)
    if idx < 0:
        return []
    data_idx = page_html.find("data:", idx)
    if data_idx < 0:
        return []
    start = page_html.find("[", data_idx)
    if start < 0:
        return []
    depth = 0
    end = -1
    for i in range(start, len(page_html)):
        ch = page_html[i]
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if end < 0:
        return []
    try:
        data = json.loads(page_html[start:end])
    except json.JSONDecodeError:
        return []
    return data if isinstance(data, list) else []


def mapping_from_spell(spell: Dict[str, Any], known_items: Dict[int, str]) -> Optional[Dict[str, Any]]:
    skill_ids = spell.get("skill")
    if not isinstance(skill_ids, list):
        return None
    profession = None
    for raw in skill_ids:
        if isinstance(raw, int) and raw in SKILL_TO_PROFESSION:
            profession = SKILL_TO_PROFESSION[raw]
            break
    if not profession:
        return None

    creates = spell.get("creates")
    if not isinstance(creates, list) or not creates or not isinstance(creates[0], int):
        return None
    crafted_item_id = int(creates[0])
    crafted_quantity = int(creates[1]) if len(creates) > 1 and isinstance(creates[1], int) else 1

    reagents_raw = spell.get("reagents")
    if not isinstance(reagents_raw, list):
        return None
    reagents: List[Dict[str, Any]] = []
    for reagent in reagents_raw:
        if not isinstance(reagent, list) or len(reagent) < 2:
            return None
        reagent_id, qty = reagent[0], reagent[1]
        if not isinstance(reagent_id, int) or not isinstance(qty, int):
            return None
        reagents.append(
            {
                "item_id": reagent_id,
                "quantity": qty,
                "name": known_items.get(reagent_id, f"item-{reagent_id}"),
            }
        )

    return {
        "recipe_name": str(spell.get("displayName") or spell.get("name") or f"item-{crafted_item_id}"),
        "wowhead_spell_id": spell.get("id"),
        "crafted_item_id": crafted_item_id,
        "crafted_item_name": str(spell.get("displayName") or spell.get("name") or f"item-{crafted_item_id}"),
        "crafted_quantity": crafted_quantity,
        "reagents": reagents,
        "skill": skill_ids,
        "profession": profession,
    }


def collect_mappings(known_items: Dict[int, str]) -> List[Dict[str, Any]]:
    by_output: Dict[int, Dict[str, Any]] = {}
    for item_id, item_name in sorted(known_items.items()):
        url = f"https://www.wowhead.com/item={item_id}"
        try:
            html = http_text(url)
        except Exception:
            continue
        for spell in extract_reagent_for_spells(html):
            if not isinstance(spell, dict):
                continue
            mapping = mapping_from_spell(spell, known_items)
            if not mapping:
                continue
            crafted_item_id = int(mapping["crafted_item_id"])
            by_output[crafted_item_id] = mapping
    return sorted(by_output.values(), key=lambda row: str(row.get("crafted_item_name", "")).lower())


def main() -> int:
    args = parse_args()
    known_items = load_targets(Path(args.targets_file))
    mappings = collect_mappings(known_items)
    out = {"items": mappings}
    out_path = Path(args.output)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"Collected {len(mappings)} mapping(s) from Wowhead reagent pages.")
    for row in mappings[:20]:
        print(f"- {row['profession']}: {row['crafted_item_name']} ({row['crafted_item_id']})")
    print(f"Wrote mappings to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
