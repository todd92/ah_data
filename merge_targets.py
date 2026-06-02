#!/usr/bin/env python3
import json
import glob
from pathlib import Path

def main():
    all_t = []
    seen = set()
    
    # Load profession targets
    prof_file = Path("targets_midnight_tailoring_enchanting.json")
    if prof_file.exists():
        data = json.load(prof_file.open())
        for t in data.get("targets", []):
            seen.add(t["item_id"])

    # Load temp world items
    for f in glob.glob("temp_*.json"):
        data = json.load(open(f))["targets"]
        for t in data:
            if t["item_id"] not in seen:
                seen.add(t["item_id"])
                all_t.append(t)

    # Save merged world items
    with open("expansion_world_items.json", "w") as out:
        json.dump({"targets": all_t}, out, indent=2)

if __name__ == "__main__":
    main()
