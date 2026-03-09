# WoW Auction House Scraper Starter

This starter pulls Auction House prices from the Blizzard Game Data API for selected items and supports:
- `connected_realm` sources (server group)
- `realm` sources (single server slug, auto-resolved to its connected realm)
- `commodity` source (region-wide commodity auction data)
- `auto` source mode (uses your default realm for non-reagents and commodity feed for reagents/trade goods)
- `targets_file` support (load large expansion/profession target lists)

## 1) Create Blizzard API credentials
1. Log into the Blizzard developer portal.
2. Create an API client with `client_credentials` access.
3. Copy your `client_id` and `client_secret`.

## 2) Configure targets
Copy the example config and edit it:

```bash
cp config.example.json config.json
```

Fill in credentials and your item/source targets.

If you set:
- `default_realm_slug` (example: `dawnbringer`)
- target `source_mode` to `auto`

then the script will:
- treat reagent/trade-goods items as commodity queries (`commodity:region`)
- treat non-reagents as realm-scoped using your default realm slug

## 3) Build Midnight profession targets (Tailoring + Enchanting)

Generate target items directly from Blizzard profession/recipe APIs:

```bash
python3 build_profession_watchlist.py \
  --config config.json \
  --expansion-keyword midnight \
  --professions tailoring,enchanting \
  --include-reagents \
  --output targets_midnight_tailoring_enchanting.json
```

This writes a `targets` JSON file that the scraper can load via `targets_file`.

Shortcut with Make:

```bash
make watchlist
```

## 4) Run AH report

```bash
python3 wow_ah_scraper.py --config config.json --output report.json
```

Shortcut with Make:

```bash
make report
```

## 5) Store snapshots + sigma alerts

Run the monitoring pipeline (scrape -> store in SQLite -> check 7-day anomalies):

```bash
python3 ah_monitor.py \
  --config config.json \
  --report report.json \
  --db ah_prices.sqlite3 \
  --metric weighted_avg_unit_price \
  --signal-direction both \
  --window-hours 168 \
  --sigma 2.0 \
  --min-history 24 \
  --trend-hours 48 \
  --min-trend-history 6 \
  --min-listings-commodity 8 \
  --min-quantity-commodity 200 \
  --min-listings-crafted 2 \
  --min-quantity-crafted 1 \
  --min-abs-move-gold-commodity 20 \
  --min-abs-move-gold-crafted 100 \
  --enable-craft-alerts \
  --craft-ah-cut-rate 0.05 \
  --craft-min-profit-gold 50 \
  --craft-min-margin-pct 0.10 \
  --retention-days-observations 30 \
  --retention-days-alerts 90
```

Shortcut with Make:

```bash
make monitor-live
```

Retention notes:
- `--retention-days-observations 30` keeps only the most recent 30 days of snapshots.
- `--retention-days-alerts 90` keeps 90 days of emitted alerts for review.
- Set either value to `0` to disable pruning for that table.

Crafting arbitrage notes:
- `build_profession_watchlist.py` now writes recipe definitions to the watchlist JSON under `recipes`.
- `--enable-craft-alerts` uses those recipes to estimate `sale_value`, `craft_cost`, and `expected_profit`.
- Profitable crafts emit BUY alerts; strongly negative crafts emit SELL alerts.
- Craft alerts are stored in the same `alerts` table with `alert_kind='craft_arbitrage'`.
- Craft rows include both `recipe_id` and `recipe_name`.

## 6) Supabase Setup

Install dependencies (preferred: `uv`):

```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

Fallback with `pip`:

```bash
python3 -m pip install -r requirements.txt
```

In Supabase:
1. Open SQL Editor.
2. Run [schema_postgres.sql](/home/toddglad/projects/personal/ah_data/schema_postgres.sql).
3. Copy your Postgres connection string.

Set environment:

```bash
export DATABASE_URL='postgresql://postgres:<password>@<host>:5432/postgres'
```

Then run the same monitor command. With `DATABASE_URL` set, `ah_monitor.py` writes to Supabase instead of local SQLite.

Run commands with `uv`:

```bash
uv run python ah_monitor.py --config config.json --report report.json --db ah_prices.sqlite3
```

Optional webhook ping (Slack/Discord):

```bash
export AH_ALERT_WEBHOOK_URL=\"https://...\"
uv run python ah_monitor.py --config config.json --webhook-format slack
```

Hourly scheduling with cron:

```bash
crontab -e
```

```cron
0 * * * * cd /home/toddglad/projects/personal/ah_data && /usr/bin/env -S bash -lc 'uv run python ah_monitor.py --config config.json --report report.json --db ah_prices.sqlite3 >> monitor.log 2>&1'
```

## Output
`report.json` includes each target item with a per-source summary:
- `listing_count`
- `total_quantity`
- `min_unit_price`
- `max_unit_price`
- `avg_unit_price`
- `weighted_avg_unit_price`

All prices are in copper.
Alert messages display prices as gold/silver/copper (`Xg Ys Zc`) for readability.

`ah_prices.sqlite3` stores:
- `observations` table: one row per item/source snapshot
- `alerts` table: z-score outliers detected per run

When using Supabase, the same tables are created in Postgres (`observations`, `alerts`).

## Signal Logic
- Baseline: 7-day window (`--window-hours 168`), alert threshold `|z| >= 2`.
- Liquidity filters:
- `listing_count`: how many active auctions exist right now.
- `total_quantity`: total stack quantity available right now.
- Low liquidity means one or two postings can fake a huge \"opportunity\" that you cannot actually enter/exit at scale.
- Trend filter:
- BUY signals require current price to not be deeply below the 48h average (helps avoid falling knives).
- SELL signals require current price to still show strength vs the 48h average.
- Use `--signal-direction buy|sell|both` to emit only BUY, only SELL, or both signal types.
- Minimum absolute move:
- Prevents tiny price changes from alerting just because volatility is very low.

## Notes
- For Retail, many realms are part of a connected-realm group; auctions for non-commodities are effectively group-level.
- If you use `realm` sources, the script resolves the realm slug to connected-realm ID once and then fetches that auction feed.
- In Blizzard's API, reagent/trade-goods style listings are exposed as commodities (region-wide), not just one connected-realm group.
- Auction payloads can be large (10MB+). Keep target lists focused for faster runs.

## Web UI (Vercel)

A deployable Next.js app now lives in [`web/`](/home/toddglad/projects/personal/ah_data/web/README.md).

Quick deploy steps:
1. Import this repository into Vercel.
2. Set project root directory to `web`.
3. Add env vars `SUPABASE_URL` and `SUPABASE_ANON_KEY`.
4. Deploy.

The UI reads `craft_arbitrage` rows from `alerts` and supports filter-driven ranking by profit and margin.
