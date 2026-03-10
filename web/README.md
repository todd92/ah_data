# AH Crafting Radar Web

Next.js web UI for browsing crafting opportunities from the `alerts` table in Supabase.

## Local setup

1. Copy env file:

```bash
cp .env.example .env.local
```

2. Install and run:

```bash
npm install
npm run dev
```

3. Open http://localhost:3000

If Supabase env vars are missing, the UI serves sample data so you can still test the interface.

## Required env vars

- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`

## Vercel deploy

1. Import this repo in Vercel.
2. Set **Root Directory** to `web`.
3. Add env vars:
   - `SUPABASE_URL`
   - `SUPABASE_ANON_KEY`
4. Deploy.

## Data contract

The API reads rows from `alerts` where:

- `alert_kind = 'craft_arbitrage'`

Expected columns used by UI:

- `alerted_at`, `observed_at`
- `item_id`, `item_name`, `source`
- `direction`
- `profession`
- `recipe_id`, `recipe_name`
- `craft_cost`, `sale_value`, `expected_profit`, `margin_pct`
