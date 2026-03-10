.PHONY: help setup watchlist report monitor monitor-live monitor-supabase check-database-url

UV ?= uv
PY ?= python

CONFIG ?= config.json
REPORT ?= report.json
DB ?= ah_prices.sqlite3
WATCHLIST ?= targets_midnight_tailoring_enchanting.json

EXPANSION_KEYWORD ?= midnight
PROFESSIONS ?= tailoring,enchanting

MONITOR_ARGS ?= --metric weighted_avg_unit_price --window-hours 168 --sigma 2.0 --min-history 24 --trend-hours 48 --min-trend-history 6 --min-listings-commodity 8 --min-quantity-commodity 200 --min-listings-crafted 2 --min-quantity-crafted 1 --min-abs-move-gold-commodity 20 --min-abs-move-gold-crafted 100 --retention-days-observations 30 --retention-days-alerts 90 --enable-craft-alerts --craft-ah-cut-rate 0.05 --craft-min-profit-gold 50 --craft-min-margin-pct 0.10

help:
	@echo "Targets:"
	@echo "  make setup         # create venv + install deps with uv"
	@echo "  make watchlist     # rebuild Midnight tailoring/enchanting targets"
	@echo "  make report        # run AH scraper and write report.json"
	@echo "  make monitor       # ingest existing report + anomaly detection"
	@echo "  make monitor-live  # watchlist refresh + scrape + ingest + alerts"
	@echo "  make monitor-supabase # same as monitor-live, requires DATABASE_URL"

setup:
	$(UV) venv
	$(UV) pip install -r requirements.txt

watchlist:
	$(UV) run $(PY) build_profession_watchlist.py \
	  --config $(CONFIG) \
	  --expansion-keyword $(EXPANSION_KEYWORD) \
	  --professions $(PROFESSIONS) \
	  --include-reagents \
	  --output $(WATCHLIST)

report:
	$(UV) run $(PY) wow_ah_scraper.py --config $(CONFIG) --output $(REPORT)

monitor:
	$(UV) run $(PY) ah_monitor.py --config $(CONFIG) --report $(REPORT) --db $(DB) --ingest-only $(MONITOR_ARGS)

monitor-live:
	$(UV) run $(PY) ah_monitor.py --config $(CONFIG) --report $(REPORT) --db $(DB) --refresh-watchlist --include-reagents --watchlist-output $(WATCHLIST) --expansion-keyword $(EXPANSION_KEYWORD) --professions $(PROFESSIONS) $(MONITOR_ARGS)

check-database-url:
	@test -n "$$DATABASE_URL" || (echo "ERROR: DATABASE_URL is not set. Export it first." && exit 1)

monitor-supabase: check-database-url
	$(UV) run $(PY) ah_monitor.py --config $(CONFIG) --report $(REPORT) --database-url "$$DATABASE_URL" --refresh-watchlist --include-reagents --watchlist-output $(WATCHLIST) --expansion-keyword $(EXPANSION_KEYWORD) --professions $(PROFESSIONS) $(MONITOR_ARGS)
