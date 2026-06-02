.PHONY: help setup watchlist report monitor monitor-live sync-gdrive predict-archive backtest-archive

UV ?= uv
PY ?= python

CONFIG ?= config.json
REPORT ?= report.json
DB ?= data/ah_prices.sqlite3
WATCHLIST ?= targets_midnight_tailoring_enchanting.json

EXPANSION_KEYWORD ?= midnight
# Expanded to all Midnight professions
PROFESSIONS ?= tailoring,enchanting,alchemy,blacksmithing,engineering,inscription,jewelcrafting,leatherworking,cooking,herbalism,mining,skinning,fishing

MONITOR_ARGS ?= --metric weighted_avg_unit_price --window-hours 168 --sigma 2.0 --min-history 24 --trend-hours 48 --min-trend-history 6 --min-listings-commodity 8 --min-quantity-commodity 200 --min-listings-crafted 2 --min-quantity-crafted 1 --min-abs-move-gold-commodity 20 --min-abs-move-gold-crafted 100 --retention-days-observations 30 --retention-days-alerts 90 --enable-predictions --prediction-window-hours 168 --prediction-short-window-hours 12 --prediction-medium-window-hours 48 --prediction-min-history 24 --prediction-min-confidence 0.80 --prediction-cooldown-hours 24 --prediction-cooldown-horizon-hours 12 --prediction-cooldown-grace-hours 6 --prediction-cooldown-min-confidence 0.85 --prediction-cooldown-loss-pct 20 --enable-craft-alerts --craft-ah-cut-rate 0.05 --craft-min-profit-gold 50 --craft-min-margin-pct 0.10 --all-commodities
BACKTEST_ARGS ?= --metric weighted_avg_unit_price --horizon-hours 12 --grace-hours 6 --min-actual-move-pct 2.0 --min-confidence 0.80 --prediction-window-hours 168 --prediction-short-window-hours 12 --prediction-medium-window-hours 48 --prediction-min-history 24 --prediction-min-short-history 6 --min-listings-commodity 8 --min-quantity-commodity 200 --min-listings-crafted 2 --min-quantity-crafted 1

help:
	@echo "Targets:"
	@echo "  make setup         # create venv + install deps with uv"
	@echo "  make watchlist     # rebuild Midnight targets for all professions"
	@echo "  make report        # run AH scraper and write report.json"
	@echo "  make monitor       # ingest existing report into local SQLite DB"
	@echo "  make monitor-live  # watchlist refresh + scrape + ingest into local SQLite DB"
	@echo "  make sync-gdrive   # Sync SQLite DB to Google Drive (requires rclone setup)"
	@echo "  make predict-archive # run prediction pass against archive DB only"
	@echo "  make backtest-archive # backtest Phase 1 predictions against archive DB"

setup:
	$(UV) venv
	$(UV) pip install -r requirements.txt

watchlist:
	$(if $(UV),$(UV) run) $(PY) build_profession_watchlist.py \
	  --config $(CONFIG) \
	  --expansion-keyword $(EXPANSION_KEYWORD) \
	  --professions $(PROFESSIONS) \
	  --include-reagents \
	  --output $(WATCHLIST)
	@echo "Collecting comprehensive world items..."
	@for kw in Midnight Silvermoon Thalassian Sindorei Haranir Amani Eversong Sunwell Farstrider Spellbreaker Aetherlume Sunforged Arcanoweave Magister Sunfire; do \
		echo "Searching for $$kw..."; \
		$(if $(UV),$(UV) run) $(PY) find_expansion_items.py --config $(CONFIG) --expansion-id 11 --keyword "$$kw" --output "temp_$$kw.json"; \
	done
	$(if $(UV),$(UV) run) $(PY) merge_targets.py
	@rm temp_*.json
	@echo "Watchlist rebuild complete."

report:
	$(if $(UV),$(UV) run) $(PY) wow_ah_scraper.py --config $(CONFIG) --output $(REPORT) --all-commodities

monitor:
	$(if $(UV),$(UV) run) $(PY) ah_monitor.py --config $(CONFIG) --report $(REPORT) --db $(DB) --ingest-only $(MONITOR_ARGS)

monitor-live:
	$(if $(UV),$(UV) run) $(PY) ah_monitor.py --config $(CONFIG) --report $(REPORT) --db $(DB) --refresh-watchlist --include-reagents --watchlist-output $(WATCHLIST) --expansion-keyword $(EXPANSION_KEYWORD) --professions $(PROFESSIONS) $(MONITOR_ARGS)

# Requires rclone to be configured with a remote named 'gdrive'
# Adjust the destination path as needed.
sync-gdrive:
	@echo "Syncing $(DB) to Google Drive..."
	rclone copy $(DB) gdrive:ah_data/
	@echo "Done."

predict-archive:
	$(UV) run $(PY) ah_monitor.py --config $(CONFIG) --report $(REPORT) --db $(DB) --ingest-only --enable-predictions --prediction-top-n 15

backtest-archive:
	$(UV) run $(PY) backtest_predictions.py --db $(DB) $(BACKTEST_ARGS)
