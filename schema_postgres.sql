CREATE TABLE IF NOT EXISTS observations (
  id BIGSERIAL PRIMARY KEY,
  observed_at TIMESTAMPTZ NOT NULL,
  item_id INTEGER NOT NULL,
  item_name TEXT NOT NULL,
  source TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  metric_value BIGINT NOT NULL,
  listing_count INTEGER NOT NULL,
  total_quantity BIGINT NOT NULL,
  min_unit_price BIGINT,
  max_unit_price BIGINT,
  avg_unit_price BIGINT,
  median_unit_price BIGINT,
  p25_unit_price BIGINT,
  weighted_avg_unit_price BIGINT,
  UNIQUE(observed_at, item_id, source, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_obs_item_source_time
  ON observations(item_id, source, metric_name, observed_at);

CREATE INDEX IF NOT EXISTS idx_obs_observed_at
  ON observations(observed_at);

CREATE TABLE IF NOT EXISTS alerts (
  id BIGSERIAL PRIMARY KEY,
  alerted_at TIMESTAMPTZ NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  item_id INTEGER NOT NULL,
  item_name TEXT NOT NULL,
  source TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  current_value BIGINT NOT NULL,
  mean_value DOUBLE PRECISION NOT NULL,
  stddev_value DOUBLE PRECISION NOT NULL,
  z_score DOUBLE PRECISION NOT NULL,
  direction TEXT NOT NULL,
  alert_kind TEXT NOT NULL DEFAULT 'price_sigma',
  profession TEXT,
  recipe_id INTEGER,
  recipe_name TEXT,
  craft_cost BIGINT,
  sale_value BIGINT,
  expected_profit BIGINT,
  margin_pct DOUBLE PRECISION,
  craft_confidence INTEGER,
  reagent_breakdown JSONB
);

CREATE TABLE IF NOT EXISTS predictions (
  id BIGSERIAL PRIMARY KEY,
  observed_at TIMESTAMPTZ NOT NULL,
  item_id INTEGER NOT NULL,
  item_name TEXT NOT NULL,
  source TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  current_value BIGINT NOT NULL,
  predicted_direction TEXT NOT NULL,
  confidence DOUBLE PRECISION NOT NULL,
  up_score DOUBLE PRECISION NOT NULL,
  down_score DOUBLE PRECISION NOT NULL,
  flat_score DOUBLE PRECISION NOT NULL,
  predicted_return_pct DOUBLE PRECISION NOT NULL,
  short_mean DOUBLE PRECISION NOT NULL,
  medium_mean DOUBLE PRECISION NOT NULL,
  long_mean DOUBLE PRECISION NOT NULL,
  price_vs_long_pct DOUBLE PRECISION NOT NULL,
  short_vs_medium_pct DOUBLE PRECISION NOT NULL,
  quantity_vs_long_pct DOUBLE PRECISION NOT NULL,
  listings_vs_long_pct DOUBLE PRECISION NOT NULL,
  reason TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_predictions_item_source_time
  ON predictions(item_id, source, metric_name, observed_at);

CREATE INDEX IF NOT EXISTS idx_predictions_observed_at
  ON predictions(observed_at);

CREATE TABLE IF NOT EXISTS prediction_cooldowns (
  id BIGSERIAL PRIMARY KEY,
  item_id INTEGER NOT NULL,
  item_name TEXT NOT NULL,
  source TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  predicted_direction TEXT NOT NULL,
  cooldown_until TIMESTAMPTZ NOT NULL,
  reason TEXT NOT NULL,
  trigger_prediction_at TIMESTAMPTZ NOT NULL,
  trigger_confidence DOUBLE PRECISION NOT NULL,
  trigger_return_pct DOUBLE PRECISION NOT NULL,
  realized_return_pct DOUBLE PRECISION NOT NULL,
  UNIQUE(item_id, source, metric_name, predicted_direction, trigger_prediction_at)
);

CREATE INDEX IF NOT EXISTS idx_prediction_cooldowns_lookup
  ON prediction_cooldowns(item_id, source, metric_name, predicted_direction, cooldown_until);
