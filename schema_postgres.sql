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
  weighted_avg_unit_price BIGINT
);

CREATE INDEX IF NOT EXISTS idx_obs_item_source_time
  ON observations(item_id, source, metric_name, observed_at);

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
  margin_pct DOUBLE PRECISION
);
