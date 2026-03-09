-- API usage hourly aggregation for usage dashboard
CREATE TABLE IF NOT EXISTS api_usage_hourly (
    id SERIAL PRIMARY KEY,
    api_key_id UUID NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
    hour_bucket TIMESTAMPTZ NOT NULL,
    request_count INTEGER NOT NULL DEFAULT 0,
    success_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    avg_response_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
    endpoint TEXT
);

CREATE INDEX IF NOT EXISTS ix_api_usage_hourly_api_key_id ON api_usage_hourly(api_key_id);
CREATE INDEX IF NOT EXISTS ix_api_usage_hourly_hour_bucket ON api_usage_hourly(hour_bucket);
