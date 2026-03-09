-- Add unique constraint for upsert in usage tracking
UPDATE api_usage_hourly SET endpoint = '' WHERE endpoint IS NULL;
ALTER TABLE api_usage_hourly ALTER COLUMN endpoint SET DEFAULT '';
ALTER TABLE api_usage_hourly ALTER COLUMN endpoint SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_api_usage_hourly_key_bucket_endpoint
    ON api_usage_hourly(api_key_id, hour_bucket, endpoint);
