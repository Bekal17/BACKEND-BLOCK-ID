-- BlockID subscriptions + wallet scan usage (Paddle tiers).
-- If an older `subscriptions` table already exists (e.g. from database/migrations/012_add_subscriptions.sql),
-- resolve schema conflicts manually before applying this file.

CREATE TABLE IF NOT EXISTS subscriptions (
  wallet VARCHAR(64) PRIMARY KEY,
  tier VARCHAR(20) NOT NULL DEFAULT 'FREE',
  paddle_subscription_id VARCHAR(100),
  paddle_customer_id VARCHAR(100),
  paddle_transaction_id VARCHAR(100),
  status VARCHAR(20) NOT NULL DEFAULT 'active',
  current_period_start TIMESTAMP,
  current_period_end TIMESTAMP,
  cancel_at_period_end BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wallet_scan_usage (
  wallet VARCHAR(64) NOT NULL,
  month VARCHAR(7) NOT NULL,  -- format: '2026-03'
  scan_count INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY (wallet, month)
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_tier
  ON subscriptions(tier);
CREATE INDEX IF NOT EXISTS idx_subscriptions_status
  ON subscriptions(status);
CREATE INDEX IF NOT EXISTS idx_scan_usage_wallet_month
  ON wallet_scan_usage(wallet, month);
