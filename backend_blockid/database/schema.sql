-- BlockID Database Schema
-- Bayesian Risk logging
-- Reputation decay tracking on trust_scores

ALTER TABLE trust_scores ADD COLUMN wallet_age_days INTEGER DEFAULT 0;
ALTER TABLE trust_scores ADD COLUMN last_scam_days INTEGER DEFAULT 9999;
ALTER TABLE trust_scores ADD COLUMN decay_adjustment INTEGER DEFAULT 0;

-- Graph distance penalty tracking
ALTER TABLE trust_scores ADD COLUMN graph_distance INTEGER DEFAULT 999;
ALTER TABLE trust_scores ADD COLUMN graph_penalty INTEGER DEFAULT 0;

-- Time-weighted penalty tracking
ALTER TABLE trust_scores ADD COLUMN time_weighted_penalty INTEGER DEFAULT 0;
ALTER TABLE trust_scores ADD COLUMN oldest_risk_days INTEGER DEFAULT 0;

-- Test wallet marker
ALTER TABLE trust_scores ADD COLUMN is_test_wallet INTEGER DEFAULT 0;

CREATE TABLE IF NOT EXISTS wallet_risk_probabilities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    prior REAL,
    posterior REAL,
    reason_code TEXT,
    likelihood REAL,
    confidence REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_wallet_risk_wallet
ON wallet_risk_probabilities(wallet);

-- Wallet metadata cache
CREATE TABLE IF NOT EXISTS wallet_meta (
    wallet TEXT PRIMARY KEY,
    first_tx_ts INTEGER,
    last_tx_ts INTEGER,
    wallet_age_days INTEGER,
    last_scam_tx_ts INTEGER,
    last_scan_time INTEGER,
    cluster_id TEXT,
    is_test_wallet INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS wallet_clusters (
    wallet TEXT,
    cluster_id TEXT,
    PRIMARY KEY(wallet)
);

CREATE TABLE IF NOT EXISTS wallet_flows (
    from_wallet TEXT,
    to_wallet TEXT,
    cluster_id TEXT,
    amount REAL,
    tx_hash TEXT,
    timestamp INTEGER
);
