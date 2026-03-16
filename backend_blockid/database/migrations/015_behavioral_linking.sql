-- Wallet link signals (audit trail)
CREATE TABLE IF NOT EXISTS wallet_link_signals (
    id              SERIAL PRIMARY KEY,
    wallet_a        TEXT NOT NULL,
    wallet_b        TEXT NOT NULL,
    signal_type     TEXT NOT NULL,
    signal_strength DOUBLE PRECISION,
    metadata        JSONB,
    detected_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Linking suggestions (pending user confirmation)
CREATE TABLE IF NOT EXISTS wallet_link_suggestions (
    id              SERIAL PRIMARY KEY,
    owner_wallet    TEXT NOT NULL,
    suggested_wallet TEXT NOT NULL,
    confidence      DOUBLE PRECISION,
    signals         TEXT[],
    status          TEXT DEFAULT 'PENDING',
    handle          TEXT,
    suggested_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    responded_at    TIMESTAMP,
    expires_at      TIMESTAMP,
    UNIQUE (owner_wallet, suggested_wallet)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_link_signals_wallet_a
    ON wallet_link_signals (wallet_a);
CREATE INDEX IF NOT EXISTS idx_link_signals_wallet_b
    ON wallet_link_signals (wallet_b);
CREATE INDEX IF NOT EXISTS idx_link_suggestions_owner
    ON wallet_link_suggestions (owner_wallet);
CREATE INDEX IF NOT EXISTS idx_link_suggestions_status
    ON wallet_link_suggestions (status);
