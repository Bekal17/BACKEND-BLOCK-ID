-- BlockID Identity NFT Phase 1
CREATE TABLE IF NOT EXISTS identity_nft (
    id                  SERIAL PRIMARY KEY,
    wallet              TEXT NOT NULL UNIQUE,
    mint_address        TEXT UNIQUE,
    token_id            TEXT UNIQUE,
    handle              TEXT UNIQUE,
    trust_score         DOUBLE PRECISION,
    risk_level          TEXT,
    badges              TEXT,
    wallet_age_days     INTEGER,
    behavioral_fingerprint TEXT,
    is_sanctioned       BOOLEAN DEFAULT FALSE,
    daemon_risk_score   INTEGER,
    daemon_risk_level   TEXT,
    mint_status         TEXT DEFAULT 'PENDING',
    ineligible_reason   TEXT,
    minted_at           TIMESTAMP,
    last_updated        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_identity_nft_wallet ON identity_nft (wallet);
CREATE INDEX IF NOT EXISTS idx_identity_nft_mint_status ON identity_nft (mint_status);
CREATE INDEX IF NOT EXISTS idx_identity_nft_handle ON identity_nft (handle);
