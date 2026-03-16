-- Handle registry
CREATE TABLE IF NOT EXISTS handle_registry (
    id                  SERIAL PRIMARY KEY,
    handle              TEXT NOT NULL UNIQUE,   -- @bee121 (stored without @)
    owner_wallet        TEXT NOT NULL,          -- current owner wallet
    mint_address        TEXT UNIQUE,            -- handle NFT mint address
    linked_wallets      TEXT[],                 -- verified linked wallets
    status              TEXT DEFAULT 'ACTIVE',
    -- ACTIVE / CHALLENGED / EXPIRED / BURNED
    is_reserved         BOOLEAN DEFAULT FALSE,
    reserved_for        TEXT,                   -- public figure name
    price_paid_usd      DOUBLE PRECISION,
    challenge_expires_at TIMESTAMP,             -- 30 day challenge window
    claimed_at          TIMESTAMP,
    transferred_at      TIMESTAMP,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Wallet linking (opt-in multi-wallet)
CREATE TABLE IF NOT EXISTS handle_wallet_links (
    id              SERIAL PRIMARY KEY,
    handle          TEXT NOT NULL,
    wallet          TEXT NOT NULL,
    is_primary      BOOLEAN DEFAULT FALSE,
    link_status     TEXT DEFAULT 'PENDING',
    -- PENDING / VERIFIED / REJECTED
    ai_confidence   DOUBLE PRECISION,      -- behavioral AI confidence 0-1
    verified_at     TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (handle, wallet)
);

-- Challenge history
CREATE TABLE IF NOT EXISTS handle_challenges (
    id              SERIAL PRIMARY KEY,
    handle          TEXT NOT NULL,
    challenger_wallet TEXT NOT NULL,
    reason          TEXT,
    status          TEXT DEFAULT 'OPEN',
    -- OPEN / RESOLVED_KEEP / RESOLVED_REVOKE
    evidence        TEXT,
    resolved_at     TIMESTAMP,
    expires_at      TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Reserved list
CREATE TABLE IF NOT EXISTS handle_reserved (
    id              SERIAL PRIMARY KEY,
    handle          TEXT NOT NULL UNIQUE,
    reserved_for    TEXT NOT NULL,    -- "Vitalik Buterin"
    category        TEXT,             -- "crypto_founder", "influencer"
    can_claim_wallet TEXT,            -- known wallet if available
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_handle_registry_owner
    ON handle_registry (owner_wallet);
CREATE INDEX IF NOT EXISTS idx_handle_registry_status
    ON handle_registry (status);
CREATE INDEX IF NOT EXISTS idx_handle_wallet_links_wallet
    ON handle_wallet_links (wallet);
CREATE INDEX IF NOT EXISTS idx_handle_wallet_links_handle
    ON handle_wallet_links (handle);
CREATE INDEX IF NOT EXISTS idx_handle_challenges_handle
    ON handle_challenges (handle);
