-- Content moderation: violations log and posting restrictions

-- Content violations log
CREATE TABLE IF NOT EXISTS content_violations (
    id              BIGSERIAL PRIMARY KEY,
    wallet          TEXT NOT NULL,
    content_preview TEXT,
    violation_level INTEGER NOT NULL,
    action_taken    TEXT,
    trust_penalty   INTEGER DEFAULT 0,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Posting restrictions (rate limit and suspend)
CREATE TABLE IF NOT EXISTS posting_restrictions (
    id               BIGSERIAL PRIMARY KEY,
    wallet           TEXT NOT NULL UNIQUE,
    restriction_type TEXT NOT NULL,
    posts_per_day    INTEGER DEFAULT 20,
    restricted_until TIMESTAMP WITH TIME ZONE,
    reason           TEXT,
    created_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_content_violations_wallet
    ON content_violations (wallet);
CREATE INDEX IF NOT EXISTS idx_posting_restrictions_wallet
    ON posting_restrictions (wallet);
