-- Social Layer MVP tables for BlockID

-- Posts
CREATE TABLE IF NOT EXISTS social_posts (
    id              BIGSERIAL PRIMARY KEY,
    wallet          TEXT NOT NULL,
    handle          TEXT,
    content         TEXT NOT NULL,
    image_url       TEXT,
    image_key       TEXT,
    post_type       TEXT DEFAULT 'PUBLIC',
    parent_id       BIGINT REFERENCES social_posts(id),
    reply_count     INTEGER DEFAULT 0,
    like_count      INTEGER DEFAULT 0,
    repost_count    INTEGER DEFAULT 0,
    is_hidden       BOOLEAN DEFAULT FALSE,
    hide_reason     TEXT,
    flag_weight     INTEGER DEFAULT 0,
    trust_score     DOUBLE PRECISION,
    risk_level      TEXT,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Follows
CREATE TABLE IF NOT EXISTS social_follows (
    id               BIGSERIAL PRIMARY KEY,
    follower_wallet  TEXT NOT NULL,
    following_wallet TEXT NOT NULL,
    created_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (follower_wallet, following_wallet)
);

-- Likes
CREATE TABLE IF NOT EXISTS social_likes (
    id        BIGSERIAL PRIMARY KEY,
    post_id   BIGINT NOT NULL REFERENCES social_posts(id),
    wallet    TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (post_id, wallet)
);

-- Flags (community moderation)
CREATE TABLE IF NOT EXISTS social_flags (
    id          BIGSERIAL PRIMARY KEY,
    post_id     BIGINT NOT NULL REFERENCES social_posts(id),
    wallet      TEXT NOT NULL,
    reason      TEXT,
    flag_weight INTEGER DEFAULT 1,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (post_id, wallet)
);

-- Notifications
CREATE TABLE IF NOT EXISTS social_notifications (
    id          BIGSERIAL PRIMARY KEY,
    wallet      TEXT NOT NULL,
    type        TEXT NOT NULL,
    from_wallet TEXT,
    post_id     BIGINT,
    is_read     BOOLEAN DEFAULT FALSE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Endorsements (trust endorsement on-chain reference)
CREATE TABLE IF NOT EXISTS social_endorsements (
    id           BIGSERIAL PRIMARY KEY,
    from_wallet  TEXT NOT NULL,
    to_wallet    TEXT NOT NULL,
    message      TEXT,
    tx_signature TEXT,
    is_active    BOOLEAN DEFAULT TRUE,
    created_at   TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (from_wallet, to_wallet)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_social_posts_wallet
    ON social_posts (wallet);
CREATE INDEX IF NOT EXISTS idx_social_posts_created
    ON social_posts (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_social_posts_parent
    ON social_posts (parent_id);
CREATE INDEX IF NOT EXISTS idx_social_posts_hidden
    ON social_posts (is_hidden);
CREATE INDEX IF NOT EXISTS idx_social_follows_follower
    ON social_follows (follower_wallet);
CREATE INDEX IF NOT EXISTS idx_social_follows_following
    ON social_follows (following_wallet);
CREATE INDEX IF NOT EXISTS idx_social_notifications_wallet
    ON social_notifications (wallet, is_read);

