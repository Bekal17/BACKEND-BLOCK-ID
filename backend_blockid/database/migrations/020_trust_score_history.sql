-- =======================================================
-- Migration 020: Trust Score History + Admin Actions
-- BlockID — AI Behavioral Foundation (Layer 1)
-- =======================================================
-- Jalankan di DBeaver:
-- 1. Copy seluruh isi file ini
-- 2. Paste di DBeaver SQL Editor
-- 3. Run (F5 atau Execute Script)
-- =======================================================

-- TABLE 1: trust_score_history
-- Main audit log — input untuk semua ML Engine (Phase 5A/5B/5C)
-- change_category menentukan bagaimana ML menggunakan data ini:
--   BEHAVIORAL    → Isolation Forest, Z-score, GNN
--   SOCIAL_ACTION → Social trust modeling
--   MODERATION    → "Human proof" signal + excluded dari anomaly
--   LINKING       → Sybil resistance
--   ADMIN         → Excluded dari semua ML (bias)
--   SYSTEM        → DB trigger fallback (no context)

CREATE TABLE IF NOT EXISTS trust_score_history (
    id               SERIAL PRIMARY KEY,
    wallet           VARCHAR NOT NULL,

    score_before     FLOAT,
    score_after      FLOAT NOT NULL,
    delta            FLOAT GENERATED ALWAYS AS (
                         score_after - COALESCE(score_before, 0)
                     ) STORED,

    -- Layer 1 category (kritis untuk ML filtering)
    change_category  VARCHAR NOT NULL,
    -- 'BEHAVIORAL'    → pipeline on-chain (dynamic_risk_v2)
    -- 'SOCIAL_ACTION' → endorse/follow
    -- 'MODERATION'    → content violation penalty
    -- 'LINKING'       → wallet link signals
    -- 'ADMIN'         → manual override (excluded ML)
    -- 'SYSTEM'        → db trigger fallback

    triggered_by     VARCHAR,
    -- 'realtime_pipeline'
    -- 'moderation_engine'
    -- 'social_engine'
    -- 'linking_engine'
    -- 'admin_panel'
    -- 'db_trigger'

    reason_codes     TEXT[],           -- array reason codes aktif
    violation_level  INTEGER,          -- 1-4, MODERATION only
    confidence       FLOAT,            -- 0.0-1.0, LINKING only

    -- Breakdown komponen score (BEHAVIORAL only)
    -- Diisi dari details dict di dynamic_risk_v2.py
    ml_score         FLOAT,            -- base ML prediction
    dynamic_risk     FLOAT,            -- ml + graph + decay + boost
    reason_penalty   FLOAT,            -- dari wallet_reasons
    graph_penalty    FLOAT,            -- scam neighbor penalty
    decay            FLOAT,            -- inactivity decay
    activity_boost   FLOAT,            -- 24h activity boost

    risk_level       VARCHAR,          -- CRITICAL/HIGH/MEDIUM/SAFE
    metadata         JSONB,            -- detail bebas per category
    recorded_at      TIMESTAMP DEFAULT NOW() NOT NULL
);

-- Index untuk ML queries
CREATE INDEX IF NOT EXISTS idx_tsh_wallet
    ON trust_score_history(wallet);

CREATE INDEX IF NOT EXISTS idx_tsh_category
    ON trust_score_history(change_category);

CREATE INDEX IF NOT EXISTS idx_tsh_recorded
    ON trust_score_history(recorded_at);

-- Index paling penting: wallet + time untuk time-series ML
CREATE INDEX IF NOT EXISTS idx_tsh_wallet_time
    ON trust_score_history(wallet, recorded_at DESC);

-- Index untuk filter ML per category per wallet
CREATE INDEX IF NOT EXISTS idx_tsh_wallet_category
    ON trust_score_history(wallet, change_category);


-- =======================================================
-- TABLE 2: admin_actions
-- Audit trail untuk semua aksi manual admin
-- EXCLUDED dari semua ML — manual action = bias
-- =======================================================

CREATE TABLE IF NOT EXISTS admin_actions (
    id           SERIAL PRIMARY KEY,
    wallet       VARCHAR,              -- null kalau global action
    action_type  VARCHAR NOT NULL,
    -- 'SCORE_OVERRIDE'
    -- 'FORCE_RECALCULATE'
    -- 'MANUAL_BAN'
    -- 'MANUAL_UNBAN'
    -- 'WEIGHT_CHANGE'      → reason_weights_optimized.csv update
    -- 'MIGRATION'          → schema change log
    value_before JSONB,
    value_after  JSONB,
    reason       TEXT,
    admin_id     VARCHAR,              -- siapa yang eksekusi
    executed_at  TIMESTAMP DEFAULT NOW() NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_aa_wallet
    ON admin_actions(wallet);

CREATE INDEX IF NOT EXISTS idx_aa_executed
    ON admin_actions(executed_at DESC);

CREATE INDEX IF NOT EXISTS idx_aa_type
    ON admin_actions(action_type);


-- =======================================================
-- TRIGGER: Safety net — catch semua UPDATE trust_scores
-- Berjalan otomatis bahkan kalau Python hook terlewat
-- Category = 'SYSTEM' berarti tidak ada context dari kode
-- ML tetap bisa pakai data ini tapi tanpa breakdown detail
-- =======================================================

CREATE OR REPLACE FUNCTION fn_log_trust_score_change()
RETURNS TRIGGER AS $$
BEGIN
    -- Hanya log kalau score benar-benar berubah
    IF OLD.score IS DISTINCT FROM NEW.score THEN
        INSERT INTO trust_score_history (
            wallet,
            score_before,
            score_after,
            change_category,
            triggered_by,
            risk_level,
            metadata
        ) VALUES (
            NEW.wallet,
            OLD.score,
            NEW.score,
            'SYSTEM',
            'db_trigger',
            NEW.risk_level,
            jsonb_build_object(
                'computed_at', NEW.computed_at,
                'note', 'captured by trigger — no Python context available'
            )
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop trigger lama kalau ada sebelum create
DROP TRIGGER IF EXISTS trg_trust_score_audit ON trust_scores;

CREATE TRIGGER trg_trust_score_audit
    AFTER UPDATE ON trust_scores
    FOR EACH ROW
    EXECUTE FUNCTION fn_log_trust_score_change();


-- =======================================================
-- VERIFY: Cek hasil migration
-- Jalankan query ini setelah migration selesai
-- =======================================================

-- Cek tables
SELECT table_name
FROM information_schema.tables
WHERE table_schema = current_schema()
  AND table_name IN ('trust_score_history', 'admin_actions')
ORDER BY table_name;

-- Cek columns trust_score_history
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = current_schema()
  AND table_name = 'trust_score_history'
ORDER BY ordinal_position;

-- Cek trigger terdaftar
SELECT trigger_name, event_manipulation, event_object_table
FROM information_schema.triggers
WHERE trigger_schema = current_schema()
  AND trigger_name = 'trg_trust_score_audit';
