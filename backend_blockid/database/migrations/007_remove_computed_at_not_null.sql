ALTER TABLE trust_scores RENAME TO trust_scores_old;

CREATE TABLE trust_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT,
    score REAL,
    computed_at INTEGER,
    metadata_json TEXT,
    created_at INTEGER,
    risk_level TEXT,
    reason_codes TEXT,
    updated_at INTEGER,
    confidence_score REAL,
    model_version TEXT,
    publisher TEXT,
    wallet_age_days INTEGER DEFAULT 0,
    last_scam_days INTEGER DEFAULT 9999,
    decay_adjustment INTEGER DEFAULT 0,
    graph_distance INTEGER DEFAULT 999,
    graph_penalty INTEGER DEFAULT 0,
    time_weighted_penalty INTEGER DEFAULT 0,
    is_test_wallet INTEGER DEFAULT 0,
    oldest_risk_days INTEGER DEFAULT 0,
    dynamic_risk REAL DEFAULT 0,
    final_score REAL DEFAULT 0,
    last_updated INTEGER,
    ml_score REAL DEFAULT 0
);

INSERT INTO trust_scores
SELECT * FROM trust_scores_old;

DROP TABLE trust_scores_old;
