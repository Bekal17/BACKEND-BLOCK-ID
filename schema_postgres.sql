CREATE TABLE tracked_wallets (
	id INTEGER NOT NULL, 
	wallet VARCHAR(64) NOT NULL, 
	label VARCHAR(256), 
	last_score BIGINT, 
	last_risk VARCHAR(32), 
	last_checked BIGINT, 
	is_active BOOLEAN NOT NULL, 
	reason_codes VARCHAR(1024), created_at TEXT, priority BIGINT, last_analyzed_at BIGINT, confidence_score DOUBLE PRECISION, 
	PRIMARY KEY (id)
);

CREATE TABLE score_history (
	id INTEGER NOT NULL, 
	wallet VARCHAR(64) NOT NULL, 
	score INTEGER NOT NULL, 
	risk VARCHAR(32), 
	timestamp INTEGER NOT NULL, confidence_score DOUBLE PRECISION, risk_level TEXT, reason_codes TEXT, updated_at BIGINT, 
	PRIMARY KEY (id)
);

CREATE TABLE wallet_profiles (
    wallet TEXT PRIMARY KEY,
    first_seen_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    profile_json TEXT,
    created_at BIGINT,
    updated_at INTEGER
, confidence_score DOUBLE PRECISION);

CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    wallet TEXT NOT NULL,
    signature TEXT NOT NULL,
    sender TEXT NOT NULL,
    receiver TEXT NOT NULL,
    amount_lamports INTEGER NOT NULL,
    timestamp BIGINT,
    slot BIGINT,
    created_at BIGINT, confidence_score DOUBLE PRECISION,
    UNIQUE(wallet, signature)
);

CREATE TABLE alerts (
    id SERIAL PRIMARY KEY,
    wallet TEXT NOT NULL,
    severity TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at INTEGER NOT NULL
, confidence_score DOUBLE PRECISION);

CREATE TABLE wallet_rolling_stats (
    id SERIAL PRIMARY KEY,
    wallet TEXT NOT NULL,
    period_end_ts INTEGER NOT NULL,
    window_days INTEGER NOT NULL,
    volume_lamports INTEGER NOT NULL,
    tx_count INTEGER NOT NULL,
    anomaly_count INTEGER NOT NULL,
    avg_trust_score DOUBLE PRECISION,
    alert_count INTEGER NOT NULL,
    created_at INTEGER
, confidence_score DOUBLE PRECISION);

CREATE TABLE wallet_escalation_state (
    wallet TEXT PRIMARY KEY,
    risk_stage TEXT NOT NULL,
    escalation_score DOUBLE PRECISION NOT NULL,
    last_alert_ts BIGINT,
    last_clean_ts BIGINT,
    state_json TEXT,
    updated_at INTEGER NOT NULL
, confidence_score DOUBLE PRECISION);

CREATE TABLE wallet_priority (
    wallet TEXT PRIMARY KEY,
    tier TEXT NOT NULL,
    updated_at INTEGER NOT NULL
, confidence_score DOUBLE PRECISION);

CREATE TABLE wallet_reputation_state (
    wallet TEXT PRIMARY KEY,
    current_score DOUBLE PRECISION NOT NULL,
    avg_7d DOUBLE PRECISION,
    avg_30d DOUBLE PRECISION,
    trend TEXT NOT NULL,
    volatility DOUBLE PRECISION,
    decay_factor DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    updated_at INTEGER NOT NULL
, confidence_score DOUBLE PRECISION);

CREATE TABLE wallet_graph_edges (
    sender_wallet TEXT NOT NULL,
    receiver_wallet TEXT NOT NULL,
    tx_count INTEGER NOT NULL DEFAULT 0,
    total_volume INTEGER NOT NULL DEFAULT 0,
    last_seen_timestamp INTEGER NOT NULL, confidence_score DOUBLE PRECISION,
    PRIMARY KEY (sender_wallet, receiver_wallet)
);

CREATE TABLE wallet_cluster_members (
    cluster_id INTEGER NOT NULL,
    wallet TEXT NOT NULL,
    added_at INTEGER NOT NULL, confidence_score DOUBLE PRECISION,
    PRIMARY KEY (cluster_id, wallet),
    FOREIGN KEY (cluster_id) REFERENCES wallet_clusters(cluster_id)
);

CREATE TABLE entity_profiles (
    entity_id INTEGER PRIMARY KEY,
    cluster_id INTEGER NOT NULL,
    reputation_score DOUBLE PRECISION NOT NULL DEFAULT 50.0,
    risk_history TEXT,
    last_updated INTEGER NOT NULL,
    decay_factor DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    reason_tags TEXT, confidence_score DOUBLE PRECISION,
    FOREIGN KEY (cluster_id) REFERENCES wallet_clusters(cluster_id)
);

CREATE TABLE entity_reputation_history (
    id SERIAL PRIMARY KEY,
    entity_id INTEGER NOT NULL,
    reputation_score DOUBLE PRECISION NOT NULL,
    reason_tags TEXT,
    snapshot_at INTEGER NOT NULL, confidence_score DOUBLE PRECISION,
    FOREIGN KEY (entity_id) REFERENCES entity_profiles(entity_id)
);

CREATE TABLE wallet_reason_evidence (
	id INTEGER NOT NULL, 
	wallet VARCHAR(64) NOT NULL, 
	reason_code VARCHAR(64) NOT NULL, 
	tx_signature VARCHAR(128), 
	counterparty VARCHAR(64), 
	amount VARCHAR(64), 
	token VARCHAR(64), 
	timestamp BIGINT, confidence_score DOUBLE PRECISION, 
	PRIMARY KEY (id)
);

CREATE TABLE wallet_scores (
                    id SERIAL PRIMARY KEY,
                    wallet TEXT,
                    score DOUBLE PRECISION,
                    created_at INTEGER
                , confidence_score DOUBLE PRECISION, risk_level TEXT, reason_codes TEXT, updated_at INTEGER);

CREATE TABLE wallet_reasons (
                id SERIAL PRIMARY KEY,
                wallet TEXT,
                reason_code TEXT,
                weight BIGINT,
                created_at INTEGER
            , confidence_score DOUBLE PRECISION, tx_hash TEXT, tx_link TEXT, code TEXT, confidence DOUBLE PRECISION DEFAULT 1, solscan TEXT);

CREATE TABLE scam_wallets (
        id SERIAL PRIMARY KEY,
        wallet TEXT UNIQUE,
        source TEXT,
        label TEXT,
        detected_at BIGINT,
        notes TEXT
    , confidence_score DOUBLE PRECISION);

CREATE TABLE wallet_clusters (
        id SERIAL PRIMARY KEY,
        wallet TEXT,
        cluster_id BIGINT,
        cluster_type TEXT,
        created_at INTEGER
    , confidence_score DOUBLE PRECISION);

CREATE TABLE wallet_history (
        id SERIAL PRIMARY KEY,
        wallet TEXT,
        score DOUBLE PRECISION,
        risk_level TEXT,
        reason_codes TEXT,
        snapshot_at INTEGER
    , confidence_score DOUBLE PRECISION, updated_at BIGINT, prior DOUBLE PRECISION, posterior DOUBLE PRECISION);

CREATE TABLE wallet_risk_probabilities (
            id SERIAL PRIMARY KEY,
            wallet TEXT,
            prior DOUBLE PRECISION,
            posterior DOUBLE PRECISION,
            reason_code TEXT,
            likelihood DOUBLE PRECISION,
            confidence DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

CREATE TABLE wallet_meta (
            wallet TEXT PRIMARY KEY,
            first_tx_ts BIGINT,
            last_tx_ts BIGINT,
            wallet_age_days BIGINT,
            last_scam_tx_ts BIGINT,
            last_scan_time BIGINT,
            cluster_id TEXT,
            is_test_wallet INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

CREATE TABLE wallet_last_update (
            wallet TEXT PRIMARY KEY,
            timestamp INTEGER NOT NULL
        );

CREATE TABLE wallet_badges (
            id SERIAL PRIMARY KEY,
            wallet TEXT NOT NULL,
            badge TEXT NOT NULL,
            timestamp INTEGER NOT NULL
        );

CREATE TABLE helius_usage (
            id SERIAL PRIMARY KEY,
            timestamp INTEGER NOT NULL,
            endpoint TEXT NOT NULL,
            wallet TEXT NOT NULL,
            request_count INTEGER NOT NULL DEFAULT 1,
            estimated_cost DOUBLE PRECISION NOT NULL DEFAULT 0
        );

CREATE TABLE wallet_scan_meta (
            wallet TEXT PRIMARY KEY,
            last_scan_ts INTEGER NOT NULL
        );

CREATE TABLE pipeline_run_log (
            id SERIAL PRIMARY KEY,
            run_start_ts INTEGER NOT NULL,
            run_end_ts BIGINT,
            success INTEGER DEFAULT 0,
            wallets_scanned INTEGER DEFAULT 0,
            errors_count INTEGER DEFAULT 0,
            steps_completed INTEGER DEFAULT 0,
            message TEXT
        );

CREATE TABLE review_queue (
                wallet TEXT PRIMARY KEY,
                score DOUBLE PRECISION,
                confidence DOUBLE PRECISION,
                risk BIGINT,
                reasons TEXT,
                created_at BIGINT,
                status TEXT DEFAULT 'pending'
            );

CREATE TABLE priority_wallets (
            wallet TEXT PRIMARY KEY,
            priority BIGINT,
            reason TEXT,
            hop_distance BIGINT,
            last_checked INTEGER
        , last_tx_time BIGINT, tx_count INTEGER DEFAULT 0);

CREATE TABLE schema_migrations (
            id SERIAL PRIMARY KEY,
            filename TEXT UNIQUE,
            applied_at INTEGER
        );

CREATE TABLE blockid_logs (
            id SERIAL PRIMARY KEY,
            timestamp BIGINT,
            stage TEXT,
            status TEXT,
            message TEXT,
            latency_ms BIGINT,
            wallet TEXT
        );

CREATE TABLE trust_scores (
    id SERIAL PRIMARY KEY,
    wallet TEXT,
    score DOUBLE PRECISION,
    computed_at BIGINT,
    metadata_json TEXT,
    created_at BIGINT,
    risk_level TEXT,
    reason_codes TEXT,
    updated_at BIGINT,
    confidence_score DOUBLE PRECISION,
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
    dynamic_risk DOUBLE PRECISION DEFAULT 0,
    final_score DOUBLE PRECISION DEFAULT 0,
    last_updated BIGINT,
    ml_score DOUBLE PRECISION DEFAULT 0
);

