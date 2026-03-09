CREATE TABLE tracked_wallets (
	id INTEGER NOT NULL, 
	wallet VARCHAR(64) NOT NULL, 
	label VARCHAR(256), 
	last_score INTEGER, 
	last_risk VARCHAR(32), 
	last_checked INTEGER, 
	is_active BOOLEAN NOT NULL, 
	reason_codes VARCHAR(1024), created_at TEXT, priority INTEGER, last_analyzed_at INTEGER, confidence_score REAL, 
	PRIMARY KEY (id)
);

CREATE TABLE score_history (
	id INTEGER NOT NULL, 
	wallet VARCHAR(64) NOT NULL, 
	score INTEGER NOT NULL, 
	risk VARCHAR(32), 
	timestamp INTEGER NOT NULL, confidence_score REAL, risk_level TEXT, reason_codes TEXT, updated_at INTEGER, 
	PRIMARY KEY (id)
);

CREATE TABLE wallet_profiles (
    wallet TEXT PRIMARY KEY,
    first_seen_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    profile_json TEXT,
    created_at INTEGER,
    updated_at INTEGER
, confidence_score REAL);

CREATE TABLE transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    signature TEXT NOT NULL,
    sender TEXT NOT NULL,
    receiver TEXT NOT NULL,
    amount_lamports INTEGER NOT NULL,
    timestamp INTEGER,
    slot INTEGER,
    created_at INTEGER, confidence_score REAL,
    UNIQUE(wallet, signature)
);

CREATE TABLE alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    severity TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at INTEGER NOT NULL
, confidence_score REAL);

CREATE TABLE wallet_rolling_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    period_end_ts INTEGER NOT NULL,
    window_days INTEGER NOT NULL,
    volume_lamports INTEGER NOT NULL,
    tx_count INTEGER NOT NULL,
    anomaly_count INTEGER NOT NULL,
    avg_trust_score REAL,
    alert_count INTEGER NOT NULL,
    created_at INTEGER
, confidence_score REAL);

CREATE TABLE wallet_escalation_state (
    wallet TEXT PRIMARY KEY,
    risk_stage TEXT NOT NULL,
    escalation_score REAL NOT NULL,
    last_alert_ts INTEGER,
    last_clean_ts INTEGER,
    state_json TEXT,
    updated_at INTEGER NOT NULL
, confidence_score REAL);

CREATE TABLE wallet_priority (
    wallet TEXT PRIMARY KEY,
    tier TEXT NOT NULL,
    updated_at INTEGER NOT NULL
, confidence_score REAL);

CREATE TABLE wallet_reputation_state (
    wallet TEXT PRIMARY KEY,
    current_score REAL NOT NULL,
    avg_7d REAL,
    avg_30d REAL,
    trend TEXT NOT NULL,
    volatility REAL,
    decay_factor REAL NOT NULL DEFAULT 1.0,
    updated_at INTEGER NOT NULL
, confidence_score REAL);

CREATE TABLE wallet_graph_edges (
    sender_wallet TEXT NOT NULL,
    receiver_wallet TEXT NOT NULL,
    tx_count INTEGER NOT NULL DEFAULT 0,
    total_volume INTEGER NOT NULL DEFAULT 0,
    last_seen_timestamp INTEGER NOT NULL, confidence_score REAL,
    PRIMARY KEY (sender_wallet, receiver_wallet)
);

CREATE TABLE wallet_cluster_members (
    cluster_id INTEGER NOT NULL,
    wallet TEXT NOT NULL,
    added_at INTEGER NOT NULL, confidence_score REAL,
    PRIMARY KEY (cluster_id, wallet),
    FOREIGN KEY (cluster_id) REFERENCES wallet_clusters(cluster_id)
);

CREATE TABLE entity_profiles (
    entity_id INTEGER PRIMARY KEY,
    cluster_id INTEGER NOT NULL,
    reputation_score REAL NOT NULL DEFAULT 50.0,
    risk_history TEXT,
    last_updated INTEGER NOT NULL,
    decay_factor REAL NOT NULL DEFAULT 1.0,
    reason_tags TEXT, confidence_score REAL,
    FOREIGN KEY (cluster_id) REFERENCES wallet_clusters(cluster_id)
);

CREATE TABLE entity_reputation_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id INTEGER NOT NULL,
    reputation_score REAL NOT NULL,
    reason_tags TEXT,
    snapshot_at INTEGER NOT NULL, confidence_score REAL,
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
	timestamp INTEGER, confidence_score REAL, 
	PRIMARY KEY (id)
);

CREATE TABLE wallet_scores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    wallet TEXT,
                    score REAL,
                    created_at INTEGER
                , confidence_score REAL, risk_level TEXT, reason_codes TEXT, updated_at INTEGER);

CREATE TABLE wallet_reasons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet TEXT,
                reason_code TEXT,
                weight INTEGER,
                created_at INTEGER
            , confidence_score REAL, tx_hash TEXT, tx_link TEXT, code TEXT, confidence REAL DEFAULT 1, solscan TEXT);

CREATE TABLE scam_wallets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wallet TEXT UNIQUE,
        source TEXT,
        label TEXT,
        detected_at INTEGER,
        notes TEXT
    , confidence_score REAL);

CREATE TABLE wallet_clusters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wallet TEXT,
        cluster_id INTEGER,
        cluster_type TEXT,
        created_at INTEGER
    , confidence_score REAL);

CREATE TABLE wallet_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wallet TEXT,
        score REAL,
        risk_level TEXT,
        reason_codes TEXT,
        snapshot_at INTEGER
    , confidence_score REAL, updated_at INTEGER, prior REAL, posterior REAL);

CREATE TABLE wallet_risk_probabilities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT,
            prior REAL,
            posterior REAL,
            reason_code TEXT,
            likelihood REAL,
            confidence REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

CREATE TABLE wallet_meta (
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

CREATE TABLE wallet_last_update (
            wallet TEXT PRIMARY KEY,
            timestamp INTEGER NOT NULL
        );

CREATE TABLE wallet_badges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT NOT NULL,
            badge TEXT NOT NULL,
            timestamp INTEGER NOT NULL
        );

CREATE TABLE helius_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            endpoint TEXT NOT NULL,
            wallet TEXT NOT NULL,
            request_count INTEGER NOT NULL DEFAULT 1,
            estimated_cost REAL NOT NULL DEFAULT 0
        );

CREATE TABLE wallet_scan_meta (
            wallet TEXT PRIMARY KEY,
            last_scan_ts INTEGER NOT NULL
        );

CREATE TABLE pipeline_run_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_start_ts INTEGER NOT NULL,
            run_end_ts INTEGER,
            success INTEGER DEFAULT 0,
            wallets_scanned INTEGER DEFAULT 0,
            errors_count INTEGER DEFAULT 0,
            steps_completed INTEGER DEFAULT 0,
            message TEXT
        );

CREATE TABLE review_queue (
                wallet TEXT PRIMARY KEY,
                score REAL,
                confidence REAL,
                risk INTEGER,
                reasons TEXT,
                created_at INTEGER,
                status TEXT DEFAULT 'pending'
            );

CREATE TABLE priority_wallets (
            wallet TEXT PRIMARY KEY,
            priority INTEGER,
            reason TEXT,
            hop_distance INTEGER,
            last_checked INTEGER
        , last_tx_time INTEGER, tx_count INTEGER DEFAULT 0);

CREATE TABLE schema_migrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE,
            applied_at INTEGER
        );

CREATE TABLE blockid_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            stage TEXT,
            status TEXT,
            message TEXT,
            latency_ms INTEGER,
            wallet TEXT
        );

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

