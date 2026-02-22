# BlockID Specification (Auto-Generated)

## 1. Project Structure

```
backend_blockid/
  ├── agent_worker/
  │   ├── __init__.py
  │   ├── health.py | future component monitoring worker
  │   ├── priority_scheduler.py | future component queue job
  │   ├── runner.py | future component worker loop
  │   ├── runtime.py
  │   ├── tasks.py | future component definition job
  │   └── worker.py
  ├── ai_engine/
  │   ├── oracle/ | publish trust score to PDA
  │   │   ├── __init__.py
  │   │   └── solana_publisher.py
  │   └── __init__.py
  ├── alerts/ 
  │   ├── __init__.py
  │   ├── engine.py | rule alert
  │   └── escalation.py | notif admin blockidscore@gmail.com
  ├── analysis_engine/
  │   ├── __init__.py
  │   ├── anomaly.py
  │   ├── entity_reputation.py
  │   ├── features.py | feature engineering
  │   ├── graph.py | graph clustering
  │   ├── identity_cluster.py | wallet clustering
  │   ├── models.py
  │   ├── reputation_memory.py
  │   ├── risk_propagation.py | risk dari cluster
  │   ├── scorer.py | count trust score
  │   └── signals.py
  ├── analytics/
  │   ├── __init__.py
  │   ├── analytics_pipeline.py
  │   ├── nft_scam_detector.py | Experimental modules
  │   ├── risk_engine.py
  │   ├── rugpull_detector.py | Experimental modules
  │   ├── scam_detector.py
  │   ├── trust_engine.py
  │   ├── wallet_classifier.py | Experimental modules
  │   ├── wallet_graph.py
  │   └── wallet_scanner.py
  ├── api_server/
  │   ├── __init__.py
  │   ├── app.py | FastAPI app
  │   ├── db_wallet_tracking.py | wallet tracking DB
  │   ├── middleware.py
  │   ├── routes.py
  │   ├── server.py
  │   ├── trust_score.py | API trust score
  │   └── trust_score_sync.py | sync on-chain
  ├── behavioral_memory/ | future component.
  │   ├── __init__.py
  │   ├── engine.py
  │   └── models.py
  ├── blockid_logging/
  │   ├── __init__.py
  │   └── logger.py
  ├── config/
  │   ├── __init__.py
  │   ├── env.py
  │   └── settings.py
  ├── core/
  │   ├── __init__.py
  │   ├── exceptions.py
  │   └── logging_utils.py
  ├── data/
  │   ├── manual_wallets.csv
  │   ├── scam_wallets.csv
  │   ├── suspicious_tokens.csv
  │   ├── test_wallets_100.csv
  │   ├── token_features.csv
  │   ├── transactions.csv
  │   └── wallet_scores.csv
  ├── database/ | wallet tracking.
  │   ├── migrations/
  │   │   └── add_tracked_wallets_reason_codes.py
  │   ├── __init__.py
  │   ├── connection.py
  │   ├── database.py
  │   ├── models.py
  │   └── repositories.py
 ... (truncated)
```

## 2. Oracle Scripts

| Script |
|--------|
| __init__.py | module init | Used
| auto_insert_scam_wallets.py | add scam wallet | Used |
| devnet_test.py | test deploy | Debug only |
| drainer_detection.py | STEP 3 drainer detect | Used |
| fetch_tx_edges_helius.py |  STEP 0 collect tx | Used |
| flow_features.py | STEP 2 flow analysis | Used |
| get_100_wallets_bonk.py | sampling | Optional |
| get_100_wallets_from_helius.py |batch publish | Used |
| graph_clustering.py | STEP 1 clustering | Used |
| helius_batch_token_scanner.py | token metadata scan | Experimental
| helius_extract_fields.py | parse Helius data | Used |
| publish_one_wallet.py | test publisher | Debug only |
| publish_scores.py | STEP 5 publish PDA | Used |
| publish_wallet_scores.py | batch publish | Used |
| scan_100_tokens_helius.py | token scan | Experimental|
| solana_publisher.py | PDA writer | Used
| test_helius_asset.py | Helius test | Debug only |
| trust_oracle.py | wrapper oracle | Used |

ai_engine/oracle/solana_publisher.py → high-level wrapper
backend_blockid/oracle/solana_publisher.py → Anchor transaction sender

**Anchor publisher scripts:**

- `backend_blockid/api_server/db_wallet_tracking.py`
- `backend_blockid/api_server/server.py`
- `backend_blockid/api_server/trust_score.py`
- `backend_blockid/api_server/trust_score_sync.py`
- `backend_blockid/ml/feature_extractor.py`
- `backend_blockid/oracle/__init__.py`
- `backend_blockid/oracle/devnet_test.py`
- `backend_blockid/oracle/publish_one_wallet.py`
- `backend_blockid/oracle/publish_scores.py`
- `backend_blockid/oracle/publish_wallet_scores.py`
- `backend_blockid/oracle/solana_publisher.py`

## 3. ML Models

| Script |
|--------|
| __init__.py |
| analyze_dataset.py |
| build_dataset.py |
| feature_builder.py |
| feature_extractor.py |
| predict.py |
| predict_wallet_score.py |
| predictor.py |
| test_predictor.py |
| train_blockid_model.py |
| train_model.py |
| train_token_scam_model.py |

| Model file |
|------------|
|backend_blockid/ml/models/blockid_model.joblib|
|backend_blockid/ml/models/token_scam_model.joblib|
|backend_blockid/ml/models/scaler.pkl|

## 4. Data Files

| CSV |
|-----|
| manual_wallets.csv | dataset manual label
| scam_wallets.csv | ground truth scam
| suspicious_tokens.csv | token scam dataset
| test_wallets_100.csv | testing dataset
| token_features.csv | token ML features
| transactions.csv | graph clustering input
| wallet_scores.csv | output scoring
| cluster_features.csv | graph clustering output
| flow_features.csv | flow analysis output
| drainer_features.csv | drainer detection output
| wallet_features.csv | merged feature dataset
| training_dataset.csv | ML training dataset


## 5. API Components

| Script |
|--------|
| __init__.py |
| app.py |
| db_wallet_tracking.py |
| middleware.py | logging / auth
| routes.py | endpoint definition
| server.py |
| trust_score.py |
| trust_score_sync.py | sync PDA → DB

## 5.1. Wallet Tracking Schema
Wallet tracking database implemented in:
backend_blockid/api_server/db_wallet_tracking.py

Tables:

TrackedWallet
- wallet (unique)
- label
- last_score
- last_risk
- last_checked
- is_active

ScoreHistory
- wallet
- score
- risk
- timestamp

Database Behavior:
If DATABASE_URL is set → PostgreSQL
If not → SQLite via WALLET_TRACKING_DB_PATH
Tables auto-created via init_db()

## 6. Environment Variables

Extracted from scanned Python files:

- `ANCHOR_IDL_PATH`
- `BLOCKID_DEBUG_LATENCY` | Unused env variables
- `CONFIRM_TIMEOUT_SEC`
- `DATABASE_PATH`
- `DATABASE_URL` | wallet tracking DB
- `DB_PATH`
- `HELIUS_API_KEY` | fetch blockchain data
- `MAX_TX_PER_MINUTE` | Unused env variables
- `ML_CONFIG_PATH`
- `ML_MODEL_PATH` | load trained model
- `ORACLE_PRIVATE_KEY` | oracle signer wallet
- `ORACLE_PROGRAM_ID` | PDA target
- `PERIODIC_INTERVAL_SEC`
- `PERIODIC_MAX_WALLETS` | Unused env variables
- `PUBLISH_INTERVAL_SECONDS`
- `SCORE`
- `SCORE_DELTA_THRESHOLD` | Unused env variables
- `SOLANA_CLUSTER`
- `SOLANA_COMMITMENT`
- `SOLANA_RPC_URL` | send transaction to devnet/mainnet
- `TRUST_SCORE_SYNC_INTERVAL_SEC`
- `WALLET`
- `WALLET_FEATURES_CSV`
- `WALLET_TRACKING_DB_PATH` | sqlite fallback
- `ANCHOR_WALLET_PATH`  | path keypair
- `HELIUS_RPC_URL` | RPC endpoint override
- `BLOCKID_MODEL_VERSION` | model version tracking
- `ANCHOR_PROVIDER_URL` | Anchor RPC override

If DATABASE_URL not set → fallback to SQLite via WALLET_TRACKING_DB_PATH



## 7. Deprecated / Experimental Components

- analytics/rugpull_detector.py → experimental
- analytics/nft_scam_detector.py → experimental
- test_helius_asset.py → debug only
- devnet_test.py → debug only


## 8. Core Pipeline

STEP 0 – Data Collection
fetch_tx_edges_helius.py
helius_extract_fields.py
→ transactions.csv

STEP 1 – Graph Clustering
graph_clustering.py
→ cluster_features.csv


STEP 2 – Flow Analysis
flow_features.py
→ flow_features.csv → outputs flow_features.csv

STEP 3 – Drainer Detection
drainer_detection.py
→ drainer_features.csv → outputs drainer_features.csv

STEP 4 – ML Training (Offline)
train_blockid_model.py
train_token_scam_model.py
→ blockid_model.joblib
→ token_scam_model.joblib

STEP 5 – Oracle Publishing
publish_scores.py
solana_publisher.py
→ TrustScoreAccount PDA
→ Wallet Tracking DB


## 9. Reason Codes

NEAR_SCAM_CLUSTER
HIGH_RAPID_TX
MULTI_VICTIM_PATTERN
NEW_CONTRACT_INTERACTION
HIGH_APPROVAL_RISK
SUDDEN_DRAIN_PATTERN

## 10. Identity Clustering:

- graph.py
- identity_cluster.py
- risk_propagation.py
Identity Model:
Wallet ≠ Identity
Identity = cluster(wallets)
Trust score applied per identity cluster, then propagated to wallets.
Cluster risk propagates to new wallets entering the cluster.



## 11 Data lineage 

transactions.csv
→ cluster_features.csv | graph clustering output
→ flow_features.csv | flow analysis output  
→ drainer_features.csv | drainer detection output
→ wallet_features.csv | merged feature dataset
→ training_dataset.csv | ML training dataset
→ blockid_model.joblib
→ wallet_scores.csv
→ PDA publish


 
 ## 12 Debug Entry Points

py backend_blockid.oracle.graph_clustering
py backend_blockid.oracle.flow_features
py backend_blockid.oracle.drainer_detection
py backend_blockid.ml.train_blockid_model
py backend_blockid.oracle.publish_scores


## 13 Deployment Info
Solana Program ID:
CxQ4mo9UQxVZMQ3T9oxvtYL7huBAGMjF8nTxL7XnkuTe

Oracle Signer Wallet:
9QCfNuQuxct1Xk9ytFYgxc5fThmTzL4pHQnSjjrVUrka

Cluster:
Devnet

Network:
Solana Devnet RPC Provider: Helius

Wallet Tracking DB:
PostgreSQL / SQLite fallback




## 14. Model Versioning:

blockid_model_YYYYMMDD_HHMM.joblib
token_scam_model_YYYYMMDD_HHMM.joblib

Metadata JSON:
- feature_count
- dataset_size
- accuracy
- precision
- recall

## 15. Replay Mode:

Use snapshot dataset
Run full pipeline without RPC
Compare trust score drift

## 16. Anti-Sybil Signals:

- wallet cluster size
- rapid wallet creation
- shared funding source
- identical behavior pattern

## 17. Security Model:

- Oracle signer isolated
- PDA publish signed tx
- Model files read-only
- Dataset snapshot hashed

## 18. Future Components:

- Real-time monitoring worker
- Cross-chain identity mapping
- OpenClaw monitoring agent
- Graph neural network model
- Public Trust API

## 19. Trust Score Formula (v1)

trust_score =
  success_rate * 0.5
+ cluster_risk * 0.3
+ drainer_signal * 0.2
trust_score normalized to 0–100


## 20. Limitations

Limitations:
- Cannot stop on-chain transactions
- Detects behavior patterns only
- Requires labeled scam dataset

## 21 Target Use Cases

Use Cases:
- Wallet risk API
- Agent reputation registry
- DeFi risk oracle
- NFT marketplace anti-scam filter

## 22. Real-Time Monitoring Design

Real-Time Monitoring Sources:
- Solana RPC subscription
- Helius webhook
- Batch scanner fallback

## 23. Model Evaluation Metrics

Evaluation Metrics:
- ROC-AUC
- Precision@TopK
- False Positive Rate
- Scam Recall Rate

## 24. Attack Vectors

Attack Vectors:
- Sybil wallet farming
- Wash transaction farming
- Fake reputation injection
- Model poisoning
- Oracle signer compromise

## 25. Production Checklist

Production Checklist:
- Model version saved
- Dataset snapshot hashed
- Oracle signer cold storage
- PDA publish retry logic
- RPC failover configured
- Monitoring alerts enabled

## 26. Model Retraining Policy

Retraining Policy:
- Retrain every 7 days OR when scam dataset +10%
- Compare ROC-AUC before deploy
- Manual review before PDA publish

## 27. False Positive Policy
False Positive Policy:
- Wallet flagged as High Risk must have ≥2 signals
- Manual override possible
- Reason codes stored with score

## 28. Data Privacy Statement
No personal data stored.
Only public blockchain data analyzed.

## 29. Failure Handling Policy
Failure Handling:

If publish PDA fails:
- retry 3x
- log error
- store pending publish queue

## 30. Monitoring Metrics
System Metrics:

- wallets scanned per hour
- PDA publish success rate
- RPC latency
- model prediction latency
- false positive reports

## 31. MVP Scope Definition
MVP Scope:

- Solana only
- Wallet trust score
- Batch analysis
- Manual scam dataset
- RandomForest model


Generated Timestamp

2026-02-19T03:52:08Z
