# BlockID Architecture Gaps

**Identified from project structure and codebase analysis.**

---

## 1. Missing Components

| Component | Status | Notes |
|-----------|--------|-------|
| **routes.py** | Stub only | Docstring describes endpoint responsibilities; no route definitions. All routes live in `server.py`. |
| **middleware.py** | Stub only | Docstring lists logging, auth, rate limiting; no implementation. |
| **config.get_settings()** | Not implemented | `config/settings.py` raises `NotImplementedError`. No central config layer. |
| **Real-time ingestion** | Not wired | `ingestion/solana_stream.py` exists but is never started. Agent uses periodic runner, not WebSocket stream. |
| **Alembic migrations** | Missing | Schema changes are in-code (`ALTER TABLE` in migrations/, `_migrate_reason_codes`). No versioned migrations. |
| **API authentication** | Missing | No API key or JWT. Middleware stub documents future auth. |
| **Rate limiting** | Missing | No throttling. Middleware documents future rate limit. |

---

## 2. Unused Modules

| Module | Referenced by | Notes |
|--------|---------------|-------|
| **ai_engine/oracle/solana_publisher.py** | Only `ai_engine.oracle.__init__` | No production code imports it. `oracle/solana_publisher.py` is used instead. Duplicate publisher. |
| **ingestion/solana_stream.py** | Only `ingestion.__init__` | Exported but never invoked. Real-time WebSocket ingestion not integrated. |
| **behavioral_memory/** | `agent_worker` references in comments | Not clearly wired into main pipeline. |
| **ml/predict.py, ml/predictor.py** | `test_predictor`, `ml/__init__`, `build_dataset` | Overlap with `predict_wallet_score`, `publish_scores`. Multiple prediction entry points. |
| **db_wallet_tracking.db_wallet_tracking** | Used by API, batch_publish | N/A — used. But `main.py` uses main DB `get_tracked_wallet_addresses`, not this. |

---

## 3. Duplicate Logic

| Area | Duplication | Location |
|------|-------------|----------|
| **Solana publisher** | Two implementations | `oracle/solana_publisher.py` (IDL-based, used) vs `ai_engine/oracle/solana_publisher.py` (hardcoded discriminator, unused). Same purpose: build `update_trust_score` instruction. |
| **tracked_wallets** | Two tables, same name | Main DB: `tracked_wallets` (wallet, created_at, priority, last_analyzed_at). Step 2: `tracked_wallets` (id, wallet, label, last_score, last_risk, reason_codes). Different schemas; potential collision if both use `blockid.db`. |
| **Trust score endpoints** | Overlapping responses | `GET /wallet/{address}` vs `GET /api/trust-score/{wallet}`. Different shapes; similar data. |
| **Track wallet endpoints** | Two endpoints | `POST /track-wallet` (main scheduler) vs `POST /track_wallet` (Step 2). Both add wallets; different backends. |
| **_raw_bytes_from_account_data** | Copy-pasted | `trust_score.py`, `trust_score_sync.py`, `read_trust_score_auto.py`, `batch_read_debug.py` — same byte-normalization logic. |
| **cluster_features vs graph_cluster_features** | Naming mismatch | `graph_clustering.py` writes `graph_cluster_features.csv`. `train_blockid_model.py` and `publish_scores.py` expect `cluster_features.csv`. Requires copy or symlink. |
| **_score_to_risk_level** | Duplicated | `oracle/solana_publisher.py`, `ai_engine/oracle/solana_publisher.py`. Same mapping (score → 0–3). |

---

## 4. Unclear Dependencies

| Dependency | Issue |
|------------|-------|
| **main.py wallet source** | Docstring says "register via POST /track-wallet", but `main.py` uses `db.get_tracked_wallet_addresses()` (main DB). `POST /track-wallet` writes to `db_wallet_tracking`. These are different tables. Unclear which source main.py should use. |
| **batch_publish vs main** | `batch_publish` uses `db_wallet_tracking.load_active_wallets()`. `main.py` uses main DB. No shared wallet registry. |
| **Oracle vs ai_engine** | `oracle/solana_publisher` used by publish scripts; `ai_engine/oracle/solana_publisher` used by `TrustOraclePublisher` (DB → publish loop). `TrustOraclePublisher` reads from main DB `get_tracked_wallets` — different from `db_wallet_tracking`. Unclear when each is used. |
| **DB_PATH vs WALLET_TRACKING_DB_PATH** | When both unset, `db_wallet_tracking` defaults to `blockid.db` (same as main DB). Two `tracked_wallets` schemas could conflict in one file. |
| **train_model vs train_blockid_model** | `train_model` uses analytics/feature_builder; `train_blockid_model` uses CSV merge. Different pipelines; unclear which is canonical for PDA publishing. |
| **publish_scores vs publish_wallet_scores vs batch_publish** | Three publish paths: ML (`publish_scores`), CSV (`publish_wallet_scores`), analytics (`batch_publish`). Overlapping responsibilities. |

---

## 5. Recommendations

1. **Consolidate solana_publisher** — Use one module (`oracle/solana_publisher.py`). Deprecate or remove `ai_engine/oracle/solana_publisher.py`.
2. **Clarify tracked_wallets** — Rename or document: main DB = agent scheduler; Step 2 = publish pipeline. Ensure `main.py` and API docstrings match actual DB usage.
3. **Fix cluster_features path** — Align `graph_clustering.py` output name with `train_blockid_model` input, or add symlink in docs.
4. **Extract _raw_bytes_from_account_data** — Move to shared util (e.g. `oracle/account_utils.py`).
5. **Implement or remove stubs** — Either implement `routes.py`, `middleware.py`, `config.get_settings()`, or remove and document that routes live in `server.py`.
6. **Wire or archive ingestion** — Integrate `solana_stream` into agent, or mark as future/experimental.
7. **Unify publish pipelines** — Document when to use `publish_scores` vs `publish_wallet_scores` vs `batch_publish`; consider a single entry point with mode flag.
