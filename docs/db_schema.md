# BlockID Database Schema

## Overview

BlockID uses two database systems:

1. **Main backend** (`backend_blockid/database/database.py`): SQLite (MVP) or PostgreSQL. Stores wallet profiles, transactions, trust scores, alerts, graph edges, clusters, and entity reputation. Used by the agent worker, scheduler, analytics, and API trust score reads.

2. **Wallet tracking** (`backend_blockid/api_server/db_wallet_tracking.py`): SQLAlchemy models. Stores tracked wallets for Step 2 publish pipeline and score history. Separate DB file or `DATABASE_URL` when using PostgreSQL.

---

## Tables

### Table: wallet_profiles
Description: Wallet profile cache; first/last seen and profile JSON.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| wallet | TEXT | No | — | Primary key; Solana address |
| first_seen_at | INTEGER | No | — | Unix timestamp |
| last_seen_at | INTEGER | No | — | Unix timestamp |
| profile_json | TEXT | Yes | — | JSON snapshot of analytics |
| created_at | INTEGER | Yes | — | Unix timestamp |
| updated_at | INTEGER | Yes | — | Unix timestamp |

**Indexes:** `ix_wallet_profiles_last_seen` on `last_seen_at`.

---

### Table: transactions
Description: Transaction records per wallet for graph and flow analysis.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| id | INTEGER | No | AUTOINCREMENT | Primary key |
| wallet | TEXT | No | — | Owning wallet |
| signature | TEXT | No | — | Solana tx signature |
| sender | TEXT | No | — | Sender address |
| receiver | TEXT | No | — | Receiver address |
| amount_lamports | INTEGER | No | — | Amount in lamports |
| timestamp | INTEGER | Yes | — | Unix timestamp |
| slot | INTEGER | Yes | — | Solana slot |
| created_at | INTEGER | Yes | — | Unix timestamp |

**Constraints:** UNIQUE(wallet, signature).

**Indexes:** `ix_transactions_wallet`, `ix_transactions_signature`, `ix_transactions_timestamp`, `ix_transactions_wallet_timestamp`.

---

### Table: trust_scores
Description: Trust score timeline per wallet; main source for API reads.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| id | INTEGER | No | AUTOINCREMENT | Primary key |
| wallet | TEXT | No | — | Solana address |
| score | REAL | No | — | Trust score 0–100 |
| computed_at | INTEGER | No | — | Unix timestamp |
| metadata_json | TEXT | Yes | — | JSON (risk, flags, etc.) |
| created_at | INTEGER | Yes | — | Unix timestamp |

**Indexes:** `ix_trust_scores_wallet`, `ix_trust_scores_wallet_computed`.

---

### Table: tracked_wallets (main backend)
Description: Agent/scheduler wallet list; priority and last analyzed.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| wallet | TEXT | No | — | Primary key; Solana address |
| created_at | INTEGER | No | — | Unix timestamp |
| priority | TEXT | No | 'normal' | Scheduler priority |
| last_analyzed_at | INTEGER | Yes | — | Unix timestamp |

**Indexes:** `ix_tracked_wallets_created_at`, `ix_tracked_wallets_priority`.

---

### Table: tracked_wallets (wallet tracking / Step 2)
Description: SQLAlchemy model; publish pipeline wallet list with last score and reason codes.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| id | INTEGER | No | AUTOINCREMENT | Primary key |
| wallet | String(64) | No | — | UNIQUE; Solana address |
| label | String(256) | Yes | — | Optional label |
| last_score | Integer | Yes | — | Last published score |
| last_risk | String(32) | Yes | — | Risk level string |
| last_checked | Integer | Yes | — | Unix timestamp |
| is_active | Boolean | No | True | Active flag for publish |
| reason_codes | String(1024) | Yes | — | JSON array of reason codes |

**Note:** Stored in a separate DB (wallet_tracking.db or DATABASE_URL).

---

### Table: score_history
Description: Append-only score history per wallet; one row per publish.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| id | INTEGER | No | AUTOINCREMENT | Primary key |
| wallet | String(64) | No | — | Solana address |
| score | Integer | No | — | Published score |
| risk | String(32) | Yes | — | Risk level |
| timestamp | Integer | No | — | Unix timestamp |

**Indexes:** `wallet`, `timestamp`.

---

### Table: alerts
Description: Wallet alerts by severity and reason.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| id | INTEGER | No | AUTOINCREMENT | Primary key |
| wallet | TEXT | No | — | Solana address |
| severity | TEXT | No | — | Severity level |
| reason | TEXT | No | — | Alert reason |
| created_at | INTEGER | No | — | Unix timestamp |

**Indexes:** `ix_alerts_wallet`, `ix_alerts_wallet_severity_reason_created`, `ix_alerts_created_at`.

---

### Table: wallet_rolling_stats
Description: Rolling window stats (volume, tx count, anomalies) per wallet.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| id | INTEGER | No | AUTOINCREMENT | Primary key |
| wallet | TEXT | No | — | Solana address |
| period_end_ts | INTEGER | No | — | Period end Unix timestamp |
| window_days | INTEGER | No | — | Window size in days |
| volume_lamports | INTEGER | No | — | Volume in lamports |
| tx_count | INTEGER | No | — | Transaction count |
| anomaly_count | INTEGER | No | — | Anomaly count |
| avg_trust_score | REAL | Yes | — | Average trust score |
| alert_count | INTEGER | No | — | Alert count |
| created_at | INTEGER | Yes | — | Unix timestamp |

**Indexes:** `ix_wallet_rolling_stats_wallet_window`, `ix_wallet_rolling_stats_period`.

---

### Table: wallet_escalation_state
Description: Escalation state per wallet (risk stage, scores).

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| wallet | TEXT | No | — | Primary key |
| risk_stage | TEXT | No | — | Current risk stage |
| escalation_score | REAL | No | — | Escalation score |
| last_alert_ts | INTEGER | Yes | — | Last alert Unix timestamp |
| last_clean_ts | INTEGER | Yes | — | Last clean Unix timestamp |
| state_json | TEXT | Yes | — | JSON state |
| updated_at | INTEGER | No | — | Unix timestamp |

**Indexes:** `ix_wallet_escalation_risk_stage`.

---

### Table: wallet_priority
Description: Priority tier per wallet for scheduler.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| wallet | TEXT | No | — | Primary key |
| tier | TEXT | No | — | Priority tier |
| updated_at | INTEGER | No | — | Unix timestamp |

**Indexes:** `ix_wallet_priority_tier`.

---

### Table: wallet_reputation_state
Description: Reputation state (score, trend, volatility) per wallet.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| wallet | TEXT | No | — | Primary key |
| current_score | REAL | No | — | Current reputation score |
| avg_7d | REAL | Yes | — | 7-day average |
| avg_30d | REAL | Yes | — | 30-day average |
| trend | TEXT | No | — | Trend direction |
| volatility | REAL | Yes | — | Volatility measure |
| decay_factor | REAL | No | 1.0 | Decay factor |
| updated_at | INTEGER | No | — | Unix timestamp |

**Indexes:** `ix_wallet_reputation_trend`.

---

### Table: wallet_graph_edges
Description: Directed edges between wallets (sender → receiver) for graph analysis.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| sender_wallet | TEXT | No | — | PK; sender address |
| receiver_wallet | TEXT | No | — | PK; receiver address |
| tx_count | INTEGER | No | 0 | Transaction count |
| total_volume | INTEGER | No | 0 | Total volume lamports |
| last_seen_timestamp | INTEGER | No | — | Last seen Unix timestamp |

**Primary key:** (sender_wallet, receiver_wallet).

**Indexes:** `ix_wallet_graph_sender`, `ix_wallet_graph_receiver`.

---

### Table: wallet_clusters
Description: Wallet clusters with confidence and risk.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| cluster_id | INTEGER | No | AUTOINCREMENT | Primary key |
| confidence_score | REAL | No | 0.0 | Confidence score |
| reason_tags | TEXT | Yes | — | JSON reason tags |
| cluster_risk | REAL | Yes | — | Cluster risk |
| risk_updated_at | INTEGER | Yes | — | Unix timestamp |
| updated_at | INTEGER | No | — | Unix timestamp |

**Indexes:** `ix_wallet_clusters_confidence`.

---

### Table: wallet_cluster_members
Description: Wallet membership in clusters.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| cluster_id | INTEGER | No | — | PK; FK → wallet_clusters |
| wallet | TEXT | No | — | PK; Solana address |
| added_at | INTEGER | No | — | Unix timestamp |

**Primary key:** (cluster_id, wallet).

**Foreign key:** cluster_id → wallet_clusters(cluster_id).

**Indexes:** `ix_wallet_cluster_members_wallet`.

---

### Table: entity_profiles
Description: Entity (cluster) reputation profiles.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| entity_id | INTEGER | No | — | Primary key |
| cluster_id | INTEGER | No | — | FK → wallet_clusters |
| reputation_score | REAL | No | 50.0 | Reputation score |
| risk_history | TEXT | Yes | — | Risk history JSON |
| last_updated | INTEGER | No | — | Unix timestamp |
| decay_factor | REAL | No | 1.0 | Decay factor |
| reason_tags | TEXT | Yes | — | JSON reason tags |

**Foreign key:** cluster_id → wallet_clusters(cluster_id).

**Indexes:** `ix_entity_profiles_cluster`.

---

### Table: entity_reputation_history
Description: Historical reputation snapshots per entity.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| id | INTEGER | No | AUTOINCREMENT | Primary key |
| entity_id | INTEGER | No | — | FK → entity_profiles |
| reputation_score | REAL | No | — | Score at snapshot |
| reason_tags | TEXT | Yes | — | JSON reason tags |
| snapshot_at | INTEGER | No | — | Unix timestamp |

**Foreign key:** entity_id → entity_profiles(entity_id).

**Indexes:** `ix_entity_reputation_history_entity`, `ix_entity_reputation_history_snapshot`.

---

## Relationships

- **wallet_cluster_members.cluster_id** → **wallet_clusters.cluster_id**  
  Links wallets to their clusters.

- **entity_profiles.cluster_id** → **wallet_clusters.cluster_id**  
  Links entity profiles to clusters.

- **entity_reputation_history.entity_id** → **entity_profiles.entity_id**  
  Links reputation history to entities.

- **wallet_graph_edges**: References wallets by address; no formal FK to wallet_profiles or tracked_wallets.

- **trust_scores, alerts, wallet_rolling_stats, wallet_escalation_state, wallet_priority, wallet_reputation_state**: All reference wallets by address; no formal FKs.

- **score_history.wallet** (wallet tracking): Logical link to **tracked_wallets.wallet** in the same DB; no explicit FK.

---

## Database Modes

### Main backend (database.py)

- **SQLite** (default): Single file at `DB_PATH` (default `blockid.db`). INTEGER for timestamps.
- **PostgreSQL** (planned): Set `DATABASE_URL`. Use SERIAL/BIGSERIAL, TIMESTAMPTZ; placeholders change from `?` to `%s`.

### Wallet tracking (db_wallet_tracking.py)

- **PostgreSQL**: Set `DATABASE_URL`. Used for wallet tracking tables when set.
- **SQLite fallback**: When `DATABASE_URL` is unset, uses `WALLET_TRACKING_DB_PATH` or `DATABASE_PATH` or `blockid.db`. Default file: `wallet_tracking.db` when `WALLET_TRACKING_DB_PATH` is set, else `blockid.db`.

**Env vars:**

- `DATABASE_URL` – PostgreSQL connection string (used by wallet tracking when set)
- `DB_PATH` – SQLite path for main backend (default `blockid.db`)
- `DATABASE_PATH` – Fallback path for wallet tracking SQLite
- `WALLET_TRACKING_DB_PATH` – SQLite path for wallet tracking

---

## Migration Notes

- **init_db()** (wallet tracking): `Base.metadata.create_all()` creates `tracked_wallets` and `score_history` if missing. Safe to run on startup.
- **Reason codes migration**: `_migrate_reason_codes()` adds `reason_codes` to `tracked_wallets` (wallet tracking) if absent. Runs inside `init_db()`.
- **Main backend**: Tables are created via `ensure_schema()` using raw `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`. No Alembic yet.
- **Future**: Alembic can be introduced for versioned migrations on both databases. Current approach uses in-code migrations (e.g. `ALTER TABLE ... ADD COLUMN`) where needed.
