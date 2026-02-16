# Wallet Tracking: SQLAlchemy + PostgreSQL Migration

## Overview

Wallet tracking uses **SQLAlchemy** with:

- **DATABASE_URL** set → PostgreSQL (e.g. `postgresql://user:pass@host:5432/dbname`)
- **DATABASE_URL** not set → SQLite (file from **WALLET_TRACKING_DB_PATH** or `wallet_tracking.db`)

Tables are created automatically on first use via `init_db()` (no Alembic required for a fresh install).

---

## Models

### TrackedWallet

| Column       | Type    | Notes                    |
|-------------|---------|---------------------------|
| id          | Integer | PK, autoincrement         |
| wallet      | String(64) | UNIQUE, NOT NULL      |
| label       | String(256) | nullable              |
| last_score  | Integer | nullable                  |
| last_risk   | String(32) | nullable               |
| last_checked| Integer | nullable (Unix timestamp) |
| is_active   | Boolean | NOT NULL, default True    |

### ScoreHistory

| Column   | Type      | Notes            |
|----------|-----------|------------------|
| id       | Integer   | PK, autoincrement |
| wallet   | String(64)| NOT NULL, indexed |
| score    | Integer   | NOT NULL         |
| risk     | String(32)| nullable         |
| timestamp| Integer   | NOT NULL (Unix), indexed |

---

## Migration Instructions

### Option A: Fresh PostgreSQL database

1. Create a database and user:

   ```bash
   createdb blockid_wallet_tracking
   # or in psql: CREATE DATABASE blockid_wallet_tracking;
   ```

2. Set the URL (no password in logs; only path is logged):

   ```bash
   export DATABASE_URL="postgresql://user:password@localhost:5432/blockid_wallet_tracking"
   ```

3. Start the API or run batch once; tables are created automatically:

   ```bash
   uvicorn backend_blockid.api_server.app:app --reload
   # or: python -c "from backend_blockid.api_server.db_wallet_tracking import init_db; init_db()"
   ```

### Option B: Migrate from existing SQLite

1. **Export data from SQLite** (optional script):

   ```python
   # run with SQLite env unset or WALLET_TRACKING_DB_PATH=wallet_tracking.db
   from backend_blockid.api_server.db_wallet_tracking import _get_engine, TrackedWallet, ScoreHistory
   from sqlalchemy.orm import Session
   engine = _get_engine()
   with Session(engine) as s:
       wallets = s.query(TrackedWallet).all()
       history = s.query(ScoreHistory).all()
       # write to JSON/CSV or insert into Postgres in second step
   ```

2. **Point to PostgreSQL:**

   ```bash
   export DATABASE_URL="postgresql://user:password@host:5432/dbname"
   unset WALLET_TRACKING_DB_PATH  # so we don't fall back to SQLite
   ```

3. **Create tables and re-import:**

   ```bash
   python -c "from backend_blockid.api_server.db_wallet_tracking import init_db; init_db()"
   # Then run your import script or re-add wallets via POST /track_wallet and POST /import_wallets_csv
   ```

### Option C: Use Alembic (versioned migrations)

1. Install Alembic:

   ```bash
   pip install alembic
   ```

2. From project root:

   ```bash
   alembic init alembic
   ```

3. Set `sqlalchemy.url` in `alembic.ini` or in `alembic/env.py` from `DATABASE_URL`:

   ```python
   # alembic/env.py
   import os
   from backend_blockid.api_server.db_wallet_tracking import Base
   config.set_main_option("sqlalchemy.url", os.getenv("DATABASE_URL", "sqlite:///wallet_tracking.db"))
   target_metadata = Base.metadata
   ```

4. Generate first migration:

   ```bash
   alembic revision --autogenerate -m "wallet_tracking_tables"
   alembic upgrade head
   ```

---

## Env summary

| Variable                  | Use                          |
|---------------------------|------------------------------|
| **DATABASE_URL**          | Postgres URL; if set, used.  |
| **WALLET_TRACKING_DB_PATH** | SQLite path when DATABASE_URL not set (default: `wallet_tracking.db`). |

---

## Compatibility

- FastAPI endpoints **POST /track_wallet**, **GET /tracked_wallets**, **POST /import_wallets_csv** are unchanged.
- **batch_publish.py** uses the same `init_db()`, `load_active_wallets_with_scores()`, `update_wallet_score()`; no code changes needed.
