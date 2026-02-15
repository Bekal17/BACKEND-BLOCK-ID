"""
Database abstraction layer — wallet profiles, transaction history, trust score timeline.

MVP uses SQLite via Database and get_database(); backend is swappable for PostgreSQL.
"""

from backend_blockid.database.database import (
    Database,
    DatabaseBackend,
    SQLiteBackend,
    get_database,
)
from backend_blockid.database.models import (
    TransactionRecord,
    TrustScoreRecord,
    WalletProfile,
)

__all__ = [
    "Database",
    "DatabaseBackend",
    "SQLiteBackend",
    "get_database",
    "TransactionRecord",
    "TrustScoreRecord",
    "WalletProfile",
]
