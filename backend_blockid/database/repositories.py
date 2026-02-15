"""
Repository layer — CRUD for transactions and trust scores.

Responsibilities:
- Persist normalized transactions and trust score results.
- Query wallets by address, time range, or score threshold.
- Abstract storage backend (PostgreSQL, etc.) behind a consistent interface.
"""
