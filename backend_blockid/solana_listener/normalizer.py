"""
Transaction normalizer — raw Solana data to internal models.

Responsibilities:
- Convert Solana RPC transaction formats into domain DTOs/models.
- Extract wallet addresses, token transfers, program interactions, and metadata.
- Provide a stable schema for downstream analysis regardless of RPC response shape.
"""
