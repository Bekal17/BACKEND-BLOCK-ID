# BlockID — Web3 Wallet Trust Infrastructure

BlockID is a Solana-based trust scoring oracle that detects scam wallets
and publishes transparent trust scores on-chain.

---

## Pipeline Overview

Transactions / Helius RPC
        ↓
Graph Clustering
        ↓
Flow Analysis
        ↓
Drainer Detection
        ↓
Evidence Collector
        ↓
Reason Aggregator
        ↓
Reason Weight Engine
        ↓
ML Wallet Scoring
        ↓
Batch Oracle Publish (Solana PDA)

---

## Transparency

Every trust score includes reason codes and transaction proof:

Example:

- SCAM_CLUSTER_MEMBER
- RUG_PULL_SIMULATION → Solscan link

Users can verify every score.

---

## ⚙ Tech Stack

- Python 3.13
- FastAPI backend
- Anchor / Solana Oracle
- Helius RPC
- RandomForest ML
- SQLite (dev) → PostgreSQL (future)

---

## Current Status

✔ Full pipeline working  
✔ Data integrity verified  
✔ Tx proof clickable  
✔ Positive + negative reasons  
✔ Batch publishing to Solana  

---

## Next Steps

- UI Trust Proof tab
- Multi-chain identity mapping
- Mainnet deployment
- API monetization