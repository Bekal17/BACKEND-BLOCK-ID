# BlockID Devnet Mode

BlockID runs in **devnet mode** by default. This document describes configuration and usage.

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SOLANA_NETWORK` | `devnet` | `devnet` or `mainnet` |
| `SOLANA_RPC_URL` | (see below) | RPC endpoint; read from .env |
| `HELIUS_API_KEY` | — | Optional; uses devnet/mainnet Helius URL when set |
| `ORACLE_PROGRAM_ID` | (from Anchor.toml) | Devnet: `55iMY3uHQadPv4PXwqF1uYWdyie3wqKCwJHs97eWPE6B` |
| `BLOCKID_USE_DUMMY_DATA` | `0` | Set `1` to use devnet dummy dataset when RPC unavailable |

### RPC URL Resolution

1. `SOLANA_RPC_URL` (if set)
2. `HELIUS_API_KEY` → `https://devnet.helius-rpc.com/?api-key=...` (devnet) or mainnet
3. `https://api.devnet.solana.com` (devnet) or `https://api.mainnet-beta.solana.com` (mainnet)

### PDA Derivation

Trust score PDAs use seeds: `[b"trust_score", oracle_pubkey, wallet_pubkey]` and the `ORACLE_PROGRAM_ID` (or default for current network). Ensure the program ID matches your deployed Anchor program.

## Devnet Dummy Dataset

When `BLOCKID_USE_DUMMY_DATA=1` or Helius/RPC is unavailable, the pipeline uses:

- `backend_blockid/data/devnet_dummy/transactions.csv`
- `backend_blockid/data/devnet_dummy/flow_features.csv`
- `backend_blockid/data/devnet_dummy/drainer_features.csv`
- `backend_blockid/data/devnet_dummy/wallets.csv`

This allows running STEP 0–5 without a live RPC connection.

## Anchor.toml

`provider.cluster` is set to `devnet`. Program IDs:

- **Devnet:** `55iMY3uHQadPv4PXwqF1uYWdyie3wqKCwJHs97eWPE6B`
- **Mainnet:** `9etRwVdKdVkvRsMbYVroGPzcdDxZnRmDH1D8Ho6waXGA`

## Script Startup

Pipeline scripts print network and program ID at start:

```
[blockid] graph_clustering | network=devnet | program_id=55iMY3uHQadPv4PXwqF1uYWdyie3wqKCwJHs97eWPE6B | rpc=https://api.devnet.solana.com
```
