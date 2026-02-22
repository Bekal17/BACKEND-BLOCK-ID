#!/usr/bin/env bash
# Deploy BlockID Anchor program to Solana MAINNET via Helius RPC (run in WSL).
# Usage:
#   export HELIUS_API_KEY=your_key
#   ./scripts/deploy_mainnet_wsl.sh
# Or from repo root: bash scripts/deploy_mainnet_wsl.sh

set -e

PROGRAM_ID_MAINNET="55iMY3uHQadPv4PXwqF1uYWdyie3wqKCwJHs97eWPE6B"
ROOT="${1:-/mnt/d/BACKENDBLOCKID}"

cd "$ROOT"
echo "[1/8] Project root: $(pwd)"

echo "[2/8] Cleaning old builds..."
anchor clean

echo "[3/8] Building Anchor program..."
anchor build

echo "[4/8] Verifying program file..."
ls -lh target/deploy
realpath target/deploy/blockid_oracle.so

if [ -z "${HELIUS_API_KEY:-}" ]; then
  echo "ERROR: Set HELIUS_API_KEY before configuring RPC and deploying."
  echo "  export HELIUS_API_KEY=your_helius_api_key"
  exit 1
fi

echo "[5/8] Setting Solana config to Helius MAINNET RPC..."
solana config set --url "https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}"

echo "[6/8] Wallet check..."
solana address
solana balance

echo "[7/8] Deploying program to MAINNET..."
anchor deploy --provider.cluster mainnet

echo "[8/8] Program ID and explorer verification..."
solana program show "$PROGRAM_ID_MAINNET"
echo "Explorer: https://explorer.solana.com/address/${PROGRAM_ID_MAINNET}?cluster=mainnet"
