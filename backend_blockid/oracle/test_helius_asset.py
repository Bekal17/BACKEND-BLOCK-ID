import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("HELIUS_API_KEY")

if not API_KEY:
    raise Exception("❌ HELIUS_API_KEY not found")

RPC_URL = f"https://rpc.helius.xyz/?api-key={API_KEY}"

TOKEN_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

payload = {
    "jsonrpc": "2.0",
    "id": "blockid-test",
    "method": "getAsset",
    "params": {
        "id": TOKEN_MINT
    }
}

print("🚀 Fetching asset from Helius RPC...")

res = requests.post(RPC_URL, json=payload)
data = res.json()

print("\n✅ RESULT:")
print(data)
