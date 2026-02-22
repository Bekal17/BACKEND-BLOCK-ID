import requests

API = "http://localhost:8000/insert_scam_wallet"

wallets = [
    "ScamWallet11111111111111111111111111111111",
    "ScamWallet22222222222222222222222222222222",
]

for w in wallets:
    r = requests.post(API, json={"wallet": w})
    print(w, r.status_code, r.text)
    