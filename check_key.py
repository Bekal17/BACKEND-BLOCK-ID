print("START SCRIPT")

import json
from solders.keypair import Keypair

path = r"C:\Users\SDA KEC MAKASAR\Desktop\id.json"
print("Path:", path)

with open(path) as f:
    secret = json.load(f)

print("Loaded json")

kp = Keypair.from_bytes(bytes(secret))
print("Oracle pubkey:", kp.pubkey())
