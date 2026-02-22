import csv
from backend_blockid.oracle.publish_one_wallet import main as publish_one
from solders.pubkey import Pubkey

CSV_PATH = "backend_blockid/data/wallets.csv"


def run_batch():
    success = 0
    failed = 0

    with open(CSV_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            print("CSV reasons:", row.get("reason_codes"))
            wallet = row["wallet"]
            score = row.get("score", "50")
            risk = row.get("risk", "1")

            try:
                print(f"Publishing {wallet} score={score}")
                publish_one(wallet=wallet, score=int(score), risk=int(risk))
                success += 1
            except Exception as e:
                print("FAILED:", wallet, e)
                failed += 1

    print("SUCCESS:", success)
    print("FAILED:", failed)


if __name__ == "__main__":
    run_batch()


def validate_wallet(w):
    try:
        Pubkey.from_string(w)
        return True
    except Exception:
        return False
