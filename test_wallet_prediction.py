import joblib
from pathlib import Path


MODEL = Path("models/wallet_classifier.pkl")

if not MODEL.is_file():
    raise FileNotFoundError(f"Model not found: {MODEL}")

model = joblib.load(MODEL)


def wallet_features(w: str):
    return [
        len(w),
        ord(w[0]),
        ord(w[-1]),
    ]


wallet = "9QCfNuQuxct1Xk9ytFYgxc5fThmTzL4pHQnSjjrVUrka"

pred = model.predict([wallet_features(wallet)])

print("Wallet:", wallet)
print("Prediction:", "SCAM" if pred[0] == 1 else "GOOD")

