import joblib
from pathlib import Path

import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

from backend_blockid.ml.feature_extractor import extract_features

# ===== FIND PROJECT ROOT =====
ROOT = Path(__file__).resolve()
for _ in range(5):
    if (ROOT / "wallet_labels_master.csv").exists():
        break
    ROOT = ROOT.parent

DATA_FILE = ROOT / "wallet_labels_master.csv"
MODEL_DIR = ROOT / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

print("Project root:", ROOT)
print("Dataset:", DATA_FILE)

df = pd.read_csv(DATA_FILE)

if "wallet" not in df.columns or "label" not in df.columns:
    raise Exception("wallet_labels_master.csv must contain 'wallet' and 'label' columns")

df["label_bin"] = df["label"].apply(lambda x: 0 if str(x).strip() == "good" else 1)

features = []
labels = []

for _, row in df.iterrows():
    wallet = str(row["wallet"])
    try:
        f = extract_features(wallet)
        # Only keep numeric features; ignore non-numeric keys like 'extracted_at'
        numeric = [
            float(f.get("total_tx", 0)),
            float(f.get("account_age_days", 0)),
            float(f.get("unique_counterparties", 0)),
        ]
        features.append(numeric)
        labels.append(int(row["label_bin"]))
    except Exception as e:  # noqa: BLE001
        print("feature error:", wallet, e)

X = features
y = labels

if len(X) < 2:
    raise Exception("Not enough valid feature rows to train (got < 2)")

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=42
)

model = RandomForestClassifier()
model.fit(X_train, y_train)

pred = model.predict(X_test)
acc = accuracy_score(y_test, pred)

print("Accuracy:", acc)

model_path = MODEL_DIR / "wallet_classifier.pkl"
joblib.dump(model, model_path)

print("Model saved to:", model_path.resolve())


