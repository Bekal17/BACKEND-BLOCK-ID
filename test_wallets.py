"""
BlockID wallet test script: run analytics + ML features for a list of wallets.
Invalid wallets (missing tx_count / wallet_age_days) are skipped and counted.
"""

print("START TEST SCRIPT")

from backend_blockid.analytics.analytics_pipeline import run_wallet_analysis
from backend_blockid.ml.feature_builder import build_features

wallets = [
    "9QCfNuQuxct1Xk9ytFYgxc5fThmTzL4pHQnSjjrVUrka",
    "7kbnvuGBxxj8AG9qp8Scn56muWGaRaFqxg1FsRp3PaFT",
    "HbtSzZgPZJabqWgXV9t7S4n6RwkmFuUDze1tQYX22oAP",
    "75tZxLzH9CEH64CtyMdmo9ANvC2jfUW3Nict9e6xeNMx",
    "3hT4PHcUT6jFToX1ge8aEGDz5bNGTaz829VtNif6aLeP",
]

valid_count = 0
invalid_count = 0

for w in wallets:
    print("\n======", w, "======")
    try:
        data = run_wallet_analysis(w)
        feats = build_features(data)
        print("metrics:", data["metrics"])
        print("features:", feats)
        valid_count += 1
    except ValueError as e:
        print("SKIPPED INVALID WALLET:", w, e)
        invalid_count += 1

print("\n" + "=" * 50)
print("SUMMARY: valid", valid_count, "| invalid (skipped)", invalid_count, "| total", len(wallets))
