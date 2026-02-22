from backend_blockid.analytics.analytics_pipeline import run_wallet_analysis
from backend_blockid.ml.predictor import predict_wallet

wallets = [
    "9QCfNuQuxct1Xk9ytFYgxc5fThmTzL4pHQnSjjrVUrka",
    "7kbnvuGBxxj8AG9qp8Scn56muWGaRaFqxg1FsRp3PaFT",
]

for w in wallets:
    print("\n===== WALLET:", w, "=====")
    data = run_wallet_analysis(w)
    result = predict_wallet(data)
    print(result)
