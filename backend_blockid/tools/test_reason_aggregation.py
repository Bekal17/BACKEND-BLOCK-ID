from backend_blockid.ai_engine.reason_weight_engine import aggregate_score


def main() -> None:
    base = 80
    reasons = [
        {"code": "RUG_PULL", "weight": -40},
        {"code": "NO_SCAM_HISTORY", "weight": 10},
    ]
    final_score = aggregate_score(base, reasons)
    print(f"[test_reason_aggregation] base={base} final={final_score}")


if __name__ == "__main__":
    main()
