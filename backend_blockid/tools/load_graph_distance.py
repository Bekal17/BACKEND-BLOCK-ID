"""
Load graph distance (hop distance from scam wallet) from cluster/feature CSVs.
"""
import csv


def load_graph_distance_map(path: str = "backend_blockid/data/cluster_features.csv") -> dict[str, int]:
    """
    Load wallet -> graph distance from CSV.
    Supports scam_distance or distance_to_scam column.
    """
    result: dict[str, int] = {}

    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                wallet = (row.get("wallet") or "").strip()
                if not wallet:
                    continue
                raw = row.get("scam_distance") or row.get("distance_to_scam", 999)
                try:
                    distance = int(float(raw or 999))
                    result[wallet] = distance if distance >= 0 else 999
                except (TypeError, ValueError):
                    result[wallet] = 999
    except FileNotFoundError:
        pass

    return result
