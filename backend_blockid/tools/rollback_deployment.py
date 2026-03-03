"""
BlockID Manual Rollback CLI.

Calls kubectl rollout undo to revert to previous deployment version.
Use when health checks fail, pipeline spikes, or API latency exceeds threshold.

Future upgrades:
  - Canary deployment
  - Blue/green deploy
  - Automatic DB migration rollback
  - Risk score rollback

Usage:
  py -m backend_blockid.tools.rollback_deployment
  py -m backend_blockid.tools.rollback_deployment --namespace blockid --deployment blockid-api
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

DEFAULT_NAMESPACE = os.getenv("BLOCKID_K8S_NAMESPACE", "blockid")
DEFAULT_DEPLOYMENT = os.getenv("BLOCKID_K8S_DEPLOYMENT", "blockid-api")


def main() -> int:
    ap = argparse.ArgumentParser(description="Rollback BlockID Kubernetes deployment")
    ap.add_argument("--namespace", "-n", default=DEFAULT_NAMESPACE, help="K8s namespace")
    ap.add_argument("--deployment", "-d", default=DEFAULT_DEPLOYMENT, help="Deployment name")
    ap.add_argument("--dry-run", action="store_true", help="Show command without running")
    args = ap.parse_args()

    cmd = [
        "kubectl", "rollout", "undo",
        f"deployment/{args.deployment}",
        "-n", args.namespace,
    ]
    logger.info("rollback_triggered", deployment=args.deployment, namespace=args.namespace)

    if args.dry_run:
        print(f"[rollback] dry-run: {' '.join(cmd)}")
        return 0

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            print(f"[rollback] FAILED: {result.stderr or result.stdout}")
            logger.warning("rollback_failed", stderr=result.stderr)
            return 1
        print(f"[rollback] SUCCESS: {args.deployment} rolled back")
        logger.info("rollback_triggered", version="previous", deployment=args.deployment)
        return 0
    except FileNotFoundError:
        print("[rollback] ERROR: kubectl not found")
        return 1
    except subprocess.TimeoutExpired:
        print("[rollback] ERROR: kubectl timeout")
        return 1


if __name__ == "__main__":
    sys.exit(main())
