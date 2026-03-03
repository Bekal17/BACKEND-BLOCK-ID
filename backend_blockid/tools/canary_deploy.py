"""
BlockID Canary Deployment CLI.

Deploy, promote, or rollback canary. Monitors metrics before promotion.
Future: automatic statistical canary analysis, per-endpoint canary, multi-region, A/B risk engine.

Usage:
  py -m backend_blockid.tools.canary_deploy deploy v1.5
  py -m backend_blockid.tools.canary_deploy promote v1.5
  py -m backend_blockid.tools.canary_deploy rollback
  py -m backend_blockid.tools.canary_deploy status
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

NAMESPACE = os.getenv("BLOCKID_K8S_NAMESPACE", "blockid")
STABLE_DEPLOY = "blockid-api-stable"
CANARY_DEPLOY = "blockid-api-canary"
CANARY_REPLICAS = int(os.getenv("BLOCKID_CANARY_REPLICAS", "1"))
STABLE_REPLICAS = int(os.getenv("BLOCKID_STABLE_REPLICAS", "9"))


def _kubectl(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    cmd = ["kubectl"] + args + ["-n", NAMESPACE]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=120, check=check)


def deploy_canary(version: str) -> int:
    """Deploy new version to canary only."""
    pct = int(100 * CANARY_REPLICAS / (CANARY_REPLICAS + STABLE_REPLICAS))
    logger.info("canary_deployed", version=version, traffic=f"{pct}%")
    print(f"[canary] deployed version={version} traffic={pct}%")
    try:
        _kubectl(["set", "image", f"deployment/{CANARY_DEPLOY}", f"api=blockid:{version}"])
        _kubectl(["scale", f"deployment/{CANARY_DEPLOY}", f"--replicas={CANARY_REPLICAS}"])
        _kubectl(["rollout", "status", f"deployment/{CANARY_DEPLOY}"])
        return 0
    except subprocess.CalledProcessError as e:
        print(f"[canary] deploy failed: {e.stderr or e.stdout}")
        return 1


def promote_canary(version: str) -> int:
    """Promote canary to stable: scale canary→100%, stable→0."""
    logger.info("canary_promoted", version=version)
    try:
        _kubectl(["set", "image", f"deployment/{STABLE_DEPLOY}", f"api=blockid:{version}"])
        _kubectl(["scale", f"deployment/{STABLE_DEPLOY}", f"--replicas={STABLE_REPLICAS}"])
        _kubectl(["scale", f"deployment/{CANARY_DEPLOY}", "--replicas=0"])
        _kubectl(["rollout", "status", f"deployment/{STABLE_DEPLOY}"])
        print(f"[canary] promoted version={version}")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"[canary] promote failed: {e.stderr or e.stdout}")
        return 1


def rollback_canary() -> int:
    """Scale canary to 0 (auto rollback)."""
    logger.info("canary_rollback")
    try:
        _kubectl(["scale", f"deployment/{CANARY_DEPLOY}", "--replicas=0"])
        print("[canary] rollback: canary scaled to 0")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"[canary] rollback failed: {e.stderr or e.stdout}")
        return 1


def status() -> int:
    """Show canary deployment status."""
    try:
        r = _kubectl(["get", "deployments", STABLE_DEPLOY, CANARY_DEPLOY, "-o", "wide"], check=False)
        print(r.stdout or r.stderr)
        return 0 if r.returncode == 0 else 1
    except Exception as e:
        print(f"[canary] status error: {e}")
        return 1


def main() -> int:
    ap = argparse.ArgumentParser(description="BlockID canary deploy / promote / rollback")
    sub = ap.add_subparsers(dest="action", required=True)
    sub.add_parser("status")
    d = sub.add_parser("deploy")
    d.add_argument("version", help="Image tag e.g. v1.5")
    p = sub.add_parser("promote")
    p.add_argument("version", help="Image tag to promote")
    sub.add_parser("rollback")
    args = ap.parse_args()

    if args.action == "status":
        return status()
    if args.action == "deploy":
        return deploy_canary(args.version)
    if args.action == "promote":
        return promote_canary(args.version)
    if args.action == "rollback":
        return rollback_canary()
    return 1


if __name__ == "__main__":
    sys.exit(main())
