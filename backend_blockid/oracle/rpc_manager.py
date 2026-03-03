"""
RPC manager for BlockID Multi-RPC Failover.

Chooses fastest healthy RPC, rotates on failure, caches best for 5 minutes.
Exposes get_client() and rpc_post() for oracle modules.

Future upgrades: geo-based selection, weighted load balancing, per-task RPC, cost optimizer.
"""

from __future__ import annotations

import os
import time
from typing import Any

from backend_blockid.blockid_logging import get_logger
from backend_blockid.config.env import load_blockid_env
from backend_blockid.config.rpc_endpoints import get_rpc_endpoints
from backend_blockid.oracle.rpc_health import check_rpc_health

logger = get_logger(__name__)

load_blockid_env()

RPC_TIMEOUT = float(os.getenv("RPC_TIMEOUT", "5") or 5)
RPC_MAX_RETRIES = int(os.getenv("RPC_MAX_RETRIES", "3") or 3)
RPC_CACHE_SECONDS = int(os.getenv("RPC_CACHE_SECONDS", "300"))


def _safe_host(url: str) -> str:
    """Extract host for logging (mask API keys)."""
    if "api-key=" in url:
        return url.split("?")[0] + "..."
    if len(url) > 50:
        return url[:50] + "..."
    return url


# Prometheus metrics (optional - avoid import error if prometheus_client missing)
_rpc_latency: Any = None
_rpc_failures: Any = None
_rpc_switch_count: Any = None


def _init_metrics() -> None:
    global _rpc_latency, _rpc_failures, _rpc_switch_count
    if _rpc_latency is not None:
        return
    try:
        from prometheus_client import Histogram, Counter, REGISTRY
        _rpc_latency = Histogram(
            "blockid_rpc_latency_seconds",
            "RPC request latency in seconds",
            ["rpc_host"],
            registry=REGISTRY,
        )
        _rpc_failures = Counter(
            "blockid_rpc_failures_total",
            "RPC request failures",
            ["rpc_host"],
            registry=REGISTRY,
        )
        _rpc_switch_count = Counter(
            "blockid_rpc_switch_total",
            "RPC failover switch count",
            registry=REGISTRY,
        )
    except Exception:
        pass


class RPCManager:
    """Manages RPC failover: health check, selection, caching, retry with rotation."""

    def __init__(
        self,
        timeout: int | None = None,
        max_retries: int | None = None,
        cache_seconds: int | None = None,
    ) -> None:
        self._timeout = timeout if timeout is not None else RPC_TIMEOUT
        self._max_retries = max_retries if max_retries is not None else RPC_MAX_RETRIES
        self._cache_seconds = cache_seconds if cache_seconds is not None else RPC_CACHE_SECONDS
        self._cached_url: str | None = None
        self._cache_until: float = 0.0
        self._current_idx = 0
        self._endpoints: list[str] = []
        _init_metrics()

    def _refresh_endpoints(self) -> list[str]:
        self._endpoints = get_rpc_endpoints()
        return self._endpoints

    def _pick_best(self) -> str | None:
        """Return fastest healthy RPC URL, or None if all fail."""
        endpoints = self._endpoints or self._refresh_endpoints()
        if not endpoints:
            return None
        results: list[tuple[str, float]] = []
        for url in endpoints:
            ok, latency = check_rpc_health(url, timeout=self._timeout)
            if ok and latency is not None:
                results.append((url, latency))
        if not results:
            return None
        results.sort(key=lambda x: x[1])
        return results[0][0]

    def get_url(self) -> str | None:
        """Return best RPC URL. Cached for RPC_CACHE_SECONDS. Refreshes on expiry."""
        now = time.monotonic()
        if self._cached_url and now < self._cache_until:
            return self._cached_url
        url = self._pick_best()
        if url:
            self._cached_url = url
            self._cache_until = now + self._cache_seconds
        return url

    def invalidate_cache(self) -> None:
        """Force next get_url to re-check health."""
        self._cached_url = None
        self._cache_until = 0.0

    def get_client(self):
        """Return solana.rpc.api.Client for best RPC. Raises RuntimeError if no healthy RPC."""
        from solana.rpc.api import Client

        url = self.get_url()
        if not url:
            raise RuntimeError("No healthy RPC endpoint available")
        return Client(url)

    def rpc_post(
        self,
        method: str,
        params: list[Any],
        *,
        rpc_id: str = "blockid-rpc",
    ) -> dict[str, Any] | None:
        """
        Execute JSON-RPC call with failover. Retries RPC_MAX_RETRIES times,
        rotating to next endpoint on each failure.
        Returns parsed response or None.
        """
        import requests

        endpoints = self._endpoints or self._refresh_endpoints()
        if not endpoints:
            return None

        payload = {"jsonrpc": "2.0", "id": rpc_id, "method": method, "params": params}
        last_err: Exception | None = None

        for attempt in range(self._max_retries):
            # Rotate on retry
            idx = (self._current_idx + attempt) % len(endpoints)
            url = endpoints[idx]
            start = time.perf_counter()
            try:
                r = requests.post(url, json=payload, timeout=self._timeout)
                r.raise_for_status()
                data = r.json()
                elapsed = time.perf_counter() - start

                if _rpc_latency is not None:
                    _rpc_latency.labels(rpc_host=_safe_host(url)).observe(elapsed)

                err = data.get("error")
                if err:
                    raise RuntimeError(str(err))
                return data
            except Exception as e:
                last_err = e
                if _rpc_failures is not None:
                    _rpc_failures.labels(rpc_host=_safe_host(url)).inc()
                logger.warning(
                    "rpc_request_failed",
                    url=_safe_host(url),
                    method=method,
                    attempt=attempt + 1,
                    error=str(e),
                )
                self._current_idx = (idx + 1) % len(endpoints)
                if _rpc_switch_count is not None:
                    _rpc_switch_count.inc()
                next_url = endpoints[self._current_idx]
                logger.info(
                    "[rpc_failover] switched to %s",
                    _safe_host(next_url),
                )

        logger.error("rpc_retries_exhausted", method=method, error=str(last_err))
        return None

    def with_failover(self, fn, *args, **kwargs):
        """
        Execute fn(client) with failover. fn receives solana Client.
        On RPC error, invalidate cache, get new client, retry up to max_retries.
        """
        last_err: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                client = self.get_client()
                return fn(client, *args, **kwargs)
            except Exception as e:
                last_err = e
                self.invalidate_cache()
                if attempt < self._max_retries - 1:
                    next_url = self.get_url()
                    if next_url:
                        logger.info("[rpc_failover] switched to %s", _safe_host(next_url))
        raise last_err or RuntimeError("RPC failover exhausted")


_default_manager: RPCManager | None = None


def get_rpc_manager() -> RPCManager:
    """Return singleton RPC manager."""
    global _default_manager
    if _default_manager is None:
        _default_manager = RPCManager()
    return _default_manager
