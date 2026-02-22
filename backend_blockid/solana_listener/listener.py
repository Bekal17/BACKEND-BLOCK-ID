"""
Solana transaction listener — subscription and event emission.

Responsibilities:
- Establish and maintain connection to Solana RPC (polling getSignaturesForAddress).
- Parse and normalize incoming transaction data into a canonical format.
- Emit transaction events via callback for the analysis engine.
- Handle reconnection, exponential backoff, and graceful shutdown for 24/7 operation.
"""

import asyncio
import signal
from collections import deque
from typing import Any, Awaitable, Callable

import httpx

from backend_blockid.blockid_logging import get_logger
from backend_blockid.solana_listener.models import SignatureInfo

logger = get_logger(__name__)

# JSON-RPC request id counter
_request_id = 0


def _next_id() -> int:
    global _request_id
    _request_id += 1
    return _request_id


def _build_rpc_body(address: str, before: str | None, limit: int) -> dict[str, Any]:
    opts: dict[str, Any] = {"limit": limit, "commitment": "finalized"}
    if before is not None:
        opts["before"] = before
    return {
        "jsonrpc": "2.0",
        "id": _next_id(),
        "method": "getSignaturesForAddress",
        "params": [address, opts],
    }


class SolanaListener:
    """
    Polling-based Solana transaction listener for one or more wallets.

    Connects to Solana RPC, periodically fetches getSignaturesForAddress
    for each wallet, deduplicates by signature, and invokes an optional
    callback with new signatures. Designed for continuous 24/7 operation
    with retry and backoff.
    """

    def __init__(
        self,
        rpc_url: str,
        wallets: list[str],
        *,
        poll_interval_sec: float = 30.0,
        on_transaction: (
            Callable[[str, list[SignatureInfo]], Awaitable[None]]
            | Callable[[str, list[SignatureInfo]], None]
            | None
        ) = None,
        min_retry_delay_sec: float = 1.0,
        max_retry_delay_sec: float = 60.0,
        max_retries_per_poll: int = 5,
        request_timeout_sec: float = 30.0,
        signatures_limit_per_request: int = 1000,
        max_seen_signatures_per_wallet: int = 10_000,
    ) -> None:
        """
        Args:
            rpc_url: Solana RPC HTTP endpoint (e.g. https://api.mainnet-beta.solana.com).
            wallets: List of base58 wallet addresses to watch.
            poll_interval_sec: Seconds between full poll cycles (all wallets).
            on_transaction: Optional callback(wallet_address, list[SignatureInfo]) for new
                signatures; may be sync or async. Invoked once per wallet per cycle with
                only newly seen signatures (oldest-first within that batch).
            min_retry_delay_sec: Initial delay for exponential backoff on RPC errors.
            max_retry_delay_sec: Cap for backoff delay.
            max_retries_per_poll: Max retries per wallet per poll cycle before moving on.
            request_timeout_sec: HTTP timeout for each RPC request.
            signatures_limit_per_request: RPC limit (1–1000) per getSignaturesForAddress call.
            max_seen_signatures_per_wallet: Max signatures to keep in memory per wallet for dedup.
        """
        if not rpc_url.strip():
            raise ValueError("rpc_url must be non-empty")
        if not wallets:
            raise ValueError("wallets must be non-empty")
        if poll_interval_sec <= 0:
            raise ValueError("poll_interval_sec must be positive")
        if not (1 <= signatures_limit_per_request <= 1000):
            raise ValueError("signatures_limit_per_request must be between 1 and 1000")

        self._rpc_url = rpc_url.rstrip("/")
        self._wallets = list(wallets)
        self._poll_interval_sec = poll_interval_sec
        self._on_transaction = on_transaction
        self._min_retry_delay = min_retry_delay_sec
        self._max_retry_delay = max_retry_delay_sec
        self._max_retries_per_poll = max_retries_per_poll
        self._request_timeout = request_timeout_sec
        self._signatures_limit = signatures_limit_per_request
        self._max_seen = max_seen_signatures_per_wallet

        # Per-wallet: set for O(1) dedup + deque for FIFO eviction when over capacity
        self._seen: dict[str, set[str]] = {w: set() for w in self._wallets}
        self._seen_order: dict[str, deque[str]] = {w: deque() for w in self._wallets}
        self._stop_event = asyncio.Event()

    def start(self) -> None:
        """
        Run the listener until shutdown (SIGINT/SIGTERM or stop requested).

        Blocks the calling thread. Installs signal handlers for graceful
        shutdown where supported (SIGINT everywhere; SIGTERM on Unix).
        """
        def _handle_sig(signum: int, frame: Any) -> None:
            sig = "SIGINT" if signum == signal.SIGINT else "SIGTERM"
            logger.info("listener_shutdown_signal", signal=sig)
            self._stop_event.set()

        try:
            signal.signal(signal.SIGINT, _handle_sig)
            if hasattr(signal, "SIGTERM"):
                signal.signal(signal.SIGTERM, _handle_sig)
        except (ValueError, OSError):
            # Signal only valid in main thread / not supported on this platform
            pass

        try:
            asyncio.run(self._run_forever())
        except KeyboardInterrupt:
            logger.info("listener_keyboard_interrupt")
        finally:
            logger.info("listener_stopped")

    async def stop(self) -> None:
        """Request shutdown; the poll loop will exit after the current cycle."""
        self._stop_event.set()

    async def _run_forever(self) -> None:
        logger.info(
            "listener_started",
            wallet_count=len(self._wallets),
            poll_interval_sec=self._poll_interval_sec,
            rpc_url=self._rpc_url,
        )
        while not self._stop_event.is_set():
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("listener_poll_cycle_error", error=str(e))
            if self._stop_event.is_set():
                break
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._poll_interval_sec
                )
            except asyncio.TimeoutError:
                pass
        logger.info("listener_poll_loop_exited")

    async def _poll_once(self) -> None:
        """Fetch signatures for all wallets and emit new ones via callback."""
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(self._request_timeout)
        ) as client:
            for wallet in self._wallets:
                if self._stop_event.is_set():
                    return
                new_sigs = await self._fetch_new_signatures(client, wallet)
                if new_sigs and self._on_transaction:
                    await self._dispatch(wallet, new_sigs)

    def _mark_seen(self, wallet: str, sig: str) -> None:
        """Mark signature as seen; evict oldest if over capacity."""
        seen_set = self._seen[wallet]
        order = self._seen_order[wallet]
        if sig in seen_set:
            return
        if len(seen_set) >= self._max_seen:
            oldest = order.popleft()
            seen_set.discard(oldest)
        seen_set.add(sig)
        order.append(sig)

    async def _fetch_new_signatures(
        self, client: httpx.AsyncClient, wallet: str
    ) -> list[SignatureInfo]:
        """Fetch latest signatures for one wallet with retry; return only new ones."""
        seen_set = self._seen[wallet]
        delay = self._min_retry_delay
        last_error: Exception | None = None

        for attempt in range(self._max_retries_per_poll):
            try:
                raw = await self._rpc_get_signatures(client, wallet, before=None)
                break
            except Exception as e:
                last_error = e
                logger.warning(
                    "listener_rpc_retry",
                    wallet_id=wallet[:8] + "...",
                    attempt=attempt + 1,
                    max_retries=self._max_retries_per_poll,
                    error=str(e),
                )
                if attempt + 1 < self._max_retries_per_poll:
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, self._max_retry_delay)
                else:
                    logger.error(
                        "listener_rpc_give_up",
                        wallet_id=wallet[:8] + "...",
                        max_retries=self._max_retries_per_poll,
                        error=str(last_error),
                    )
                    return []

        # RPC returns newest first; we want to emit oldest-first (chronological)
        items = raw if isinstance(raw, list) else []
        infos: list[SignatureInfo] = []
        for item in items:
            if not isinstance(item, dict) or "signature" not in item:
                continue
            sig = item["signature"]
            if sig in seen_set:
                continue
            try:
                infos.append(SignatureInfo.from_rpc_item(item))
            except (KeyError, TypeError) as e:
                logger.debug("Skip invalid signature item: %s", e)
                continue
            self._mark_seen(wallet, sig)

        # Emit in chronological order (oldest first)
        infos.reverse()
        if infos:
            logger.info(
                "listener_new_signatures",
                wallet_id=wallet,
                signature_count=len(infos),
                oldest_slot=infos[0].slot if infos else None,
            )
        return infos

    async def _rpc_get_signatures(
        self,
        client: httpx.AsyncClient,
        address: str,
        before: str | None,
    ) -> list[dict[str, Any]]:
        """Perform getSignaturesForAddress JSON-RPC call; raise on transport or RPC error."""
        body = _build_rpc_body(address, before, self._signatures_limit)
        resp = await client.post(self._rpc_url, json=body)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            err = data["error"]
            raise RuntimeError(
                f"Solana RPC error: {err.get('message', err)} (code={err.get('code')})"
            )
        result = data.get("result")
        if result is None:
            raise RuntimeError("Solana RPC returned no result")
        return result

    async def _dispatch(self, wallet: str, infos: list[SignatureInfo]) -> None:
        """Call on_transaction with wallet and new signatures; support sync or async callback."""
        cb = self._on_transaction
        if cb is None:
            return
        try:
            if asyncio.iscoroutinefunction(cb):
                await cb(wallet, infos)
            else:
                # Run sync callback in executor to avoid blocking the loop
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, lambda: cb(wallet, infos))
        except Exception as e:
            logger.exception(
                "listener_callback_failed",
                wallet_id=wallet,
                error=str(e),
            )
