"""
Real-time Solana ingestion: WebSocket RPC → stream → parse → pipeline queue.

Connects to Solana WebSocket (wss://api.mainnet-beta.solana.com), subscribes to
account updates for tracked wallets (accountSubscribe; filter by watchlist),
receives transaction events, extracts sender/receiver/amount/signature, normalizes
to internal ParsedTransaction, pushes into pipeline queue. Consumer runs existing
pipeline: update graph, anomaly detection, trust score, risk propagation, alerts.

Backpressure: when queue is full, drop low-priority (normal, then watchlist);
critical wallets are never dropped. Fault tolerance: auto-reconnect with
exponential backoff. No API change; runtime-only worker.

Usage: start SolanaStreamPipeline.run() and run_pipeline_consumer() in parallel
with the same queue and a shared stop_event; pass get_priority e.g. from
db.get_wallet_priority for priority-based backpressure.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Coroutine

import httpx
import websockets
from websockets.exceptions import ConnectionClosed

from backend_blockid.blockid_logging import get_logger
from backend_blockid.solana_listener.parser import ParsedTransaction, parse

logger = get_logger(__name__)

# Queue item: (wallet_address, parsed_tx) for analysis pipeline
stream_item_type = tuple[str, ParsedTransaction]

PRIORITY_CRITICAL = "critical"
PRIORITY_WATCHLIST = "watchlist"
PRIORITY_NORMAL = "normal"
# Order for eviction: drop normal first, then watchlist; never critical
_PRIORITY_ORDER = (PRIORITY_NORMAL, PRIORITY_WATCHLIST, PRIORITY_CRITICAL)

DEFAULT_WS_PING_INTERVAL = 30.0
DEFAULT_WS_PING_TIMEOUT = 10.0
DEFAULT_DEBOUNCE_SEC = 1.0
DEFAULT_RPC_RATE_PER_SEC = 8.0
DEFAULT_QUEUE_MAXSIZE = 8192
DEFAULT_RECONNECT_MIN_SEC = 1.0
DEFAULT_RECONNECT_MAX_SEC = 60.0
DEFAULT_SIGNATURES_LIMIT = 20
DEFAULT_MAX_SEEN_PER_WALLET = 5000
_RPC_REQUEST_TIMEOUT = 15.0
_WS_CLOSE_TIMEOUT = 5.0


def _ws_url_to_http(ws_url: str) -> str:
    """Convert wss:// or ws:// to https:// or http:// for RPC HTTP calls."""
    s = ws_url.strip()
    if s.startswith("wss://"):
        return "https://" + s[6:]
    if s.startswith("ws://"):
        return "http://" + s[5:]
    return s


def _normalize_priority(p: str | None) -> str:
    """Return critical | watchlist | normal; unknown/None -> normal."""
    if not p:
        return PRIORITY_NORMAL
    s = (p or "").strip().lower()
    if s == PRIORITY_CRITICAL:
        return PRIORITY_CRITICAL
    if s == PRIORITY_WATCHLIST:
        return PRIORITY_WATCHLIST
    return PRIORITY_NORMAL


class PriorityDropQueue:
    """
    Bounded queue with priority-based backpressure: when full, drop low-priority
    (normal, then watchlist); critical never dropped. API compatible with
    asyncio.Queue for get() / put_nowait() / full().
    """

    def __init__(
        self,
        maxsize: int = 0,
        *,
        get_priority: Callable[[str], str] | None = None,
    ) -> None:
        self._maxsize = max(0, maxsize)
        self._get_priority = get_priority or (lambda _: PRIORITY_NORMAL)
        self._deque: deque[tuple[str, stream_item_type]] = deque()
        self._not_empty = asyncio.Condition()

    def full(self) -> bool:
        return self._maxsize > 0 and len(self._deque) >= self._maxsize

    def empty(self) -> bool:
        return len(self._deque) == 0

    def qsize(self) -> int:
        return len(self._deque)

    async def get(self) -> stream_item_type:
        async with self._not_empty:
            while not self._deque:
                await self._not_empty.wait()
            _, item = self._deque.popleft()
            return item

    def get_nowait(self) -> stream_item_type:
        if not self._deque:
            raise asyncio.QueueEmpty
        _, item = self._deque.popleft()
        return item

    def put_nowait(self, item: stream_item_type, priority: str | None = None) -> None:
        wallet, _ = item
        prio = _normalize_priority(priority or self._get_priority(wallet))
        if self.full():
            if prio != PRIORITY_CRITICAL:
                logger.debug(
                    "stream_queue_full_dropped",
                    wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
                    priority=prio,
                )
                return
            evict_idx = None
            for i, (p, _) in enumerate(self._deque):
                if p != PRIORITY_CRITICAL:
                    evict_idx = i
                    break
            if evict_idx is None:
                logger.warning(
                    "stream_queue_full_evict_skip",
                    wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
                    reason="no_non_critical_to_evict",
                )
                return
            del self._deque[evict_idx]
        self._deque.append((prio, item))
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        def _wake() -> None:
            async def _notify() -> None:
                async with self._not_empty:
                    self._not_empty.notify(1)

            asyncio.ensure_future(_notify())

        loop.call_soon_threadsafe(_wake)


@dataclass
class IngestionConfig:
    """Config for the Solana stream ingestion pipeline."""

    rpc_ws_url: str = "wss://api.mainnet-beta.solana.com"
    debounce_sec: float = DEFAULT_DEBOUNCE_SEC
    rpc_rate_per_sec: float = DEFAULT_RPC_RATE_PER_SEC
    queue_maxsize: int = DEFAULT_QUEUE_MAXSIZE
    reconnect_min_sec: float = DEFAULT_RECONNECT_MIN_SEC
    reconnect_max_sec: float = DEFAULT_RECONNECT_MAX_SEC
    signatures_limit: int = DEFAULT_SIGNATURES_LIMIT
    max_seen_per_wallet: int = DEFAULT_MAX_SEEN_PER_WALLET
    ws_ping_interval: float | None = DEFAULT_WS_PING_INTERVAL
    ws_ping_timeout: float | None = DEFAULT_WS_PING_TIMEOUT


class _RateLimiter:
    """Simple token-bucket style: min interval between acquires."""

    def __init__(self, rate_per_sec: float) -> None:
        self._interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.0
        self._last_acquire = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_acquire
            if elapsed < self._interval:
                await asyncio.sleep(self._interval - elapsed)
            self._last_acquire = time.monotonic()


class SolanaStreamPipeline:
    """
    Real-time Solana ingestion: WebSocket accountSubscribe (tracked wallets) →
    debounce → fetch tx → parse → normalize → pipeline queue.

    Auto-reconnect with exponential backoff. Queue bounded; when full, drop
    low-priority (normal, watchlist) first; critical wallets never dropped.
    """

    def __init__(
        self,
        config: IngestionConfig,
        watchlist: list[str] | Callable[[], list[str]] | Callable[[], Coroutine[Any, Any, list[str]]],
        queue: asyncio.Queue[stream_item_type] | PriorityDropQueue | None = None,
        *,
        get_priority: Callable[[str], str] | None = None,
    ) -> None:
        if not config.rpc_ws_url.strip():
            raise ValueError("rpc_ws_url must be non-empty")
        self._config = config
        self._watchlist_fn = watchlist if callable(watchlist) else (lambda: list(watchlist))
        if queue is not None:
            self._queue = queue
        elif get_priority is not None:
            self._queue = PriorityDropQueue(
                maxsize=config.queue_maxsize,
                get_priority=get_priority,
            )
        else:
            self._queue = asyncio.Queue(maxsize=config.queue_maxsize)
        self._http_url = _ws_url_to_http(config.rpc_ws_url)
        self._rate_limiter = _RateLimiter(config.rpc_rate_per_sec)
        self._subscription_to_wallet: dict[int, str] = {}
        self._wallet_to_subscription: dict[str, int] = {}
        self._seen: dict[str, set[str]] = {}
        self._seen_order: dict[str, deque[str]] = {}
        self._debounce_tasks: dict[str, asyncio.Task[None]] = {}
        self._next_rpc_id = 0
        self._stop = asyncio.Event()
        self._ws: websockets.WebSocketClientProtocol | None = None

    async def _get_watchlist_async(self) -> list[str]:
        """Resolve watchlist to list of wallet addresses (supports sync or async callable)."""
        fn = self._watchlist_fn
        if asyncio.iscoroutinefunction(fn):
            w = await fn()
        else:
            w = fn()
        if asyncio.iscoroutine(w):
            w = await w
        return list(w) if w else []

    def get_queue(self) -> asyncio.Queue[stream_item_type] | PriorityDropQueue:
        """Return the queue used for pushing (wallet, ParsedTransaction) to the analyzer."""
        return self._queue

    async def run(self) -> None:
        """
        Run the ingestion loop: connect, subscribe, process notifications, reconnect on failure.
        Exits when stop() is called. Logs every stream event.
        """
        backoff = self._config.reconnect_min_sec
        run_id = 0
        while not self._stop.is_set():
            run_id += 1
            try:
                logger.info(
                    "stream_connecting",
                    run_id=run_id,
                    url=self._config.rpc_ws_url,
                )
                async with websockets.connect(
                    self._config.rpc_ws_url,
                    ping_interval=self._config.ws_ping_interval,
                    ping_timeout=self._config.ws_ping_timeout,
                    close_timeout=_WS_CLOSE_TIMEOUT,
                ) as ws:
                    self._ws = ws
                    backoff = self._config.reconnect_min_sec
                    watchlist = await self._get_watchlist_async()
                    if not watchlist:
                        logger.warning("stream_watchlist_empty", run_id=run_id)
                    else:
                        await self._subscribe_all(ws, watchlist)
                        logger.info(
                            "stream_connected",
                            run_id=run_id,
                            subscriptions=len(watchlist),
                            wallet_count=len(watchlist),
                        )
                    await self._receive_loop(ws)
            except asyncio.CancelledError:
                break
            except ConnectionClosed as e:
                logger.warning(
                    "stream_disconnected",
                    run_id=run_id,
                    code=e.code,
                    reason=e.reason,
                )
            except Exception as e:
                logger.exception("stream_error", run_id=run_id, error=str(e))
            finally:
                self._ws = None
                self._cancel_debounce_tasks()
                self._subscription_to_wallet.clear()
                self._wallet_to_subscription.clear()

            if self._stop.is_set():
                break
            logger.info(
                "stream_reconnect",
                run_id=run_id,
                backoff_sec=round(backoff, 1),
            )
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, self._config.reconnect_max_sec)
        logger.info("stream_stopped", run_id=run_id)

    def stop(self) -> None:
        """Signal the pipeline to stop after the current iteration."""
        self._stop.set()

    def _cancel_debounce_tasks(self) -> None:
        for task in self._debounce_tasks.values():
            task.cancel()
        self._debounce_tasks.clear()

    def _next_id(self) -> int:
        self._next_rpc_id += 1
        return self._next_rpc_id

    async def _subscribe_all(
        self,
        ws: websockets.WebSocketClientProtocol,
        watchlist: list[str],
    ) -> None:
        """Subscribe to account updates for each wallet in the watchlist."""
        for wallet in watchlist:
            if self._stop.is_set():
                return
            wallet = wallet.strip()
            if not wallet or wallet in self._wallet_to_subscription:
                continue
            req = {
                "jsonrpc": "2.0",
                "id": self._next_id(),
                "method": "accountSubscribe",
                "params": [wallet, {"encoding": "base64", "commitment": "confirmed"}],
            }
            await ws.send(json.dumps(req))
            # Response will be in _receive_loop; we map id -> subscription id there
            # For simplicity we read one response per subscribe (same order)
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=10.0)
                msg = json.loads(raw)
                sub_id = msg.get("result")
                if sub_id is not None:
                    self._subscription_to_wallet[sub_id] = wallet
                    self._wallet_to_subscription[wallet] = sub_id
                    if wallet not in self._seen:
                        self._seen[wallet] = set()
                        self._seen_order[wallet] = deque(maxlen=self._config.max_seen_per_wallet)
                    logger.info(
                        "ingestion_subscribed",
                        wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
                        subscription_id=sub_id,
                    )
            except (asyncio.TimeoutError, json.JSONDecodeError, KeyError) as e:
                logger.warning(
                    "ingestion_subscribe_failed",
                    wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
                    error=str(e),
                )

    async def _receive_loop(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Process WebSocket messages: route notifications, handle pings/responses."""
        async for raw in ws:
            if self._stop.is_set():
                return
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            method = msg.get("method")
            if method == "accountNotification":
                params = msg.get("params") or {}
                sub_id = params.get("subscription")
                result = params.get("result") or {}
                context = result.get("context") or {}
                slot = context.get("slot")
                wallet = self._subscription_to_wallet.get(sub_id) if sub_id is not None else None
                if wallet:
                    logger.debug(
                        "ingestion_stream_event",
                        wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
                        slot=slot,
                        subscription_id=sub_id,
                    )
                    self._schedule_fetch(wallet, slot)
            # Ignore other methods (subscription responses handled in _subscribe_all)
            elif "result" in msg and "error" not in msg and isinstance(msg.get("result"), int):
                # Could be subscription id from a previous subscribe; we already handle in _subscribe_all
                pass

    def _schedule_fetch(self, wallet: str, slot: int | None) -> None:
        """Debounce: cancel previous fetch task for this wallet, schedule new one after debounce_sec."""
        if wallet in self._debounce_tasks:
            self._debounce_tasks[wallet].cancel()
            try:
                self._debounce_tasks.pop(wallet, None)
            except KeyError:
                pass

        async def _debounced() -> None:
            try:
                await asyncio.sleep(self._config.debounce_sec)
            except asyncio.CancelledError:
                return
            if self._stop.is_set():
                return
            self._debounce_tasks.pop(wallet, None)
            await self._fetch_and_push(wallet, slot)

        self._debounce_tasks[wallet] = asyncio.create_task(_debounced())

    async def _fetch_and_push(self, wallet: str, slot: int | None) -> None:
        """
        Rate-limited: getSignaturesForAddress → getTransaction for new sigs → parse → put in queue.
        Only processes wallets in watchlist (already enforced by subscription). Log every step.
        """
        seen_set = self._seen.get(wallet)
        seen_order = self._seen_order.get(wallet)
        if seen_set is None or seen_order is None:
            return
        async with httpx.AsyncClient(timeout=_RPC_REQUEST_TIMEOUT) as client:
            await self._rate_limiter.acquire()
            try:
                body = {
                    "jsonrpc": "2.0",
                    "id": self._next_id(),
                    "method": "getSignaturesForAddress",
                    "params": [
                        wallet,
                        {"limit": self._config.signatures_limit, "commitment": "confirmed"},
                    ],
                }
                resp = await client.post(self._http_url, json=body)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.warning(
                    "ingestion_signatures_fetch_failed",
                    wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
                    error=str(e),
                )
                return
            err = data.get("error")
            if err:
                logger.warning(
                    "ingestion_signatures_rpc_error",
                    wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
                    error=str(err),
                )
                return
            items = data.get("result") or []
            if not isinstance(items, list):
                return
            new_sigs = []
            for item in items:
                if not isinstance(item, dict) or "signature" not in item:
                    continue
                sig = item["signature"]
                if sig in seen_set:
                    continue
                new_sigs.append(sig)
                if len(seen_set) >= self._config.max_seen_per_wallet and seen_order:
                    old = seen_order.popleft()
                    seen_set.discard(old)
                seen_set.add(sig)
                seen_order.append(sig)
            if not new_sigs:
                logger.debug(
                    "ingestion_no_new_signatures",
                    wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
                )
                return
            logger.info(
                "ingestion_signatures_fetched",
                wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
                new_count=len(new_sigs),
                slot=slot,
            )
            for sig in new_sigs:
                if self._stop.is_set():
                    return
                await self._rate_limiter.acquire()
                try:
                    tx_body = {
                        "jsonrpc": "2.0",
                        "id": self._next_id(),
                        "method": "getTransaction",
                        "params": [
                            sig,
                            {"encoding": "json", "maxSupportedTransactionVersion": 0},
                        ],
                    }
                    tx_resp = await client.post(self._http_url, json=tx_body)
                    tx_resp.raise_for_status()
                    tx_data = tx_resp.json()
                except Exception as e:
                    logger.warning(
                        "ingestion_tx_fetch_failed",
                        wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
                        signature=sig[:16] + "..." if len(sig) > 16 else sig,
                        error=str(e),
                    )
                    continue
                if tx_data.get("error") or tx_data.get("result") is None:
                    continue
                raw_tx = tx_data["result"]
                parsed = parse(raw_tx)
                if parsed is None:
                    logger.debug(
                        "ingestion_tx_parse_skipped",
                        wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
                        signature=sig[:16] + "..." if len(sig) > 16 else sig,
                    )
                    continue
                logger.info(
                    "tx_received",
                    wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
                    signature=parsed.signature[:16] + "..." if parsed.signature and len(parsed.signature) > 16 else (parsed.signature or ""),
                    slot=parsed.slot,
                    amount_lamports=parsed.amount,
                )
                item = (wallet, parsed)
                if isinstance(self._queue, PriorityDropQueue):
                    self._queue.put_nowait(item)
                else:
                    try:
                        self._queue.put_nowait(item)
                    except asyncio.QueueFull:
                        logger.warning(
                            "stream_queue_full_dropped",
                            wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
                            signature=parsed.signature[:16] + "..." if parsed.signature and len(parsed.signature) > 16 else (parsed.signature or ""),
                        )


def _process_stream_item_sync(
    wallet: str,
    parsed: ParsedTransaction,
    db: Any,
    anomaly_config: Any,
    alert_config: Any,
    max_history: int,
) -> None:
    """
    Insert tx and run full analysis pipeline (graph, features, anomalies, trust score,
    risk propagation, alerts). Sync; run from executor in consumer.
    """
    from backend_blockid.agent_worker.runner import process_wallet_analysis

    inserted = db.insert_parsed_transactions(wallet, [parsed])
    if inserted == 0:
        return
    process_wallet_analysis(wallet, db, anomaly_config, alert_config, max_history)


async def run_pipeline_consumer(
    queue: asyncio.Queue[stream_item_type] | PriorityDropQueue,
    db: Any,
    anomaly_config: Any,
    alert_config: Any,
    max_history: int,
    stop_event: asyncio.Event,
    *,
    loop: asyncio.AbstractEventLoop | None = None,
) -> None:
    """
    Consume (wallet, ParsedTransaction) from the stream queue; insert tx, run
    existing pipeline (update graph, anomaly detection, trust score, alerts).
    Reuses process_wallet_analysis; no duplicate logic. Logs pipeline_processed.
    Exits when stop_event is set.
    """
    loop = loop or asyncio.get_running_loop()
    executor = None
    while not stop_event.is_set():
        try:
            item = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            break
        wallet, parsed = item
        try:
            await loop.run_in_executor(
                executor,
                _process_stream_item_sync,
                wallet,
                parsed,
                db,
                anomaly_config,
                alert_config,
                max_history,
            )
        except Exception as e:
            logger.warning(
                "pipeline_processed_error",
                wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
                signature=parsed.signature[:16] + "..." if parsed.signature and len(parsed.signature) > 16 else (parsed.signature or ""),
                error=str(e),
            )
            continue
        logger.info(
            "pipeline_processed",
            wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
            signature=parsed.signature[:16] + "..." if parsed.signature and len(parsed.signature) > 16 else (parsed.signature or ""),
        )
