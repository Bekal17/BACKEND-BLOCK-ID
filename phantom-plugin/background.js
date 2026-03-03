/**
 * BlockID Phantom Plugin - Background Service Worker
 * Fetches wallet trust scores and transaction checks from BlockID API.
 * Privacy: Only public wallet address + tx preview. Never private keys.
 */

const WALLET_CACHE_TTL_MS = 10 * 60 * 1000; // 10 min
const TX_CACHE_TTL_MS = 5 * 60 * 1000;      // 5 min
const API_BASE = "https://api.blockidscore.fun";

const walletCache = new Map();
const txCache = new Map(); // "from|to" -> { data, expires }

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (msg.type === "CHECK_WALLET") {
    checkWallet(msg.wallet).then(sendResponse);
    return true;
  }
  if (msg.type === "CHECK_TRANSACTION") {
    checkTransaction(msg.payload).then(sendResponse);
    return true;
  }
});

async function checkWallet(wallet) {
  if (!wallet || typeof wallet !== "string") {
    return { error: "invalid_wallet" };
  }
  const addr = wallet.trim();
  if (addr.length < 32) {
    return { error: "invalid_wallet" };
  }

  const cached = walletCache.get(addr);
  if (cached && Date.now() < cached.expires) {
    return cached.data;
  }

  try {
    const url = `${API_BASE}/wallet/${addr}/badge`;
    const res = await fetch(url);
    if (res.status === 404) {
      const data = { wallet: addr, score: null, badge: "UNKNOWN", risk: null, top_reason: null };
      walletCache.set(addr, { data, expires: Date.now() + WALLET_CACHE_TTL_MS });
      return data;
    }
    if (!res.ok) {
      return { error: "api_error", status: res.status };
    }
    const json = await res.json();
    const topReasons = json.top_reasons || json.top_reason_codes || [];
    const topReason = Array.isArray(topReasons) ? topReasons[0] : (topReasons[0]?.code || topReasons[0]);
    const data = {
      wallet: addr,
      score: json.score ?? null,
      badge: json.badge || "UNKNOWN",
      risk: json.risk ?? null,
      confidence: json.confidence ?? null,
      top_reason: topReason || null,
    };
    walletCache.set(addr, { data, expires: Date.now() + WALLET_CACHE_TTL_MS });
    return data;
  } catch (e) {
    return { error: "network_error", message: String(e.message) };
  }
}

async function checkTransaction(payload) {
  const { from: fromW, to: toW, token = "SOL", amount = null } = payload || {};
  if (!toW || typeof toW !== "string" || toW.length < 32) {
    return { error: "invalid_to_wallet" };
  }

  const cacheKey = `${fromW || ""}|${toW}`;
  const cached = txCache.get(cacheKey);
  if (cached && Date.now() < cached.expires) {
    return cached.data;
  }

  try {
    const res = await fetch(`${API_BASE}/transaction/check`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        from: fromW || "",
        to: toW,
        token: token || "SOL",
        amount: amount,
      }),
    });
    if (!res.ok) {
      return { error: "api_error", status: res.status };
    }
    const data = await res.json();
    txCache.set(cacheKey, { data, expires: Date.now() + TX_CACHE_TTL_MS });
    return data;
  } catch (e) {
    return { error: "network_error", message: String(e.message) };
  }
}
