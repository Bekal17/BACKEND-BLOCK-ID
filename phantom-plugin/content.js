/**
 * BlockID Phantom Plugin - Content Script
 * Intercepts Phantom wallet connect and transaction signing.
 * Transaction-level warning: checks from/to/token/amount via POST /transaction/check.
 * UX: Low risk → banner, High risk → full popup, Extreme → double confirm.
 * Privacy: Only public wallet + tx preview. Never private keys.
 */

(function () {
  "use strict";

  const WARNING_THRESHOLD = 40;
  const ALERT_THRESHOLD = 20;
  const INVESTIGATION_BASE = "https://app.blockidscore.fun/wallet";
  const RISK_LOW = 0;
  const RISK_MEDIUM = 1;
  const RISK_HIGH = 2;
  const RISK_EXTREME = 3;
  const BADGE_COLORS = {
    SCAM: "#dc2626",
    SCAM_SUSPECTED: "#dc2626",
    HIGH_RISK: "#ea580c",
    MEDIUM_RISK: "#f59e0b",
    LOW_RISK: "#22c55e",
    SAFE: "#22c55e",
    TRUSTED: "#22c55e",
    UNKNOWN: "#6b7280",
  };

  function log(msg, data = {}) {
    console.log("[phantom_warning]", msg, JSON.stringify(data));
  }

  function getBadgeColor(badge) {
    const key = String(badge || "UNKNOWN").toUpperCase().replace(/-/g, "_");
    return BADGE_COLORS[key] || BADGE_COLORS.UNKNOWN;
  }

  function extractTransactionPreview(tx) {
    const previews = [];
    let fromAddr = "";
    try {
      const fp = tx?.feePayer || tx?.message?.feePayer;
      fromAddr = fp?.toBase58?.() || String(fp || "").trim();
    } catch (_) {}
    const keys = extractAccountKeysFromTransaction(tx);
    const others = keys.filter((k) => k && k !== fromAddr);
    let amount = null;
    let token = "SOL";
    try {
      const ix = tx?.instructions?.[0] || tx?.message?.instructions?.[0];
      if (ix?.programId) {
        const pid = String(ix.programId?.toBase58?.() || ix.programId || "");
        if (pid && !pid.startsWith("11111111111111111111111111111111")) token = "TOKEN";
      }
      const data = ix?.data;
      if (data) {
        let arr = null;
        if (typeof data === "string") arr = b64Decode(data);
        else if (data instanceof Uint8Array || Array.isArray(data)) arr = new Uint8Array(data);
        if (arr && arr.length >= 12) {
          const view = new DataView(arr.buffer);
          if (view.getUint32(0, true) === 2) amount = Number(view.getBigUint64(4, true)) / 1e9;
        }
      }
    } catch (_) {}
    if (others.length > 0) {
      for (const to of others) {
        if (to && to.length >= 32) previews.push({ from: fromAddr, to, token, amount });
      }
    } else if (keys.length > 0) {
      for (const k of keys) {
        if (k && k.length >= 32 && k !== fromAddr) previews.push({ from: fromAddr, to: k, token, amount });
      }
    }
    return previews;
  }

  function b64Decode(s) {
    try {
      return Uint8Array.from(atob(s.replace(/-/g, "+").replace(/_/g, "/")), (c) => c.charCodeAt(0));
    } catch (_) {
      return null;
    }
  }

  function extractAccountKeysFromTransaction(tx) {
    const keys = new Set();
    try {
      if (tx?.message?.accountKeys) {
        tx.message.accountKeys.forEach((k) => keys.add(k?.toBase58?.() || String(k)));
      }
      if (tx?.message?.staticAccountKeys) {
        tx.message.staticAccountKeys.forEach((k) => keys.add(k?.toBase58?.() || String(k)));
      }
      (tx?.instructions || []).forEach((ix) => {
        (ix?.keys || ix?.accountKeys || []).forEach((k) => {
          const pub = k?.pubkey || k;
          keys.add(pub?.toBase58?.() || String(pub));
        });
      });
    } catch (_) {}
    return Array.from(keys).filter((k) => k && k.length >= 32);
  }

  function showTransactionWarningModal(txData, onContinue, onCancel) {
    if (document.getElementById("blockid-tx-warning-modal")) return;

    const risk = txData.risk_level ?? 2;
    const isExtreme = risk >= RISK_EXTREME;
    const color = getBadgeColor(txData.to_badge);
    const shortTo = txData.to_wallet ? `${txData.to_wallet.slice(0, 6)}...${txData.to_wallet.slice(-4)}` : "—";
    const amountStr = txData.amount != null ? `${txData.amount} ${txData.token || "SOL"}` : "funds";
    const topReason = txData.top_reason || txData.warning_reason || "Risk indicators detected";
    const clusterNote = txData.cluster_link ? " (Cluster link)" : "";

    const modal = document.createElement("div");
    modal.id = "blockid-tx-warning-modal";
    modal.innerHTML = `
      <div class="blockid-overlay"></div>
      <div class="blockid-modal blockid-tx-modal ${isExtreme ? "blockid-alert" : ""}">
        <div class="blockid-modal-header" style="border-color: ${color}; background: #fef2f2;">
          <h3>⚠️ You are sending ${amountStr} to ${shortTo}</h3>
          <p class="blockid-tx-sub">${(txData.to_badge || "HIGH_RISK").replace(/_/g, " ")} wallet</p>
        </div>
        <div class="blockid-modal-body">
          <p><strong>Badge:</strong> <span style="color:${color}">${(txData.to_badge || "UNKNOWN").replace(/_/g, " ")}</span></p>
          <p><strong>Top Reason:</strong> ${String(topReason).replace(/_/g, " ")}${clusterNote}</p>
          ${isExtreme ? "<p class=\"blockid-extreme-note\">Please confirm you understand the risk.</p>" : ""}
        </div>
        <div class="blockid-modal-actions">
          <a href="${INVESTIGATION_BASE}/${encodeURIComponent(txData.to_wallet || "")}" target="_blank" rel="noopener" class="blockid-btn blockid-btn-secondary">View Investigation</a>
          <button type="button" class="blockid-btn blockid-btn-danger" id="blockid-tx-cancel">Cancel</button>
          <button type="button" class="blockid-btn blockid-btn-primary" id="blockid-tx-continue">Continue Anyway</button>
        </div>
      </div>
    `;

    document.body.appendChild(modal);

    const doContinue = () => {
      modal.remove();
      onContinue();
    };

    modal.querySelector("#blockid-tx-continue").onclick = () => {
      if (isExtreme) {
        if (confirm("This wallet has been flagged as high risk. Are you sure you want to proceed?")) doContinue();
      } else {
        doContinue();
      }
    };
    modal.querySelector("#blockid-tx-cancel").onclick = () => {
      modal.remove();
      onCancel();
    };
    modal.querySelector(".blockid-overlay").onclick = () => {
      modal.remove();
      onCancel();
    };
  }

  function showBanner(txData) {
    const existing = document.getElementById("blockid-banner");
    if (existing) return;
    const banner = document.createElement("div");
    banner.id = "blockid-banner";
    banner.className = "blockid-banner";
    banner.innerHTML = `
      <span>⚠️ Sending to moderate risk wallet. </span>
      <a href="${INVESTIGATION_BASE}/${encodeURIComponent(txData.to_wallet || "")}" target="_blank">View</a>
      <button id="blockid-banner-dismiss">×</button>
    `;
    document.body.appendChild(banner);
    banner.querySelector("#blockid-banner-dismiss").onclick = () => banner.remove();
    setTimeout(() => banner.remove(), 8000);
  }

  function showWarningModal(data, onContinue, onCancel) {
    if (document.getElementById("blockid-warning-modal")) return;

    const isStrongAlert = (data.score ?? 100) < ALERT_THRESHOLD;
    const color = getBadgeColor(data.badge);
    const shortWallet = data.wallet ? `${data.wallet.slice(0, 6)}...${data.wallet.slice(-4)}` : "—";
    const topReason = data.top_reason || "Risk indicators detected";

    const modal = document.createElement("div");
    modal.id = "blockid-warning-modal";
    modal.innerHTML = `
      <div class="blockid-overlay"></div>
      <div class="blockid-modal ${isStrongAlert ? "blockid-alert" : ""}">
        <div class="blockid-modal-header" style="border-color: ${color}">
          <h3>${isStrongAlert ? "⚠️ High Risk Wallet" : "⚠️ Wallet Risk Warning"}</h3>
        </div>
        <div class="blockid-modal-body">
          <p><strong>Wallet:</strong> <code>${shortWallet}</code></p>
          <p><strong>Badge:</strong> <span style="color:${color}">${(data.badge || "UNKNOWN").replace(/_/g, " ")}</span></p>
          <p><strong>Score:</strong> ${data.score ?? "N/A"}</p>
          <p><strong>Top Reason:</strong> ${String(topReason).replace(/_/g, " ")}</p>
        </div>
        <div class="blockid-modal-actions">
          <a href="${INVESTIGATION_BASE}/${encodeURIComponent(data.wallet)}" target="_blank" rel="noopener" class="blockid-btn blockid-btn-secondary">View Investigation</a>
          <button type="button" class="blockid-btn blockid-btn-danger" id="blockid-cancel">Cancel Transaction</button>
          <button type="button" class="blockid-btn blockid-btn-primary" id="blockid-continue">Continue Anyway</button>
        </div>
      </div>
    `;

    document.body.appendChild(modal);

    modal.querySelector("#blockid-continue").onclick = () => {
      modal.remove();
      onContinue();
    };
    modal.querySelector("#blockid-cancel").onclick = () => {
      modal.remove();
      onCancel();
    };
    modal.querySelector(".blockid-overlay").onclick = () => {
      modal.remove();
      onCancel();
    };
  }

  async function checkTransactionAndWarn(preview) {
    try {
      const data = await chrome.runtime.sendMessage({
        type: "CHECK_TRANSACTION",
        payload: {
          from: preview.from,
          to: preview.to,
          token: preview.token,
          amount: preview.amount,
        },
      });
      if (data?.error) return null;
      const risk = data?.risk_level ?? 0;
      if (risk > RISK_LOW) {
        log("phantom_tx_warning", { from: (preview.from || "").slice(0, 8), to: (preview.to || "").slice(0, 8), risk });
        return { ...data, to_wallet: preview.to, from_wallet: preview.from, amount: preview.amount, token: preview.token };
      }
    } catch (e) {
      log("tx_check_error", { error: String(e?.message || e) });
    }
    return null;
  }

  async function checkAndWarn(walletAddress, context) {
    if (!walletAddress || walletAddress.length < 32) return null;
    try {
      const data = await chrome.runtime.sendMessage({ type: "CHECK_WALLET", wallet: walletAddress });
      if (data?.error) return null;
      const score = data?.score;
      if (score != null && score < WARNING_THRESHOLD) {
        log("risky_wallet", { wallet: walletAddress.slice(0, 16) + "...", score, badge: data.badge });
        return data;
      }
    } catch (e) {
      log("check_error", { error: String(e?.message || e) });
    }
    return null;
  }

  function wrapPhantom() {
    const phantom = window.phantom?.solana || window.solana;
    if (!phantom || phantom.__blockid_wrapped) return;

    const origConnect = phantom.connect?.bind(phantom);
    if (origConnect) {
      phantom.connect = async function (opts) {
        const result = await origConnect(opts);
        const pk = result?.publicKey || phantom?.publicKey;
        const addr = pk?.toBase58?.() || pk;
        if (addr) {
          const riskData = await checkAndWarn(addr, "connect");
          if (riskData) {
            return new Promise((resolve, reject) => {
              showWarningModal(riskData, () => resolve(result), () => reject(new Error("User cancelled")));
            });
          }
        }
        return result;
      };
    }

    const origSignTx = phantom.signTransaction?.bind(phantom);
    if (origSignTx) {
      phantom.signTransaction = async function (tx) {
        const previews = extractTransactionPreview(tx);
        let txRiskData = null;
        for (const p of previews) {
          const r = await checkTransactionAndWarn(p);
          if (r && (txRiskData === null || (r.risk_level || 0) > (txRiskData.risk_level || 0))) txRiskData = r;
        }
        if (txRiskData) {
          return new Promise((resolve, reject) => {
            const risk = txRiskData.risk_level ?? 2;
            if (risk === RISK_MEDIUM) {
              showBanner(txRiskData);
              origSignTx(tx).then(resolve).catch(reject);
            } else {
              showTransactionWarningModal(
                txRiskData,
                () => origSignTx(tx).then(resolve).catch(reject),
                () => reject(new Error("User cancelled"))
              );
            }
          });
        }
        const keys = extractAccountKeysFromTransaction(tx);
        for (const addr of keys) {
          const riskData = await checkAndWarn(addr, "signTransaction");
          if (riskData) {
            return new Promise((resolve, reject) => {
              showWarningModal(
                riskData,
                () => origSignTx(tx).then(resolve).catch(reject),
                () => reject(new Error("User cancelled"))
              );
            });
          }
        }
        return origSignTx(tx);
      };
    }

    phantom.__blockid_wrapped = true;
    log("phantom_wrapped");
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => setTimeout(wrapPhantom, 500));
  } else {
    setTimeout(wrapPhantom, 500);
  }
  setInterval(wrapPhantom, 2000);
})();
