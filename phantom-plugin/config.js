/**
 * BlockID Phantom Plugin Configuration
 * Privacy: Only public wallet addresses are sent. Never private keys.
 */

const CONFIG = {
  API_BASE: "https://api.blockidscore.fun",
  // Fallback for local dev: "http://localhost:8000",

  // Score thresholds
  WARNING_THRESHOLD: 40,   // Show warning popup
  ALERT_THRESHOLD: 20,     // Show strong red alert

  // Cache wallet score for 10 minutes (ms)
  CACHE_TTL_MS: 10 * 60 * 1000,

  // Investigation app URL
  INVESTIGATION_BASE: "https://app.blockidscore.fun/wallet",

  // Badge color rules
  BADGE_COLORS: {
    SCAM: "#dc2626",
    SCAM_SUSPECTED: "#dc2626",
    HIGH_RISK: "#ea580c",
    MEDIUM_RISK: "#f59e0b",
    LOW_RISK: "#22c55e",
    SAFE: "#22c55e",
    TRUSTED: "#22c55e",
    UNKNOWN: "#6b7280",
  },
};
