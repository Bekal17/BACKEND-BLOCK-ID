# BlockID Phantom Wallet Plugin

Browser extension (Chrome/Brave) that warns users before interacting with risky Solana wallets. Uses the BlockID trust API.

## Features

- **Wallet check**: `GET /wallet/{wallet}/badge` — score, badge, risk
- **Transaction check**: `POST /transaction/check` — from, to, token, amount
- **UX tiers**: Low risk → yellow banner; High risk → full popup; Extreme → double confirm
- **Cache**: Wallet 10 min, transaction 5 min
- **Privacy**: Only public wallet + tx preview. Never private keys.

## Popup UI

- Wallet (shortened)
- Badge (e.g. SCAM SUSPECTED)
- Score
- Top Reason
- **View Investigation** → opens https://app.blockidscore.fun/wallet/{wallet}
- **Cancel Transaction**
- **Continue Anyway**

## Color Rules

| Badge         | Color  |
|---------------|--------|
| SCAM          | red    |
| SCAM_SUSPECTED| red    |
| HIGH_RISK     | orange |
| MEDIUM_RISK   | amber  |
| TRUSTED/SAFE  | green  |

## Installation

1. Open `chrome://extensions`
2. Enable "Developer mode"
3. Click "Load unpacked"
4. Select the `phantom-plugin` folder

## Configuration

Edit `background.js` to change:

- `API_BASE`: BlockID API URL (default: `https://api.blockidscore.fun`)
- `CACHE_TTL_MS`: Cache duration (default: 10 min)

Edit `content.js` to change:

- `WARNING_THRESHOLD`: Show warning when score < 40
- `ALERT_THRESHOLD`: Strong alert when score < 20

## Backend Requirements

- BlockID API must expose `GET /wallet/{wallet}/badge` with CORS enabled for extension origin
- Response shape: `{ score, badge, risk, top_reasons }`

## Logging

```
[phantom_warning] risky_wallet {"wallet":"8X35rQ...","score":12,"badge":"SCAM_SUSPECTED"}
```

## Transaction-Level Warning

Extracts from/to/token/amount from signing requests. Backend checks:
- Trust score of receiver
- Scam cluster distance
- Large unusual transfer
- Special warnings: new wallet + large transfer, drainer contract

Log: `[phantom_tx_warning] from=ABC to=XYZ risk=3`

## Future Upgrades

- Simulate tx result
- MEV sandwich detection
- Cross-chain wallet check
- Exchange wallet alerts
