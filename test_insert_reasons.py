import asyncio
from backend_blockid.ai_engine.positive_reasons import detect_positive_reasons
from backend_blockid.database.repositories import insert_wallet_reason

WALLET = '9hXa2JEguhjX2ixNHrjnyhcoFduarSRazf8kEeKpLAEE'

async def main():
    # Step 1: detect
    reasons = await detect_positive_reasons(WALLET)
    print(f'Detected {len(reasons)} reasons:')
    for r in reasons:
        print(f'  {r["code"]} weight={r["weight"]}')
    
    # Step 2: insert one by one
    for r in reasons:
        try:
            await insert_wallet_reason(
                wallet=WALLET,
                reason_code=r['code'],
                weight=int(r.get('weight', 0)),
                confidence=float(r.get('confidence', 1.0)),
                tx_hash=r.get('tx_hash'),
            )
            print(f'  Inserted: {r["code"]}')
        except Exception as e:
            print(f'  ERROR inserting {r["code"]}: {e}')

asyncio.run(main())
