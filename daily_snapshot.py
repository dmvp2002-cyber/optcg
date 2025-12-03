import sqlite3
from datetime import datetime
import json
import aiohttp
import asyncio

DB_PATH = "history.db"
BASE_URL = "https://optcg.onrender.com"

# Load all cards
with open("all_cards.json", "r", encoding="utf-8") as f:
    all_cards = json.load(f)

today = datetime.utcnow().strftime("%Y-%m-%d")
print(f"Saving snapshot for {today}")

# ----------------------------
# BUILD CLEAN CARD ID
# ----------------------------
def build_card_id(card):
    """
    Produces EXACTLY what the API expects:
    OP12-030v=0
    OP09-005v=1
    PRB02-014v=0
    ST24-001v=0
    EB02-003v=0
    """
    code = card.get("code")
    version = card.get("version")

    if not code:
        return None

    if version is None:
        version = 0

    return f"{code}v={version}"


invalid_cards = []

# ----------------------------
# CONTROL CONCURRENCY
# ----------------------------
SEM = asyncio.Semaphore(10)   # <---- MAX 10 PARALLEL REQUESTS


# ----------------------------
# FETCH PRICE WITH RETRIES
# ----------------------------
async def fetch_price(session, cid, retries=3):
    async with SEM:
        url = f"{BASE_URL}/price/{cid}"

        for attempt in range(1, retries + 1):
            try:
                async with session.get(url, timeout=10) as resp:
                    status = resp.status

                    # 404 = card does not exist on Limitless
                    if status == 404:
                        invalid_cards.append(cid)
                        return None

                    if status == 200:
                        data = await resp.json()
                        return cid, data

                    print(f"{cid}: HTTP {status} (attempt {attempt})")

            except Exception as e:
                print(f"{cid}: ERROR {e} (attempt {attempt})")

            await asyncio.sleep(0.5)

        return None


# ----------------------------
# RUN ALL IN PARALLEL (10 at a time)
# ----------------------------
async def run_all():
    connector = aiohttp.TCPConnector(limit=50)
    tasks = []

    async with aiohttp.ClientSession(connector=connector) as session:
        for card in all_cards:
            cid = build_card_id(card)
            if cid:
                tasks.append(fetch_price(session, cid))

        results = await asyncio.gather(*tasks)

        rows = []
        for item in results:
            if not item:
                continue

            cid, data = item
            prices = data.get("prices", {})

            eur = float(prices.get("eur_price") or 0)
            usd = float(prices.get("usd_price") or 0)

            rows.append((cid, today, eur, usd))
            print(f"Saved {cid}: {eur} EUR")

        return rows


# ----------------------------
# EXECUTE
# ----------------------------
rows = asyncio.run(run_all())

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

if rows:
    cursor.executemany(
        "INSERT INTO card_history (card_id, date, eur_price, usd_price) VALUES (?, ?, ?, ?)",
        rows
    )
    print(f"Inserted {len(rows)} rows")

conn.commit()
conn.close()

# Print invalid cards
if invalid_cards:
    print("\n⚠️ INVALID CARDS:")
    for c in sorted(set(invalid_cards)):
        print(" -", c)

print("\nDaily snapshot complete! 🎉")
