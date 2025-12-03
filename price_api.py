from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import json
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import os
import sqlite3  # <-- NEW

# Path to your history database (created by daily_snapshot.py)
DB_PATH = "history.db"

# Simple cache: { card_id: { "timestamp": datetime, "data": {...} } }
PRICE_CACHE = {}
CACHE_TTL = timedelta(hours=24)
CACHE_FILE = "price_cache.json"

# Load persistent cache at startup
if os.path.exists(CACHE_FILE):
    try:
        with open(CACHE_FILE, "r") as f:
            raw = json.load(f)
            for key, val in raw.items():
                PRICE_CACHE[key] = {
                    "timestamp": datetime.fromisoformat(val["timestamp"]),
                    "data": val["data"],
                }
        print("Loaded persistent cache:", len(PRICE_CACHE))
    except Exception as e:
        print("Failed to load persistent cache:", e)


def save_cache_to_disk():
    try:
        serializable = {
            key: {
                "timestamp": val["timestamp"].isoformat(),
                "data": val["data"],
            }
            for key, val in PRICE_CACHE.items()
        }
        with open(CACHE_FILE, "w") as f:
            json.dump(serializable, f)
    except Exception as e:
        print("Failed to save cache:", e)


app = FastAPI()

# Allow Flutter (macOS, web, mobile) to access the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_URL = "https://onepiece.limitlesstcg.com/cards/{}"


# ------------------------------------------------------
# FIXED SCRAPER — version-correct prices + links restored
# ------------------------------------------------------
def scrape_prices(card_id: str):
    card_id = card_id.upper().replace("?", "")

    # Extract base ID (handles OP, EB, ST, PRB, PR, CP, etc.)
    m = re.match(r"([A-Z]+[0-9]{2}-[0-9]{3})", card_id)
    if not m:
        raise ValueError(f"Invalid card_id format: {card_id}")

    base = m.group(1)

    # Extract version number
    m2 = re.search(r"V=(\d+)", card_id)
    version = int(m2.group(1)) if m2 else 0

    # Format ID for Limitless
    if version > 0:
        formatted = f"{base}?v={version}"
    else:
        formatted = base

    url = BASE_URL.format(formatted)
    r = requests.get(url)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # UNIVERSAL TABLE FINDER
    table = (
        soup.select_one("table.prints-table")
        or soup.select_one("div.card-prints table")
        or soup.select_one("div.price-table table")
        or soup.select_one("table")
    )

    if not table:
        print("NO TABLE FOUND for", card_id)
        return {
            "usd_price": 0,
            "usd_url": None,
            "eur_price": 0,
            "eur_url": None,
        }

    dollar = []
    euro = []
    usd_urls = []
    eur_urls = []

    for row in table.select("tr"):
        if row.find("th"):
            continue

        # USD price + URL
        usd_link = row.select_one("a.card-price.usd")
        if usd_link:
            m_usd = re.search(r"([\d\.,]+)", usd_link.get_text())
            usd_price = float(m_usd.group(1).replace(",", "")) if m_usd else 0
            usd_url = usd_link["href"]
        else:
            usd_price = 0
            usd_url = None

        # EUR price + URL
        eur_link = row.select_one("a.card-price.eur")
        if eur_link:
            m_eur = re.search(r"([\d\.,]+)", eur_link.get_text())
            eur_price = float(m_eur.group(1).replace(",", "")) if m_eur else 0
            eur_url = eur_link["href"]
        else:
            eur_price = 0
            eur_url = None

        dollar.append(usd_price)
        euro.append(eur_price)
        usd_urls.append(usd_url)
        eur_urls.append(eur_url)

    # Avoid out-of-range version index
    if version >= len(dollar):
        version = 0

    return {
        "usd_price": dollar[version],
        "usd_url": usd_urls[version],
        "eur_price": euro[version],
        "eur_url": eur_urls[version],
    }


@app.get("/price/{card_id}")
def get_price(card_id: str):
    now = datetime.utcnow()

    # Check cache
    cached = PRICE_CACHE.get(card_id)
    if cached and now - cached["timestamp"] < CACHE_TTL:
        return {"card_id": card_id, "prices": cached["data"], "cached": True}

    # Not cached or expired → scrape
    prices = scrape_prices(card_id)

    # Save to cache
    PRICE_CACHE[card_id] = {"timestamp": now, "data": prices}
    save_cache_to_disk()

    return {"card_id": card_id, "prices": prices, "cached": False}


# ------------------------------------------------------
# NEW: HISTORY ENDPOINT (for graphs)
# ------------------------------------------------------
@app.get("/history/{card_id}")
def get_history(card_id: str, limit: int = 365):
    """
    Returns historical EUR/USD prices for a given card_id, e.g.
    /history/OP13-001v=0?limit=90
    """
    # Normalize the ID in the same style as stored in history.db
    cid = card_id.upper().replace("?", "").strip()

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT date, eur_price, usd_price
            FROM card_history
            WHERE card_id = ?
            ORDER BY date ASC
            LIMIT ?
            """,
            (cid, limit),
        )

        rows = cursor.fetchall()
        conn.close()

        history = [
            {"date": d, "eur": eur, "usd": usd}
            for (d, eur, usd) in rows
        ]

        return {
            "card_id": cid,
            "count": len(history),
            "history": history,
        }

    except Exception as e:
        return {"card_id": cid, "error": str(e)}


if __name__ == "__main__":
    uvicorn.run("price_api:app", host="0.0.0.0", port=8000, reload=True)
