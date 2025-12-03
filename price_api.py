from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from dataclasses import dataclass
from typing import List, Optional
import json
import re
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta
import os
import sqlite3

# --- CREATE history.db automatically on startup ---
def init_history_db():
    conn = sqlite3.connect("history.db")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS card_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        card_id TEXT NOT NULL,
        date TEXT NOT NULL,
        eur_price REAL,
        usd_price REAL
    )
    """)

    conn.commit()
    conn.close()

# Run DB initialization
init_history_db()

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
                    "data": val["data"]
                }
        print("Loaded persistent cache:", len(PRICE_CACHE))
    except Exception as e:
        print("Failed to load persistent cache:", e)

def save_cache_to_disk():
    try:
        serializable = {
            key: {
                "timestamp": val["timestamp"].isoformat(),
                "data": val["data"]
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
# Example scraper function (replace with your real one)
def scrape_prices(card_id: str):
    card_id = str(card_id).upper()

    # Parse version
    if "V=" in card_id:
        base_id, version_str = card_id.split("V=")
        try:
            version = int(version_str)
        except:
            version = 0
    else:
        base_id = card_id
        version = 0

    base_id = base_id[:8]
    formatted_id = f"{base_id}?v={version}" if version > 0 else base_id
    url = BASE_URL.format(formatted_id)

    r = requests.get(url)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # Find prints table
    table = (
        soup.select_one("table.prints-table")
        or soup.select_one("div.card-prints table")
    )

    if not table:
        # fallback: any table with Print / USD / EUR in header
        for t in soup.select("table"):
            header = t.get_text(" ", strip=True)
            if "Print" in header and "USD" in header and "EUR" in header:
                table = t
                break

    dollar = []
    euro = []
    usd_urls = []
    eur_urls = []

    # Parse table rows
    if table:
        for row in table.select("tr"):
            if row.find("th"):
                continue

            # USD
            usd_link = row.select_one("a.card-price.usd")
            if usd_link:
                m_usd = re.search(r"([\d\.\,]+)", usd_link.get_text())
                usd_price = float(m_usd.group(1).replace(",", "")) if m_usd else 0
                dollar.append(usd_price)
                usd_urls.append(usd_link["href"])
            else:
                dollar.append(0)
                usd_urls.append(None)

            # EUR
            eur_link = row.select_one("a.card-price.eur")
            if eur_link:
                m_eur = re.search(r"([\d\.\,]+)", eur_link.get_text())
                eur_price = float(m_eur.group(1).replace(",", "")) if m_eur else 0
                euro.append(eur_price)
                eur_urls.append(eur_link["href"])
            else:
                euro.append(0)
                eur_urls.append(None)

    # Determine version index
    if "?" in url:
        version_idx = int(url.split("?v=")[1])
    else:
        version_idx = 0

    # Safety checks
    if version_idx >= len(dollar):
        version_idx = 0

    return {
        "usd_price": dollar[version_idx],
        "usd_url": usd_urls[version_idx],
        "eur_price": euro[version_idx],
        "eur_url": eur_urls[version_idx],
    }
@app.get("/price/{card_id}")
def get_price(card_id: str):
    now = datetime.utcnow()

    # Check cache
    cached = PRICE_CACHE.get(card_id)
    if cached:
        if now - cached["timestamp"] < CACHE_TTL:
            return {"card_id": card_id, "prices": cached["data"], "cached": True}

    # Not cached or expired → scrape
    prices = scrape_prices(card_id)

    # Save to cache
    PRICE_CACHE[card_id] = {
        "timestamp": now,
        "data": prices
    }
    save_cache_to_disk()

    return {"card_id": card_id, "prices": prices, "cached": False}

@app.get("/history/{card_id}")
def get_history(card_id: str):
    try:
        conn = sqlite3.connect("history.db")
        cursor = conn.cursor()

        cursor.execute(
            "SELECT date, eur_price, usd_price FROM card_history WHERE card_id = ? ORDER BY date",
            (card_id,)
        )

        rows = cursor.fetchall()
        conn.close()

        return [
            {"date": d, "eur": eur, "usd": usd}
            for (d, eur, usd) in rows
        ]

    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    uvicorn.run("price_api:app", host="0.0.0.0", port=8000, reload=True)

