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
    card_id = card_id.upper().replace("?", "")

    # Extract base ID and version
    if "V=" in card_id:
        base, version_str = card_id.split("V=")
        base = base[:8]
        try:
            version = int(version_str)
        except:
            version = 0
    else:
        base = card_id[:8]
        version = 0

    # Format ID for URL
    if version > 0:
        formatted = f"{base}?v={version}"
    else:
        formatted = base

    url = BASE_URL.format(formatted)
    r = requests.get(url)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    # -----------------------------
    # UNIVERSAL PRINT TABLE FINDER
    # Works for ALL Limitless formats
    # -----------------------------
    table = (
        soup.select_one("table.prints-table") or
        soup.select_one("div.card-prints table") or
        soup.select_one("div.price-table table") or
        soup.select_one("table")  # fallback (will filter)
    )

    if not table:
        print("NO TABLE FOUND for", card_id)
        return {"usd_price": 0, "eur_price": 0}

    dollar = []
    euro = []

    for row in table.select("tr"):
        if row.find("th"):
            continue

        # USD cell
        usd_link = row.select_one("a.card-price.usd")
        if usd_link:
            m = re.search(r"([\d\.,]+)", usd_link.text)
            usd = float(m.group(1).replace(",", "")) if m else 0
        else:
            usd = 0
        dollar.append(usd)

        # EUR cell
        eur_link = row.select_one("a.card-price.eur")
        if eur_link:
            m2 = re.search(r"([\d\.,]+)", eur_link.text)
            eur = float(m2.group(1).replace(",", "")) if m2 else 0
        else:
            eur = 0
        euro.append(eur)

    # Avoid out of range version index
    if version >= len(euro):
        version = 0

    return {
        "usd_price": dollar[version],
        "eur_price": euro[version]
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

if __name__ == "__main__":
    uvicorn.run("price_api:app", host="0.0.0.0", port=8000, reload=True)
