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

# Simple cache: { card_id: { "timestamp": datetime, "data": {...} } }
PRICE_CACHE = {}
CACHE_TTL = timedelta(hours=24)


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
    card_id=str(card_id)
    if len(card_id)>8:
        card_id=card_id[:8]+"?"+card_id[8:]
        card_id=card_id.upper()
    else:
        card_id=card_id[:8]

    url = BASE_URL.format(card_id)
    r = requests.get(url)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    
    main = soup.select_one("div.card-details-main")
    if main is None:
        raise ValueError(f"card-details-main not found for {code}")
    
        # --- Prints Table ----------------------------------------------------
    prints: list[CardPrint] = []

    # Try known classes first
    table = soup.select_one("table.prints-table")

    # If not found, try the card-prints wrapper
    if not table:
        div = soup.select_one("div.card-prints")
        if div:
            table = div.select_one("table")

    # Fallback: find any table whose header contains "Print"
    if not table:
        for t in soup.select("table"):
            header = t.get_text(" ", strip=True)
            if "Print" in header and "USD" in header and "EUR" in header:
                table = t
                break

    # If still not found -> no prints for this card
    if not table:
        print("WARNING: No prints table found for", card_id)
    else:
        dollar=[]
        euro=[]
        for row in table.select("tr"):
            
            # skip header row
            if row.find("th"):
                continue

            is_current = "current" in (row.get("class") or [])

            # PRINT NAME + VERSION
            name_cell = row.select_one("td:nth-of-type(1) a")
            
            raw_name = name_cell.get_text(" ", strip=True) if name_cell else ""
            m_version = re.search(r"\b([A-Za-z0-9]{1,3})$", raw_name)
            version = m_version.group(1) if m_version else None
            name = raw_name.replace(version or "", "").strip()

            # USD
            usd_link = row.select_one("a.card-price.usd")
            usd_url = usd_link["href"] if usd_link else None
            usd_price = None
            if usd_link:
                m_usd = re.search(r"([\d\.\,]+)", usd_link.get_text())
                if m_usd:
                    usd_price = str(m_usd.group(1))
                    usd_price=usd_price.replace(",","")
                    usd_price=float(usd_price)
                    dollar.append(usd_price)
            else:
                dollar.append(0)


            # EUR
            eur_link = row.select_one("a.card-price.eur")
            eur_url = eur_link["href"] if eur_link else None
            eur_price = None
            if eur_link:
                m_eur = re.search(r"([\d\.\,]+)", eur_link.get_text())
                if m_eur:
                    eur_price = str(m_eur.group(1))
                    eur_price=eur_price.replace(",","")
                    eur_price=float(eur_price)
                    euro.append(eur_price)
            else:
                euro.append(0)
        if ("?" in url):
            card_version_dollar=int(url[-1])
        else:
            card_version_dollar=0

        if ("?" in url):
            card_version_euro=int(url[-1])
        else:
            card_version_euro=0
    # TODO: replace with your real scraping logic
    return {
        "usd_price": dollar[card_version_dollar],
        "usd_url": usd_url,
        "eur_price": euro[card_version_euro],
        "eur_url": eur_url,
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

    return {"card_id": card_id, "prices": prices, "cached": False}

if __name__ == "__main__":
    uvicorn.run("price_api:app", host="0.0.0.0", port=8000, reload=True)


