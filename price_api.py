from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import json
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import os
import sqlite3

# ------------------------------------------------------
# PATHS
# ------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "history.db")
DECKS_PATH = os.path.join(BASE_DIR, "all_decks_by_region_and_set.json")
CACHE_FILE = os.path.join(BASE_DIR, "price_cache.json")

# ------------------------------------------------------
# CARD PRICE CACHE (Limitless)
# ------------------------------------------------------
PRICE_CACHE = {}
CACHE_TTL = timedelta(hours=24)

if os.path.exists(CACHE_FILE):
    try:
        with open(CACHE_FILE, "r") as f:
            raw = json.load(f)
            for key, val in raw.items():
                PRICE_CACHE[key] = {
                    "timestamp": datetime.fromisoformat(val["timestamp"]),
                    "data": val["data"],
                }
        print("Loaded persistent card cache:", len(PRICE_CACHE))
    except Exception as e:
        print("Failed to load card cache:", e)


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


# ------------------------------------------------------
# DON / SEALED BULK CACHE (Collectr)
# ------------------------------------------------------
DONS_CACHE = {"timestamp": None, "data": []}
SEALED_CACHE = {"timestamp": None, "data": []}
COLLECTR_CACHE_TTL = timedelta(hours=6)

USD_TO_EUR = 0.75
COLLECTR_URL = (
    "https://app.getcollectr.com/?sortType=price&sortOrder=DESC&cardType={}&category=68"
)

# ------------------------------------------------------
# FASTAPI APP
# ------------------------------------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------
# LIMITLESS SCRAPER (CARDS)
# ------------------------------------------------------
BASE_URL = "https://onepiece.limitlesstcg.com/cards/{}"


def scrape_prices(card_id: str):
    card_id = card_id.upper().replace("?", "")

    m = re.match(r"([A-Z]+[0-9]{2}-[0-9]{3})", card_id)
    if not m:
        raise ValueError(f"Invalid card_id format: {card_id}")

    base = m.group(1)
    m2 = re.search(r"V=(\d+)", card_id)
    version = int(m2.group(1)) if m2 else 0

    formatted = f"{base}?v={version}" if version > 0 else base
    url = BASE_URL.format(formatted)

    r = requests.get(url)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    table = (
        soup.select_one("table.prints-table")
        or soup.select_one("div.card-prints table")
        or soup.select_one("table")
    )

    if not table:
        return {"usd_price": 0, "eur_price": 0}

    usd, eur = [], []

    for row in table.select("tr"):
        if row.find("th"):
            continue

        usd_link = row.select_one("a.card-price.usd")
        eur_link = row.select_one("a.card-price.eur")

        usd_val = (
            float(re.search(r"([\d\.]+)", usd_link.text).group(1))
            if usd_link and re.search(r"([\d\.]+)", usd_link.text)
            else 0
        )

        eur_val = (
            float(re.search(r"([\d\.]+)", eur_link.text).group(1))
            if eur_link and re.search(r"([\d\.]+)", eur_link.text)
            else 0
        )

        usd.append(usd_val)
        eur.append(eur_val)

    if version >= len(usd):
        version = 0

    return {
        "usd_price": usd[version],
        "eur_price": eur[version],
    }


@app.get("/price/{card_id}")
def get_price(card_id: str):
    now = datetime.utcnow()
    cached = PRICE_CACHE.get(card_id)

    if cached and now - cached["timestamp"] < CACHE_TTL:
        return {"card_id": card_id, "prices": cached["data"], "cached": True}

    prices = scrape_prices(card_id)
    PRICE_CACHE[card_id] = {"timestamp": now, "data": prices}
    save_cache_to_disk()

    return {"card_id": card_id, "prices": prices, "cached": False}


# ------------------------------------------------------
# COLLECTR SCRAPERS (DON / SEALED)
# ------------------------------------------------------
def scrape_collectr(card_type: str):
    r = requests.get(COLLECTR_URL.format(card_type))
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    items = []

    for card in soup.select("div.card-item"):
        name_el = card.select_one(".card-name")
        price_el = card.select_one(".price")

        if not name_el or not price_el:
            continue

        m = re.search(r"([\d\.]+)", price_el.text)
        usd = float(m.group(1)) if m else 0
        eur = round(usd * USD_TO_EUR, 2)

        items.append(
            {
                "name": name_el.get_text(strip=True),
                "usd_price": usd,
                "eur_price": eur,
                "source": "collectr",
            }
        )

    return items


def get_dons_cached():
    now = datetime.utcnow()
    if DONS_CACHE["timestamp"] and now - DONS_CACHE["timestamp"] < COLLECTR_CACHE_TTL:
        return DONS_CACHE["data"], True

    data = scrape_collectr("don")
    DONS_CACHE.update({"timestamp": now, "data": data})
    return data, False


def get_sealed_cached():
    now = datetime.utcnow()
    if SEALED_CACHE["timestamp"] and now - SEALED_CACHE["timestamp"] < COLLECTR_CACHE_TTL:
        return SEALED_CACHE["data"], True

    data = scrape_collectr("sealed")
    SEALED_CACHE.update({"timestamp": now, "data": data})
    return data, False


# ------------------------------------------------------
# DON / SEALED LIVE PRICE ENDPOINTS
# ------------------------------------------------------
@app.get("/prices/dons")
def get_dons_prices():
    data, cached = get_dons_cached()
    return {
        "type": "don",
        "count": len(data),
        "cached": cached,
        "updated_at": DONS_CACHE["timestamp"],
        "items": data,
    }


@app.get("/prices/sealed")
def get_sealed_prices():
    data, cached = get_sealed_cached()
    return {
        "type": "sealed",
        "count": len(data),
        "cached": cached,
        "updated_at": SEALED_CACHE["timestamp"],
        "items": data,
    }


@app.post("/refresh/collectr")
def refresh_collectr():
    now = datetime.utcnow()
    DONS_CACHE.update({"timestamp": now, "data": scrape_collectr("don")})
    SEALED_CACHE.update({"timestamp": now, "data": scrape_collectr("sealed")})
    return {
        "ok": True,
        "updated_at": now,
        "dons": len(DONS_CACHE["data"]),
        "sealed": len(SEALED_CACHE["data"]),
    }


# ------------------------------------------------------
# HISTORY — CARDS
# ------------------------------------------------------
@app.get("/history/{card_id}")
def get_history(card_id: str, limit: int = 365):
    cid = card_id.replace("?", "").strip()

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

    return {
        "type": "card",
        "id": cid,
        "count": len(rows),
        "history": [{"date": d, "eur": eur, "usd": usd} for d, eur, usd in rows],
    }


# ------------------------------------------------------
# HISTORY — DON
# ------------------------------------------------------
@app.get("/history/don/{name}")
def get_don_history(name: str, limit: int = 365):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT date, eur_price, usd_price
        FROM don_history
        WHERE name = ?
        ORDER BY date ASC
        LIMIT ?
        """,
        (name, limit),
    )

    rows = cursor.fetchall()
    conn.close()

    return {
        "type": "don",
        "name": name,
        "count": len(rows),
        "history": [{"date": d, "eur": eur, "usd": usd} for d, eur, usd in rows],
    }


# ------------------------------------------------------
# HISTORY — SEALED
# ------------------------------------------------------
@app.get("/history/sealed/{name}")
def get_sealed_history(name: str, limit: int = 365):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT date, eur_price, usd_price
        FROM sealed_history
        WHERE name = ?
        ORDER BY date ASC
        LIMIT ?
        """,
        (name, limit),
    )

    rows = cursor.fetchall()
    conn.close()

    return {
        "type": "sealed",
        "name": name,
        "count": len(rows),
        "history": [{"date": d, "eur": eur, "usd": usd} for d, eur, usd in rows],
    }


# ------------------------------------------------------
# DECKS (UNCHANGED)
# ------------------------------------------------------
@app.get("/decks")
def get_decks():
    if not os.path.exists(DECKS_PATH):
        return {"error": "Deck file not found"}

    with open(DECKS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# ------------------------------------------------------
# MAIN
# ------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run("price_api:app", host="0.0.0.0", port=8000, reload=True)
