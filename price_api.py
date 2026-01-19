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

DATA_DIR = os.path.join(BASE_DIR, "data")
DONS_FILE = os.path.join(DATA_DIR, "dons_collectr.json")
SEALED_FILE = os.path.join(DATA_DIR, "sealed_collectr.json")

# ------------------------------------------------------
# CARD PRICE CACHE
# ------------------------------------------------------
PRICE_CACHE = {}
CACHE_TTL = timedelta(hours=24)

if os.path.exists(CACHE_FILE):
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
            for key, val in raw.items():
                PRICE_CACHE[key] = {
                    "timestamp": datetime.fromisoformat(val["timestamp"]),
                    "data": val["data"],
                }
        print("Loaded card price cache:", len(PRICE_CACHE))
    except Exception as e:
        print("Failed to load card cache:", e)


def save_cache_to_disk():
    try:
        serializable = {
            k: {
                "timestamp": v["timestamp"].isoformat(),
                "data": v["data"],
            }
            for k, v in PRICE_CACHE.items()
        }
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(serializable, f)
    except Exception as e:
        print("Failed to save cache:", e)

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
# HELPERS
# ------------------------------------------------------
def parse_price(text: str) -> float:
    if not text:
        return 0.0

    m = re.search(r"([\d\.,]+)", text.replace("\u00a0", " ").strip())
    if not m:
        return 0.0

    s = m.group(1)

    if "," in s and "." in s:
        s = s.replace(",", "")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")

    try:
        return float(s)
    except Exception:
        return 0.0


def file_mtime_iso(path: str):
    try:
        return datetime.utcfromtimestamp(os.path.getmtime(path)).isoformat()
    except Exception:
        return None

# ------------------------------------------------------
# LIMITLESS SCRAPER (CARDS + PROMOS)
# ------------------------------------------------------
BASE_URL = "https://onepiece.limitlesstcg.com/cards/{}"

CARD_ID_REGEX = re.compile(
    r"^(?:"
    r"(?:[A-Z]+[0-9]{2}-[0-9]{3})"  # normal cards
    r"|"
    r"(?:P-[0-9]{3})"              # promo cards
    r")$"
)


def scrape_prices(card_id: str):
    card_id = card_id.upper().replace("?", "").strip()

    # Extract base ID
    base_match = re.match(r"([A-Z0-9\-]+)", card_id)
    if not base_match:
        raise ValueError(f"Invalid card_id format: {card_id}")

    base = base_match.group(1)

    if not CARD_ID_REGEX.match(base):
        raise ValueError(f"Invalid card_id format: {card_id}")

    # Extract version (v=0 default)
    m_version = re.search(r"V=(\d+)", card_id)
    version = int(m_version.group(1)) if m_version else 0

    formatted = f"{base}?v={version}" if version > 0 else base
    url = BASE_URL.format(formatted)

    r = requests.get(url, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    table = (
        soup.select_one("table.prints-table")
        or soup.select_one("div.card-prints table")
        or soup.select_one("table")
    )

    if not table:
        return {
            "usd_price": 0.0,
            "eur_price": 0.0,
            "usd_url": None,
            "eur_url": None,
        }

    usd_prices, eur_prices, usd_urls, eur_urls = [], [], [], []

    for row in table.select("tr"):
        if row.find("th"):
            continue

        usd_link = row.select_one("a.card-price.usd")
        eur_link = row.select_one("a.card-price.eur")

        usd_prices.append(parse_price(usd_link.text) if usd_link else 0.0)
        eur_prices.append(parse_price(eur_link.text) if eur_link else 0.0)
        usd_urls.append(usd_link.get("href") if usd_link else None)
        eur_urls.append(eur_link.get("href") if eur_link else None)

    if version >= len(usd_prices):
        version = 0

    return {
        "usd_price": usd_prices[version],
        "eur_price": eur_prices[version],
        "usd_url": usd_urls[version],
        "eur_url": eur_urls[version],
    }

# ------------------------------------------------------
# PRICE ENDPOINT
# ------------------------------------------------------
@app.get("/price/{card_id}")
def get_price(card_id: str):
    now = datetime.utcnow()
    cid = card_id.upper().strip()

    cached = PRICE_CACHE.get(cid)
    if cached and now - cached["timestamp"] < CACHE_TTL:
        return {"card_id": cid, "prices": cached["data"], "cached": True}

    prices = scrape_prices(cid)
    PRICE_CACHE[cid] = {"timestamp": now, "data": prices}
    save_cache_to_disk()

    return {"card_id": cid, "prices": prices, "cached": False}

# ------------------------------------------------------
# COLLECTR PRICE ENDPOINTS
# ------------------------------------------------------
@app.get("/prices/dons")
def get_dons_prices():
    with open(DONS_FILE, "r", encoding="utf-8") as f:
        items = json.load(f)
    return {
        "type": "don",
        "count": len(items),
        "cached": True,
        "updated_at": file_mtime_iso(DONS_FILE),
        "items": items,
    }


@app.get("/prices/sealed")
def get_sealed_prices():
    with open(SEALED_FILE, "r", encoding="utf-8") as f:
        items = json.load(f)
    return {
        "type": "sealed",
        "count": len(items),
        "cached": True,
        "updated_at": file_mtime_iso(SEALED_FILE),
        "items": items,
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
# DECKS
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
