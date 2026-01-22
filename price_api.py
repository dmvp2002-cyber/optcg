import os
import re
import json
import sqlite3
import threading
import time
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ------------------------------------------------------
# PATHS
# ------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "history.db")
DECKS_PATH = os.path.join(BASE_DIR, "all_decks_by_region_and_set.json")

DATA_DIR = os.path.join(BASE_DIR, "data")
DONS_FILE = os.path.join(DATA_DIR, "dons_collectr.json")
SEALED_FILE = os.path.join(DATA_DIR, "sealed_collectr.json")

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
# HTTP SESSION
# ------------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "AllBluePriceAPI/1.0",
        "Accept": "text/html,application/xhtml+xml",
    }
)

# ------------------------------------------------------
# SMALL TTL + LRU CACHE (Render-safe)
# ------------------------------------------------------
class TTLCacheLRU:
    def __init__(self, maxsize: int = 600, ttl_seconds: int = 24 * 3600):
        self.maxsize = maxsize
        self.ttl = ttl_seconds
        self._lock = threading.Lock()
        self._data: OrderedDict[str, Tuple[float, Any]] = OrderedDict()

    def get(self, key: str) -> Optional[Any]:
        now = time.time()
        with self._lock:
            item = self._data.get(key)
            if not item:
                return None
            ts, val = item
            if now - ts > self.ttl:
                self._data.pop(key, None)
                return None
            self._data.move_to_end(key)
            return val

    def set(self, key: str, value: Any):
        now = time.time()
        with self._lock:
            self._data[key] = (now, value)
            self._data.move_to_end(key)
            while len(self._data) > self.maxsize:
                self._data.popitem(last=False)

PRICE_CACHE = TTLCacheLRU()

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
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


# ------------------------------------------------------
# CARD ID NORMALIZATION
# ------------------------------------------------------
BASE_URL = "https://onepiece.limitlesstcg.com/cards/{}"
NORMAL_ID_RE = re.compile(r"^[A-Z]+[0-9]{2}-[0-9]{3}$")
PROMO_ID_RE = re.compile(r"^P-[0-9]{3}$")
EMBEDDED_V_RE = re.compile(r"V=(\d+)", re.IGNORECASE)

def normalize_card_and_version(card_id_raw: str, v_query: Optional[int]) -> Tuple[str, int]:
    s = (card_id_raw or "").strip().upper()
    embedded = EMBEDDED_V_RE.search(s)
    embedded_v = int(embedded.group(1)) if embedded else None
    version = v_query if v_query is not None else (embedded_v or 0)

    base = re.split(r"[?&]", s, maxsplit=1)[0]
    base = re.sub(r"V=\d+", "", base, flags=re.IGNORECASE).strip()

    if not (NORMAL_ID_RE.match(base) or PROMO_ID_RE.match(base)):
        raise ValueError(f"Invalid card_id format: {card_id_raw}")

    return base, max(0, version)

def normalize_history_id(card_id_raw: str) -> str:
    s = card_id_raw.upper()
    m = EMBEDDED_V_RE.search(s)
    version = int(m.group(1)) if m else 0
    base = re.sub(r"V=\d+", "", s, flags=re.IGNORECASE)
    base = re.split(r"[?&]", base, maxsplit=1)[0].strip()
    return f"{base}v={version}" if version > 0 else base


# ------------------------------------------------------
# LIMITLESS SCRAPER
# ------------------------------------------------------
def scrape_prices(base_id: str, version: int) -> Dict[str, Any]:
    formatted = f"{base_id}?v={version}" if version > 0 else base_id
    url = BASE_URL.format(formatted)

    r = SESSION.get(url, timeout=(5, 20))
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    table = (
        soup.select_one("table.prints-table")
        or soup.select_one("div.card-prints table")
        or soup.select_one("table")
    )

    if not table:
        return {"eur": 0.0, "usd": 0.0}

    eur_prices, usd_prices = [], []

    for row in table.select("tr"):
        if row.find("th"):
            continue
        eur = row.select_one("a.card-price.eur")
        usd = row.select_one("a.card-price.usd")
        eur_prices.append(parse_price(eur.text) if eur else 0.0)
        usd_prices.append(parse_price(usd.text) if usd else 0.0)

    if version >= len(eur_prices):
        version = 0

    return {
        "eur": eur_prices[version],
        "usd": usd_prices[version],
    }


# ------------------------------------------------------
# SQLITE HELPERS (SAFE)
# ------------------------------------------------------
def db_connect():
    return sqlite3.connect(DB_PATH)

def fetch_price_at_or_before(card_id: str, days_ago: int):
    cutoff = (datetime.utcnow() - timedelta(days=days_ago)).isoformat()
    q = """
        SELECT eur_price, usd_price
        FROM card_history
        WHERE card_id = ?
          AND date <= ?
        ORDER BY date DESC
        LIMIT 1
    """
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(q, (card_id, cutoff))
        row = cur.fetchone()
    return row


# ------------------------------------------------------
# PRICE ENDPOINT (WITH pct_7d)
# ------------------------------------------------------
@app.get("/price/{card_id}")
def get_price(card_id: str, v: Optional[int] = None):
    try:
        base, version = normalize_card_and_version(card_id, v)
    except ValueError as e:
        return {"card_id": card_id, "error": str(e)}

    cache_key = f"{base}?v={version}" if version > 0 else base
    cached = PRICE_CACHE.get(cache_key)
    if cached:
        return cached

    prices = scrape_prices(base, version)

    hist_id = normalize_history_id(cache_key)
    week_ago = fetch_price_at_or_before(hist_id, 7)

    pct_7d = pct_7d_usd = 0.0
    if week_ago:
        old_eur, old_usd = week_ago
        if old_eur and prices["eur"]:
            pct_7d = ((prices["eur"] - old_eur) / old_eur) * 100
        if old_usd and prices["usd"]:
            pct_7d_usd = ((prices["usd"] - old_usd) / old_usd) * 100

    response = {
        "card_id": cache_key,
        "prices": {
            "eur": prices["eur"],
            "usd": prices["usd"],
            "pct_7d": round(pct_7d, 2),
            "pct_7d_usd": round(pct_7d_usd, 2),
        },
        "cached": False,
    }

    PRICE_CACHE.set(cache_key, response)
    return response


# ------------------------------------------------------
# HISTORY ENDPOINTS (UNCHANGED)
# ------------------------------------------------------
@app.get("/history/{card_id}")
def get_history(card_id: str, limit: int = 365):
    cid = normalize_history_id(card_id)
    q = """
        SELECT date, eur_price, usd_price
        FROM card_history
        WHERE card_id = ?
        ORDER BY date DESC
        LIMIT ?
    """
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(q, (cid, min(limit, 2000)))
        rows = cur.fetchall()

    rows.reverse()
    return {
        "type": "card",
        "id": cid,
        "count": len(rows),
        "history": [{"date": d, "eur": eur, "usd": usd} for d, eur, usd in rows],
    }


# ------------------------------------------------------
# MAIN
# ------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("price_api:app", host="0.0.0.0", port=8000, reload=False)
