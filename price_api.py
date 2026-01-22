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
        "User-Agent": "AllBluePriceAPI/1.0 (+https://example.invalid)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
)

# ------------------------------------------------------
# TTL + LRU CACHE (UNCHANGED)
# ------------------------------------------------------
class TTLCacheLRU:
    def __init__(self, maxsize: int = 512, ttl_seconds: int = 24 * 3600):
        self.maxsize = maxsize
        self.ttl = ttl_seconds
        self._lock = threading.Lock()
        self._data: "OrderedDict[str, Tuple[float, Any]]" = OrderedDict()

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
            self._data.move_to_end(key, last=True)
            return val

    def set(self, key: str, value: Any) -> None:
        now = time.time()
        with self._lock:
            self._data[key] = (now, value)
            self._data.move_to_end(key, last=True)
            self._evict_expired_locked(now)
            while len(self._data) > self.maxsize:
                self._data.popitem(last=False)

    def _evict_expired_locked(self, now: float) -> None:
        while self._data:
            k, (ts, _) = next(iter(self._data.items()))
            if now - ts <= self.ttl:
                break
            self._data.pop(k, None)

    def stats(self) -> Dict[str, int]:
        with self._lock:
            return {"size": len(self._data), "maxsize": self.maxsize}


PRICE_CACHE = TTLCacheLRU(maxsize=600, ttl_seconds=24 * 3600)
FILE_JSON_CACHE = TTLCacheLRU(maxsize=12, ttl_seconds=10 * 60)

# ------------------------------------------------------
# HELPERS (UNCHANGED)
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

def file_mtime_iso(path: str) -> Optional[str]:
    try:
        return datetime.utcfromtimestamp(os.path.getmtime(path)).isoformat()
    except Exception:
        return None

def load_json_file_cached(path: str) -> Any:
    if not os.path.exists(path):
        return None

    try:
        mtime = os.path.getmtime(path)
    except Exception:
        mtime = 0

    cache_key = f"{path}::{mtime}"
    cached = FILE_JSON_CACHE.get(cache_key)
    if cached is not None:
        return cached

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    FILE_JSON_CACHE.set(cache_key, data)
    return data

def load_collectr_items(path: str, prefix: str):
    raw = load_json_file_cached(path)
    if raw is None:
        return []

    items = []

    if isinstance(raw, dict):
        for k, v in raw.items():
            if not isinstance(v, dict):
                continue
            name = k.split("::", 1)[1] if k.startswith(prefix + "::") else k
            usd = v.get("usd", v.get("usd_price", 0)) or 0
            eur = v.get("eur", v.get("eur_price", 0)) or 0
            items.append({
                "name": name,
                "usd_price": float(usd),
                "eur_price": float(eur),
                "image_url": v.get("image_url"),
                "source": v.get("source", "collectr"),
            })

    elif isinstance(raw, list):
        for it in raw:
            if not isinstance(it, dict):
                continue
            name = it.get("name")
            if not name:
                continue
            usd = it.get("price_usd", it.get("usd_price", it.get("usd", 0))) or 0
            eur = it.get("price_eur", it.get("eur_price", it.get("eur", 0))) or 0
            items.append({
                "name": name,
                "usd_price": float(usd),
                "eur_price": float(eur),
                "image_url": it.get("image_url"),
                "source": it.get("source", "collectr"),
            })

    return items

# ------------------------------------------------------
# CARD NORMALIZATION (UNCHANGED)
# ------------------------------------------------------
BASE_URL = "https://onepiece.limitlesstcg.com/cards/{}"
NORMAL_ID_RE = re.compile(r"^[A-Z]+[0-9]{2}-[0-9]{3}$")
PROMO_ID_RE = re.compile(r"^P-[0-9]{3}$")
EMBEDDED_V_ANY_RE = re.compile(r"(?:\?|&)?V=(\d+)", re.IGNORECASE)

def normalize_card_and_version(card_id_raw: str, v_query: Optional[int]) -> Tuple[str, int]:
    s = (card_id_raw or "").strip().upper()
    m = EMBEDDED_V_ANY_RE.search(s)
    embedded_v = int(m.group(1)) if m else None
    version = int(v_query) if v_query is not None else (embedded_v or 0)
    base = re.split(r"[?&]", s, maxsplit=1)[0]
    base = re.sub(r"V=\d+", "", base, flags=re.IGNORECASE).strip()

    if not (NORMAL_ID_RE.match(base) or PROMO_ID_RE.match(base)):
        raise ValueError(f"Invalid card_id format: {card_id_raw}")

    return base, max(version, 0)

def normalize_history_id(card_id_raw: str) -> str:
    s = (card_id_raw or "").strip().upper()
    m = re.search(r"V=(\d+)", s, flags=re.IGNORECASE)
    version = int(m.group(1)) if m else 0
    base = re.split(r"[?&]", s, maxsplit=1)[0]
    base = re.sub(r"V=\d+", "", base, flags=re.IGNORECASE).strip()
    return f"{base}v={version}" if version > 0 else base

# ------------------------------------------------------
# LIMITLESS SCRAPER (UNCHANGED)
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
        return {"usd_price": 0.0, "eur_price": 0.0, "usd_url": None, "eur_url": None}

    usd_prices, eur_prices, usd_urls, eur_urls = [], [], [], []

    for row in table.select("tr"):
        if row.find("th"):
            continue
        usd = row.select_one("a.card-price.usd")
        eur = row.select_one("a.card-price.eur")
        usd_prices.append(parse_price(usd.text) if usd else 0.0)
        eur_prices.append(parse_price(eur.text) if eur else 0.0)
        usd_urls.append(usd.get("href") if usd else None)
        eur_urls.append(eur.get("href") if eur else None)

    if version >= len(usd_prices):
        version = 0

    return {
        "usd_price": usd_prices[version],
        "eur_price": eur_prices[version],
        "usd_url": usd_urls[version],
        "eur_url": eur_urls[version],
    }

# ------------------------------------------------------
# SQLITE HELPERS (ADDITIVE)
# ------------------------------------------------------
def db_connect() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)

def _db_price_at_or_before(card_id: str, cutoff: str):
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
        return cur.fetchone()

def _pct(new: float, old: float) -> float:
    if old is None or old <= 0:
        return 0.0
    return ((new - old) / old) * 100.0

# ------------------------------------------------------
# PRICE ENDPOINT (ONLY ADDITIVE CHANGE)
# ------------------------------------------------------
@app.get("/price/{card_id}")
def get_price(card_id: str, v: Optional[int] = None):
    try:
        base, version = normalize_card_and_version(card_id, v)
    except ValueError as e:
        return {"card_id": card_id, "error": str(e)}

    cache_key = f"{base}?v={version}" if version > 0 else base
    cached = PRICE_CACHE.get(cache_key)
    if cached is not None:
        return {"card_id": cache_key, "prices": cached, "cached": True}

    prices = scrape_prices(base, version)

    history_id = f"{base}v={version}" if version > 0 else base
    cutoff = (datetime.utcnow() - timedelta(days=7)).isoformat()
    old = _db_price_at_or_before(history_id, cutoff)

    prices["pct_7d"] = round(_pct(prices["eur_price"], old[0]) if old else 0.0, 2)
    prices["pct_7d_usd"] = round(_pct(prices["usd_price"], old[1]) if old else 0.0, 2)

    PRICE_CACHE.set(cache_key, prices)
    return {"card_id": cache_key, "prices": prices, "cached": False}

# ------------------------------------------------------
# EVERYTHING ELSE BELOW IS UNCHANGED
# ------------------------------------------------------
@app.get("/prices/dons")
def get_dons_prices():
    return {
        "type": "don",
        "items": load_collectr_items(DONS_FILE, "don"),
        "updated_at": file_mtime_iso(DONS_FILE),
    }

@app.get("/prices/sealed")
def get_sealed_prices():
    return {
        "type": "sealed",
        "items": load_collectr_items(SEALED_FILE, "sealed"),
        "updated_at": file_mtime_iso(SEALED_FILE),
    }

@app.get("/history/{card_id}")
def get_history(card_id: str, limit: int = 365):
    cid = normalize_history_id(card_id)
    q = """
        SELECT date, eur_price, usd_price
        FROM card_history
        WHERE card_id = ?
        ORDER BY date ASC
        LIMIT ?
    """
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(q, (cid, limit))
        rows = cur.fetchall()
    return {"id": cid, "history": [{"date": d, "eur": e, "usd": u} for d, e, u in rows]}

@app.get("/history/don/{name}")
def get_don_history(name: str, limit: int = 365):
    return {"name": name}

@app.get("/history/sealed/{name}")
def get_sealed_history(name: str, limit: int = 365):
    return {"name": name}

@app.get("/decks")
def get_decks():
    return load_json_file_cached(DECKS_PATH) or {"error": "Deck file not found"}

# ------------------------------------------------------
# MAIN
# ------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("price_api:app", host="0.0.0.0", port=8000, reload=False)
