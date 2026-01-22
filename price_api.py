# price_api.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import json
import os
import re
import sqlite3
import threading
import time
from collections import OrderedDict
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup

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
# HTTP SESSION (connection pooling = less CPU/RAM churn)
# ------------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "AllBluePriceAPI/1.0 (+https://example.invalid)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
)

# ------------------------------------------------------
# SMALL, BOUNDED TTL + LRU CACHE (prevents Render OOM)
# ------------------------------------------------------
class TTLCacheLRU:
    """
    A tiny dependency-free TTL+LRU cache.
    - maxsize bounds memory
    - ttl_seconds bounds staleness
    Thread-safe.
    """

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
                # expired
                self._data.pop(key, None)
                return None
            # mark as recently used
            self._data.move_to_end(key, last=True)
            return val

    def set(self, key: str, value: Any) -> None:
        now = time.time()
        with self._lock:
            self._data[key] = (now, value)
            self._data.move_to_end(key, last=True)

            # evict expired first (cheap sweep)
            self._evict_expired_locked(now)

            # then enforce maxsize
            while len(self._data) > self.maxsize:
                self._data.popitem(last=False)

    def _evict_expired_locked(self, now: float) -> None:
        # Because OrderedDict is in access order, we can pop from the front
        # until we see a non-expired item.
        while self._data:
            k, (ts, _) = next(iter(self._data.items()))
            if now - ts <= self.ttl:
                break
            self._data.pop(k, None)

    def stats(self) -> Dict[str, int]:
        with self._lock:
            return {"size": len(self._data), "maxsize": self.maxsize}


PRICE_CACHE = TTLCacheLRU(maxsize=600, ttl_seconds=24 * 3600)
FILE_JSON_CACHE = TTLCacheLRU(maxsize=12, ttl_seconds=10 * 60)  # small + short TTL

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


def file_mtime_iso(path: str) -> Optional[str]:
    try:
        return datetime.utcfromtimestamp(os.path.getmtime(path)).isoformat()
    except Exception:
        return None


def load_json_file_cached(path: str) -> Any:
    """
    Loads JSON with a small TTL cache, also invalidated when mtime changes.
    Prevents repeated json.load() memory churn under traffic.
    """
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
    """
    Supports BOTH formats:
    A) dict: {"don::NAME": {"usd": 1.2, "eur": 0.9, "image_url": "..."}}
    B) list: [{"name": "...", "price_usd": 1.2, "price_eur": 0.9, ...}]
    Returns list of:
      {name, usd_price, eur_price, image_url, source}
    """
    raw = load_json_file_cached(path)
    if raw is None:
        return []

    items = []

    if isinstance(raw, dict):
        for k, v in raw.items():
            if not isinstance(v, dict):
                continue
            name = k
            if isinstance(k, str) and k.startswith(prefix + "::"):
                name = k.split("::", 1)[1]

            usd = v.get("usd", v.get("usd_price", 0)) or 0
            eur = v.get("eur", v.get("eur_price", 0)) or 0

            items.append(
                {
                    "name": name,
                    "usd_price": float(usd) if usd is not None else 0.0,
                    "eur_price": float(eur) if eur is not None else 0.0,
                    "image_url": v.get("image_url"),
                    "source": v.get("source", "collectr"),
                }
            )
        return items

    if isinstance(raw, list):
        for it in raw:
            if not isinstance(it, dict):
                continue
            name = it.get("name")
            if not name:
                continue

            usd = it.get("price_usd", it.get("usd_price", it.get("usd", 0))) or 0
            eur = it.get("price_eur", it.get("eur_price", it.get("eur", 0))) or 0

            items.append(
                {
                    "name": name,
                    "usd_price": float(usd) if usd is not None else 0.0,
                    "eur_price": float(eur) if eur is not None else 0.0,
                    "image_url": it.get("image_url"),
                    "source": it.get("source", "collectr"),
                }
            )

    return items


# ------------------------------------------------------
# CARD ID NORMALIZATION
# ------------------------------------------------------
BASE_URL = "https://onepiece.limitlesstcg.com/cards/{}"

NORMAL_ID_RE = re.compile(r"^[A-Z]+[0-9]{2}-[0-9]{3}$")
PROMO_ID_RE = re.compile(r"^P-[0-9]{3}$")

# Accept encoded legacy variants, plus "v=2" inside path
EMBEDDED_V_ANY_RE = re.compile(r"(?:\?|&)?V[=](\d+)", re.IGNORECASE)
EMBEDDED_V_PATH_RE = re.compile(r"V[=](\d+)", re.IGNORECASE)


def normalize_card_and_version(card_id_raw: str, v_query: Optional[int]) -> Tuple[str, int]:
    """
    Returns (base_id, version_int).
    Accepts:
      - /price/OP01-001?v=2
      - /price/P-001?v=1
      - legacy path forms: OP01-001?v=1, OP01-001V=1, OP01-001v=1, OP01-001v=1
    """
    s = (card_id_raw or "").strip().upper()

    embedded = EMBEDDED_V_ANY_RE.search(s) or EMBEDDED_V_PATH_RE.search(s)
    embedded_v = int(embedded.group(1)) if embedded else None

    version = int(v_query) if v_query is not None else (embedded_v if embedded_v is not None else 0)
    if version < 0:
        version = 0

    # Remove any query fragments
    base = re.split(r"[?&]", s, maxsplit=1)[0]
    base = re.sub(r"V=\d+", "", base, flags=re.IGNORECASE).strip()

    if not (NORMAL_ID_RE.match(base) or PROMO_ID_RE.match(base)):
        raise ValueError(f"Invalid card_id format: {card_id_raw}")

    return base, version


def normalize_history_id(card_id_raw: str) -> str:
    """
    Your DB IDs look like:
      OP09-118v=2   (from your logs)
    while some callers may send:
      OP09-118?v=2
      OP09-118V=2
      OP09-118v=2
    We normalize to:
      base            if version == 0
      base + "v=K"    if version > 0
    """
    s = (card_id_raw or "").strip().upper()

    # Find version anywhere (including "v%3D2" decoded already as "v=2")
    m = re.search(r"V=(\d+)", s, flags=re.IGNORECASE)
    version = int(m.group(1)) if m else 0

    # Base is up to first ? or & or the "V=" part
    base = re.split(r"[?&]", s, maxsplit=1)[0]
    base = re.sub(r"V=\d+", "", base, flags=re.IGNORECASE).strip()

    if not (NORMAL_ID_RE.match(base) or PROMO_ID_RE.match(base)):
        # For history endpoints, don't hard-fail; just best-effort sanitize
        base = base.replace(" ", "").replace("/", "").strip()

    if version > 0:
        return f"{base}v={version}"
    return base


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
        return {"usd_price": 0.0, "eur_price": 0.0, "usd_url": None, "eur_url": None}

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

    if not usd_prices or version >= len(usd_prices):
        version = 0

    return {
        "usd_price": float(usd_prices[version] or 0.0),
        "eur_price": float(eur_prices[version] or 0.0),
        "usd_url": usd_urls[version],
        "eur_url": eur_urls[version],
    }


# ------------------------------------------------------
# PRICE ENDPOINT
# ------------------------------------------------------
@app.get("/price/{card_id}")
def get_price(card_id: str, v: Optional[int] = None):
    """
    Preferred:
      /price/OP01-001?v=1
      /price/P-001?v=2

    Still supports legacy callers that pass encoded '?v=1' inside the path.
    """
    try:
        base, version = normalize_card_and_version(card_id, v)
    except ValueError as e:
        return {"card_id": card_id, "error": str(e)}

    cache_key = f"{base}?v={version}" if version > 0 else base

    cached_val = PRICE_CACHE.get(cache_key)
    if cached_val is not None:
        return {"card_id": cache_key, "prices": cached_val, "cached": True, "cache": PRICE_CACHE.stats()}

    prices = scrape_prices(base, version)
    PRICE_CACHE.set(cache_key, prices)

    return {"card_id": cache_key, "prices": prices, "cached": False, "cache": PRICE_CACHE.stats()}


# ------------------------------------------------------
# COLLECTR PRICE ENDPOINTS
# ------------------------------------------------------
@app.get("/prices/dons")
def get_dons_prices():
    items = load_collectr_items(DONS_FILE, "don")
    return {
        "type": "don",
        "count": len(items),
        "cached": True,
        "updated_at": file_mtime_iso(DONS_FILE),
        "items": items,
    }


@app.get("/prices/sealed")
def get_sealed_prices():
    items = load_collectr_items(SEALED_FILE, "sealed")
    return {
        "type": "sealed",
        "count": len(items),
        "cached": True,
        "updated_at": file_mtime_iso(SEALED_FILE),
        "items": items,
    }


# ------------------------------------------------------
# SQLITE HELPERS
# ------------------------------------------------------
def db_connect() -> sqlite3.Connection:
    # keep it simple + predictable; new connection per request (closed promptly)
    conn = sqlite3.connect(DB_PATH)
    return conn


def fetch_history(table: str, key_col: str, key_val: str, limit: int) -> list:
    """
    Efficient pattern:
      - Pull the most recent `limit` rows (DESC + LIMIT)
      - Reverse in Python to return ASC chronological
    This avoids 'oldest N rows' mistakes and is cheaper on SQLite.
    """
    limit = int(limit) if limit is not None else 365
    if limit <= 0:
        limit = 365
    if limit > 2000:
        limit = 2000  # safety cap

    q = f"""
        SELECT date, eur_price, usd_price
        FROM {table}
        WHERE {key_col} = ?
        ORDER BY date DESC
        LIMIT ?
    """

    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(q, (key_val, limit))
        rows = cur.fetchall()

    rows.reverse()
    return rows


# ------------------------------------------------------
# HISTORY — CARDS
# ------------------------------------------------------
@app.get("/history/{card_id}")
def get_history(card_id: str, limit: int = 365):
    cid = normalize_history_id(card_id)
    rows = fetch_history("card_history", "card_id", cid, limit)

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
    rows = fetch_history("don_history", "name", name, limit)

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
    rows = fetch_history("sealed_history", "name", name, limit)

    return {
        "type": "sealed",
        "name": name,
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

    data = load_json_file_cached(DECKS_PATH)
    if data is None:
        return {"error": "Deck file not found"}
    return data


# ------------------------------------------------------
# MAIN (for local dev only — DO NOT run reload=True on Render)
# ------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("price_api:app", host="0.0.0.0", port=8000, reload=False)
