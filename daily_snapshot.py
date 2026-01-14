import os
import re
import json
import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# ------------------------------------------------------
# PATHS
# ------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "history.db")
ALL_CARDS_PATH = os.path.join(BASE_DIR, "all_cards.json")

DATA_DIR = os.path.join(BASE_DIR, "data")
DONS_FILE = os.path.join(DATA_DIR, "dons_collectr.json")
SEALED_FILE = os.path.join(DATA_DIR, "sealed_collectr.json")

# ------------------------------------------------------
# CONSTANTS
# ------------------------------------------------------
LIMITLESS_BASE = "https://onepiece.limitlesstcg.com/cards/{}"
TODAY = datetime.utcnow().date().isoformat()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; OPTCGSnapshot/1.0)"
}

# ------------------------------------------------------
# DB SETUP
# ------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS card_history (
        card_id TEXT,
        date TEXT,
        eur_price REAL,
        usd_price REAL,
        UNIQUE(card_id, date)
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS sealed_history (
        name TEXT,
        date TEXT,
        eur_price REAL,
        usd_price REAL,
        UNIQUE(name, date)
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS don_history (
        name TEXT,
        date TEXT,
        eur_price REAL,
        usd_price REAL,
        UNIQUE(name, date)
    )
    """)

    conn.commit()
    conn.close()

# ------------------------------------------------------
# HELPERS
# ------------------------------------------------------
def normalize_name(name: str) -> str:
    """
    Normalize Collectr keys:
    - sealed::NAME → NAME
    - don::NAME → NAME
    """
    if not name:
        return name
    if "::" in name:
        return name.split("::", 1)[1].strip()
    return name.strip()


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


def load_all_base_codes():
    with open(ALL_CARDS_PATH, "r", encoding="utf-8") as f:
        cards = json.load(f)

    base_codes = set()
    for c in cards:
        code = c.get("code")
        if code and re.match(r"[A-Z]{2,4}\d{2}-\d{3}", code):
            base_codes.add(code)

    print(f"✔ Loaded {len(base_codes)} base card codes")
    return sorted(base_codes)


def extract_versions(card_code: str):
    url = LIMITLESS_BASE.format(card_code)
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    table = (
        soup.select_one("table.prints-table")
        or soup.select_one("div.card-prints table")
        or soup.select_one("table")
    )

    if not table:
        return []

    versions = []
    idx = 0
    for row in table.select("tr"):
        if row.find("th"):
            continue
        versions.append(card_code if idx == 0 else f"{card_code}v={idx}")
        idx += 1

    return versions


def scrape_card_price(card_id: str):
    try:
        base = re.match(r"([A-Z]+[0-9]{2}-[0-9]{3})", card_id).group(1)
        m = re.search(r"v=(\d+)", card_id)
        version = int(m.group(1)) if m else 0

        formatted = f"{base}?v={version}" if version > 0 else base
        url = LIMITLESS_BASE.format(formatted)

        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        table = (
            soup.select_one("table.prints-table")
            or soup.select_one("div.card-prints table")
            or soup.select_one("table")
        )

        if not table:
            return 0.0, 0.0

        usd, eur = [], []

        for row in table.select("tr"):
            if row.find("th"):
                continue

            usd_link = row.select_one("a.card-price.usd")
            eur_link = row.select_one("a.card-price.eur")

            usd.append(parse_price(usd_link.text) if usd_link else 0.0)
            eur.append(parse_price(eur_link.text) if eur_link else 0.0)

        if version >= len(usd):
            version = 0

        return eur[version], usd[version]

    except Exception as e:
        print(f"⚠ Price fetch failed for {card_id}: {e}")
        return 0.0, 0.0


# ------------------------------------------------------
# COLLECTR — LOAD STATIC SNAPSHOT (PATCHED)
# ------------------------------------------------------
def load_collectr_snapshot(path):
    if not os.path.exists(path):
        print(f"⚠ Missing Collectr snapshot: {path}")
        return []

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []

    # Case 1: dict { "sealed::NAME" → {...} }
    if isinstance(data, dict):
        for raw_name, prices in data.items():
            name = normalize_name(raw_name)

            if isinstance(prices, dict):
                eur = prices.get("price_eur") or prices.get("eur_price") or prices.get("eur") or 0
                usd = prices.get("price_usd") or prices.get("usd_price") or prices.get("usd") or 0
            else:
                eur = usd = 0

            rows.append((name, float(eur), float(usd)))

        return rows

    # Case 2: list of dicts
    for it in data:
        if isinstance(it, dict):
            raw_name = it.get("name")
            name = normalize_name(raw_name)

            eur = it.get("price_eur") or it.get("eur_price") or it.get("eur") or 0
            usd = it.get("price_usd") or it.get("usd_price") or it.get("usd") or 0

        else:
            continue

        if name:
            rows.append((name, float(eur), float(usd)))

    return rows


# ------------------------------------------------------
# SNAPSHOT
# ------------------------------------------------------
def main():
    init_db()
    card_codes = load_all_base_codes()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # ---------------- CARDS ----------------
    for code in card_codes:
        try:
            versions = extract_versions(code)
            for cid in versions:
                eur, usd = scrape_card_price(cid)
                if eur == 0 and usd == 0:
                    continue

                cursor.execute(
                    """
                    INSERT OR IGNORE INTO card_history
                    (card_id, date, eur_price, usd_price)
                    VALUES (?, ?, ?, ?)
                    """,
                    (cid, TODAY, eur, usd),
                )
        except Exception as e:
            print(f"⚠ Failed card {code}: {e}")

    # ---------------- SEALED ----------------
    for name, eur, usd in load_collectr_snapshot(SEALED_FILE):
        cursor.execute(
            "INSERT OR IGNORE INTO sealed_history VALUES (?, ?, ?, ?)",
            (name, TODAY, eur, usd),
        )

    # ---------------- DON ----------------
    for name, eur, usd in load_collectr_snapshot(DONS_FILE):
        cursor.execute(
            "INSERT OR IGNORE INTO don_history VALUES (?, ?, ?, ?)",
            (name, TODAY, eur, usd),
        )

    conn.commit()
    conn.close()

    print("✅ FULL daily snapshot completed:", TODAY)


if __name__ == "__main__":
    main()
