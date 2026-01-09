import os
import re
import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# ------------------------------------------------------
# PATHS
# ------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "history.db")

# ------------------------------------------------------
# CONSTANTS
# ------------------------------------------------------
LIMITLESS_BASE = "https://onepiece.limitlesstcg.com/cards/{}"
COLLECTR_URL = "https://app.getcollectr.com/?sortType=price&sortOrder=DESC&cardType={}&category=68"
USD_TO_EUR = 0.75

TODAY = datetime.utcnow().date().isoformat()

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
def normalize_card_id(card_id: str) -> str:
    """
    IMPORTANT RULE:
    - v=0 → NO SUFFIX (OP13-001)
    - v>=1 → OP13-001v=1
    """
    cid = card_id.upper().replace("?", "").strip()
    return cid


def extract_versions(card_code: str):
    """
    Returns list of card_ids to snapshot:
    ['OP13-001', 'OP13-001v=1', 'OP13-001v=2', ...]
    """
    url = LIMITLESS_BASE.format(card_code)
    r = requests.get(url)
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
    for idx, row in enumerate(table.select("tr")):
        if row.find("th"):
            continue
        if idx == 0:
            versions.append(card_code)  # v=0 → NO SUFFIX
        else:
            versions.append(f"{card_code}v={idx}")

    return versions


# ------------------------------------------------------
# LIMITLESS PRICE SCRAPER
# ------------------------------------------------------
def scrape_card_price(card_id: str):
    base = re.match(r"([A-Z]+[0-9]{2}-[0-9]{3})", card_id).group(1)

    m = re.search(r"v=(\d+)", card_id)
    version = int(m.group(1)) if m else 0

    formatted = f"{base}?v={version}" if version > 0 else base
    url = LIMITLESS_BASE.format(formatted)

    r = requests.get(url)
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

        usd_val = float(re.search(r"([\d\.]+)", usd_link.text).group(1)) if usd_link else 0
        eur_val = float(re.search(r"([\d\.]+)", eur_link.text).group(1)) if eur_link else 0

        usd.append(usd_val)
        eur.append(eur_val)

    if version >= len(usd):
        version = 0

    return eur[version], usd[version]

# ------------------------------------------------------
# COLLECTR SCRAPER
# ------------------------------------------------------
import time
from requests.exceptions import RequestException

def scrape_collectr(card_type: str, retries: int = 3, timeout: int = 15):
    url = COLLECTR_URL.format(card_type)

    for attempt in range(1, retries + 1):
        try:
            r = requests.get(
                url,
                timeout=timeout,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; OPTCGSnapshot/1.0)"
                },
            )

            # If Collectr is up but slow, this still may be HTML error page
            if r.status_code != 200:
                raise RequestException(f"HTTP {r.status_code}")

            soup = BeautifulSoup(r.text, "html.parser")
            items = []

            for card in soup.select("div.card-item"):
                name_el = card.select_one(".card-name")
                price_el = card.select_one(".price")

                if not name_el or not price_el:
                    continue

                m = re.search(r"([\d\.]+)", price_el.text)
                if not m:
                    continue

                usd = float(m.group(1))
                eur = round(usd * USD_TO_EUR, 2)

                items.append((name_el.get_text(strip=True), eur, usd))

            print(f"✔ Collectr {card_type}: {len(items)} items")
            return items

        except Exception as e:
            print(
                f"⚠ Collectr {card_type} attempt {attempt}/{retries} failed: {e}"
            )
            time.sleep(3)

    # HARD FAIL PROTECTION
    print(f"❌ Collectr {card_type} unavailable — skipping snapshot")
    return []

# ------------------------------------------------------
# SNAPSHOT RECORDERS
# ------------------------------------------------------
def record_cards(cursor, card_codes):
    for code in card_codes:
        versions = extract_versions(code)

        for cid in versions:
            cid = normalize_card_id(cid)
            eur, usd = scrape_card_price(cid)

            cursor.execute(
                """
                INSERT OR IGNORE INTO card_history
                (card_id, date, eur_price, usd_price)
                VALUES (?, ?, ?, ?)
                """,
                (cid, TODAY, eur, usd),
            )


def record_sealed(cursor):
    for name, eur, usd in scrape_collectr("sealed"):
        cursor.execute(
            """
            INSERT OR IGNORE INTO sealed_history
            (name, date, eur_price, usd_price)
            VALUES (?, ?, ?, ?)
            """,
            (name, TODAY, eur, usd),
        )


def record_dons(cursor):
    for name, eur, usd in scrape_collectr("don"):
        cursor.execute(
            """
            INSERT OR IGNORE INTO don_history
            (name, date, eur_price, usd_price)
            VALUES (?, ?, ?, ?)
            """,
            (name, TODAY, eur, usd),
        )

# ------------------------------------------------------
# MAIN
# ------------------------------------------------------
def main():
    init_db()

    # EXAMPLE: load your card list from file or elsewhere
    # This MUST be base codes ONLY (NO v=)
    card_codes = [
        "OP13-001",
        "OP13-002",
        "OP05-041",
        # ...
    ]

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    record_cards(cursor, card_codes)
    record_sealed(cursor)
    record_dons(cursor)

    conn.commit()
    conn.close()

    print("Daily snapshot completed:", TODAY)


if __name__ == "__main__":
    main()
