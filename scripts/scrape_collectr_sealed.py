from playwright.sync_api import sync_playwright
import json
import os
import re

URL = "https://app.getcollectr.com/?sortType=price&sortOrder=DESC&cardType=sealed&category=68"
USD_TO_EUR_RATE = 0.75

OUT_PATH = os.path.join("data", "sealed_collectr.json")
os.makedirs("data", exist_ok=True)


def parse_price(text):
    if not text:
        return None
    text = text.replace("$", "").replace(",", "").strip()
    try:
        return float(text)
    except ValueError:
        return None


def usd_to_eur(usd):
    if usd is None:
        return None
    return round(usd * USD_TO_EUR_RATE, 2)


def scrape_collectr_sealed():
    out = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        page.goto(URL, timeout=60000)

        # Wait for actual product cards
        page.wait_for_selector("li div.flex", timeout=30000)
        page.wait_for_timeout(2000)

        # Infinite scroll until stable
        prev_count = 0
        same_rounds = 0

        while True:
            rows = page.query_selector_all("li")
            cur = len(rows)

            if cur == prev_count:
                same_rounds += 1
            else:
                same_rounds = 0

            if same_rounds >= 3:
                break

            prev_count = cur
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)

        rows = page.query_selector_all("li")

        for row in rows:
            # NAME (stable)
            name_el = row.query_selector("span.line-clamp-2")

            # PRICE (find any span containing "$")
            price_candidates = row.query_selector_all("span")
            price_el = None

            for span in price_candidates:
                text = span.inner_text().strip()
                if "$" in text and re.search(r"\d", text):
                    price_el = span
                    break

            img_el = row.query_selector("img")

            if not name_el or not price_el:
                continue

            name = name_el.inner_text().strip()
            usd = parse_price(price_el.inner_text())
            eur = usd_to_eur(usd)
            image_url = img_el.get_attribute("src") if img_el else None

            if not name or usd is None:
                continue

            key = f"sealed::{name}"
            out[key] = {
                "usd": float(usd),
                "eur": float(eur) if eur is not None else 0.0,
                "image_url": image_url,
                "source": "collectr",
            }

        browser.close()

    return out


if __name__ == "__main__":
    data = scrape_collectr_sealed()

    # 🔥 SAFETY: do not overwrite file if scrape failed
    if len(data) == 0:
        print("⚠️ No sealed data scraped. Skipping overwrite.")
    else:
        with open(OUT_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        print(f"Saved {len(data)} sealed items → {OUT_PATH}")
