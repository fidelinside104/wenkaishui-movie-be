"""
This script:
1. Opens the atmovies page
2. Waits 3 seconds
3. Extracts the content inside the div with class "theaterShowtimeBlock"
4. Saves the content into a text file
"""

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# Load .env file automatically
load_dotenv()

JOBS = [
    {"url": "https://www.atmovies.com.tw/showtime/t07707/a07/", "cinema": "Dream_Mall"},
    {"url": "https://www.atmovies.com.tw/showtime/t07703/a07/", "cinema": "VieShow"},
]

ROOT_DIR = Path(__file__).resolve().parent.parent
DEBUG_DIR = ROOT_DIR / "temp"
SELECTOR = ".theaterShowtimeBlock"
TIMEZONE = ZoneInfo("Asia/Taipei")
GOTO_TIMEOUT_MS = 8000
SELECTOR_TIMEOUT_MS = 8000


def scrape_job(page, url, cinema, run_date):
    print(f"Opening page for {cinema}...")
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        print(f"Warning: page.goto timed out for {cinema}; continuing to try extracting content...")

    # Wait until the relevant content exists (avoid full-page load)
    try:
        page.wait_for_selector(SELECTOR, timeout=SELECTOR_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        print(f"Warning: selector not found in time for {cinema}; continuing to capture current HTML...")

    # Print basic info
    print("Page title:", page.title())
    print("Current URL:", page.url)

    debug_file = DEBUG_DIR / f"FPG_{run_date}_{cinema}.html"
    full_html = page.content()
    with debug_file.open("w", encoding="utf-8") as f:
        f.write(full_html)
    print(f"Saved full page HTML to {debug_file}")


def main():
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    run_date = datetime.now(TIMEZONE).strftime("%Y-%m-%d")

    # Start Playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        for job in JOBS:
            scrape_job(page, job["url"], job["cinema"], run_date)
        
        # Close the browser
        browser.close()


# Run the script
if __name__ == "__main__":
    main()
