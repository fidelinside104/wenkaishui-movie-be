"""
This script:
1. Opens the atmovies page
2. Waits 3 seconds
3. Extracts the content inside the div with class "theaterShowtimeBlock"
4. Saves the content into a text file
"""

from dotenv import load_dotenv
from pathlib import Path

from playwright.sync_api import sync_playwright

# Load .env file automatically
load_dotenv()

# The URL we want to open
URL = "https://www.atmovies.com.tw/showtime/t07707/a07/"

ROOT_DIR = Path(__file__).resolve().parent.parent
DEBUG_DIR = ROOT_DIR / "temp"
DEBUG_FILE = DEBUG_DIR / "FULL_PAGE_DEBUG.html"


def main():
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    # Start Playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        print("Opening page...")
        page.goto(URL)

        # Wait 3 seconds (as before)
        page.wait_for_timeout(3000)

        # Print basic info
        print("Page title:", page.title())
        print("Current URL:", page.url)

        # Save full HTML
        full_html = page.content()
        with DEBUG_FILE.open("w", encoding="utf-8") as f:
            f.write(full_html)
        print(f"Saved full page HTML to {DEBUG_FILE}")
        
        # Close the browser
        browser.close()


# Run the script
if __name__ == "__main__":
    main()
