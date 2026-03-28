#!/usr/bin/env python3
"""
Orchestrate the Atreus E2E flow: scrape HTML -> parse JSON -> load database.
"""

import at_loader
import at_linker
import at_parser
import at_scraper

# Define jobs here so the scraper can inherit them.
JOBS = [
    {"url": "https://www.atmovies.com.tw/showtime/t07707/a07/", "cinema": "Dream_Mall"},
    {"url": "https://www.atmovies.com.tw/showtime/t07703/a07/", "cinema": "VieShow"},
]


def main() -> None:
    # Step 1: scrape raw HTML into temp files.
    at_scraper.main(jobs=JOBS)

    # Step 2: parse HTML into a JSON payload.
    at_parser.main()

    # Step 3: load the JSON payload into Supabase.
    at_loader.main()

    # Step 4: link titles to TMDB IDs.
    at_linker.main()


if __name__ == "__main__":
    main()
