#!/usr/bin/env python3
"""
Parse FULL_PAGE_DEBUG.html and return JSON listing movies and screening times.

Assumptions:
- Each movie block is a <ul id="theaterShowtimeTable"> element.
- Movie title is inside <li class="filmTitle"> -> <a>.
- Screening times appear as <li> text like "13：00" or "13:00".
"""

import json
import os
import re
from typing import List, Dict

from bs4 import BeautifulSoup


# Match time strings that use either a full-width colon "：" or a normal colon ":".
TIME_RE = re.compile(r"\b\d{1,2}[：:]\d{2}\b")


def normalize_time(time_str: str) -> str:
    """
    Normalize time strings to use the standard colon ":".
    Example: "13：00" -> "13:00"
    """
    return time_str.replace("：", ":").strip()


def extract_movie_blocks(html: str) -> List[Dict[str, object]]:
    """
    Extract movie titles and screening times from the HTML document.
    Returns a list of dicts: [{"title": "...", "times": ["13:00", "22:50"]}, ...]
    """
    soup = BeautifulSoup(html, "html.parser")
    movies = []

    # There are multiple <ul id="theaterShowtimeTable"> blocks, one per movie.
    for block in soup.find_all("ul", id="theaterShowtimeTable"):
        # 1) Find the movie title.
        title_li = block.find("li", class_="filmTitle")
        title = None
        if title_li:
            title_link = title_li.find("a")
            if title_link:
                title = title_link.get_text(strip=True)

        # Skip if we can't find a title (defensive).
        if not title:
            continue

        # 2) Find screening times within the same block.
        # We scan all <li> tags inside the block and pick out strings that look like times.
        times = []
        for li in block.find_all("li"):
            text = li.get_text(strip=True)
            if TIME_RE.search(text):
                times.append(normalize_time(text))

        # Remove duplicates while preserving order.
        seen = set()
        unique_times = []
        for t in times:
            if t not in seen:
                seen.add(t)
                unique_times.append(t)

        movies.append({"title": title, "times": unique_times})

    return movies


def main() -> None:
    # Default to FULL_PAGE_DEBUG.html in the same folder as this script.
    here = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(here, "FULL_PAGE_DEBUG.html")

    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()

    result = extract_movie_blocks(html)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
