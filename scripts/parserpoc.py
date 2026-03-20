#!/usr/bin/env python3
"""
Parse today's FPG_YYYY-MM-DD_<cinema>.html files and write flat JSON rows.

Assumptions:
- Each movie block is a <ul id="theaterShowtimeTable"> element.
- Movie title is inside <li class="filmTitle"> -> <a>.
- Screening times appear as <li> text like "13：00" or "13:00".
- Input files are named FPG_YYYY-MM-DD_<cinema>.html in the temp folder.
"""

import json
import re
from pathlib import Path
from typing import List, Dict
from datetime import datetime
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup


# Match time strings that use either a full-width colon "：" or a normal colon ":".
TIME_RE = re.compile(r"\b\d{1,2}[：:]\d{2}\b")
FPG_FILE_RE = re.compile(r"^FPG_(\d{4}-\d{2}-\d{2})_(.+)\.html$")
TIMEZONE = ZoneInfo("Asia/Taipei")


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
    # Default to the temp folder at the repository root.
    temp_dir = Path(__file__).resolve().parents[1] / "temp"
    today_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    pattern = f"FPG_{today_str}_*.html"

    rows: List[Dict[str, object]] = []
    files_processed = 0
    movies_parsed = 0
    for html_path in sorted(temp_dir.glob(pattern)):
        match = FPG_FILE_RE.match(html_path.name)
        if not match:
            continue

        files_processed += 1
        file_date, cinema_name = match.group(1), match.group(2).replace("_", " ")
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()

        movies = extract_movie_blocks(html)
        movies_parsed += len(movies)
        for movie in movies:
            for time in movie["times"]:
                rows.append(
                    {
                        "screening_date": file_date,
                        "cinema_name": cinema_name,
                        "movie_name": movie["title"],
                        "screening_time": time,
                    }
                )

    output_path = temp_dir / f"Scraped_{today_str}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    print(
        "Summary:",
        f"files={files_processed}",
        f"movies={movies_parsed}",
        f"rows={len(rows)}",
        f"output={output_path}",
    )


if __name__ == "__main__":
    main()
