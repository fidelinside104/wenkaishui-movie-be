#!/usr/bin/env python3
"""
Link today's scraped movie titles to TMDB IDs in title_links.

Behavior:
- Uses Taiwan timezone to pick today's file.
- Looks up only titles not already in title_links.
- Uses the first TMDB search result (if any).
- Inserts NULL id when TMDB returns no results.
"""

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Any
from urllib.parse import urlencode
from urllib.request import urlopen
from zoneinfo import ZoneInfo

from supabase import create_client

from dotenv import load_dotenv

# Load local env vars without overriding existing (e.g., GitHub Actions) values.
load_dotenv(override=False)

TIMEZONE = ZoneInfo("Asia/Taipei")
TABLE_NAME_TITLE_LINKS = "title_links"
TABLE_NAME_MOVIES = "movies"
TMDB_SEARCH_ENDPOINT = "https://api.themoviedb.org/3/search/movie"
TMDB_TRANSLATIONS_ENDPOINT = "https://api.themoviedb.org/3/movie/{movie_id}/translations"
TMDB_MOVIE_ENDPOINT = "https://api.themoviedb.org/3/movie/{movie_id}"
PAGE_SIZE = 1000
PAUSE_EVERY = 25
PAUSE_SECONDS = 1


# Ensure required env vars exist.
def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


# Container for Supabase connection info.
@dataclass(frozen=True)
class SupabaseConfig:
    url: str
    key: str


# Load Supabase URL/key from env.
def _load_config() -> SupabaseConfig:
    return SupabaseConfig(
        url=_require_env("SUPABASE_URL_MO"),
        key=_require_env("SUPABASE_SECRET_KEY_MO"),
    )


# Build today's date string in Taiwan timezone.
def _today_str() -> str:
    return datetime.now(TIMEZONE).strftime("%Y-%m-%d")


# Read JSON rows from a file.
def _load_rows(json_path: Path) -> list[dict]:
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise RuntimeError(f"Expected a JSON array in {json_path}")
    return data


# Extract unique, non-empty movie titles in input order.
def _extract_titles(rows: list[dict]) -> list[str]:
    titles: list[str] = []
    seen = set()
    for row in rows:
        title = row.get("movie_name")
        if title is None:
            continue
        title = str(title).strip()
        if not title:
            continue
        if title in seen:
            continue
        seen.add(title)
        titles.append(title)
    return titles


# Fetch all existing movie_title values from title_links.
def _fetch_existing_titles(client) -> set[str]:
    existing: set[str] = set()
    offset = 0
    while True:
        response = (
            client.table(TABLE_NAME_TITLE_LINKS)
            .select("movie_title")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        if response.data is None:
            raise RuntimeError(f"Select failed: {response}")
        rows = response.data
        if not rows:
            break
        for row in rows:
            title = row.get("movie_title")
            if title:
                existing.add(title)
        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return existing


# Fetch all existing movie ids from movies.
def _fetch_existing_movie_ids(client) -> set[int]:
    existing: set[int] = set()
    offset = 0
    while True:
        response = (
            client.table(TABLE_NAME_MOVIES)
            .select("id")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        if response.data is None:
            raise RuntimeError(f"Select failed: {response}")
        rows = response.data
        if not rows:
            break
        for row in rows:
            movie_id = row.get("id")
            if isinstance(movie_id, int):
                existing.add(movie_id)
        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return existing


# Call TMDB search endpoint and return the first result object (or None).
def _tmdb_first_result(api_key: str, title: str) -> Optional[dict[str, Any]]:
    params = urlencode({"query": title, "api_key": api_key})
    url = f"{TMDB_SEARCH_ENDPOINT}?{params}"
    with urlopen(url, timeout=10) as response:
        payload = json.load(response)
    results = payload.get("results") if isinstance(payload, dict) else None
    if not results:
        return None
    first = results[0] if isinstance(results, list) else None
    if not isinstance(first, dict):
        return None
    return first


# Map TMDB result to movies table payload.
def _movie_payload(result: dict[str, Any]) -> dict[str, Any]:
    genre_ids = result.get("genre_ids")
    if isinstance(genre_ids, list):
        genre_ids_value = json.dumps(genre_ids, ensure_ascii=False)
    else:
        genre_ids_value = None
    return {
        "adult": result.get("adult"),
        "backdrop_path": result.get("backdrop_path"),
        "genre_ids": genre_ids_value,
        "id": result.get("id"),
        "original_language": result.get("original_language"),
        "original_title": result.get("original_title"),
        "overview_en": result.get("overview"),
        "popularity": result.get("popularity"),
        "poster_path": result.get("poster_path"),
        "release_date": result.get("release_date"),
        "video": result.get("video"),
        "vote_average": result.get("vote_average"),
        "vote_count": result.get("vote_count"),
    }


# Upsert rows into title_links by movie_title.
def _upsert_title_links(client, rows: list[dict]) -> int:
    if not rows:
        return 0
    response = (
        client.table(TABLE_NAME_TITLE_LINKS)
        .upsert(rows, on_conflict="movie_title", ignore_duplicates=True)
        .execute()
    )
    if response.data is None:
        raise RuntimeError(f"Upsert failed: {response}")
    return len(response.data)


# Insert rows into movies.
def _insert_movies(client, rows: list[dict]) -> int:
    if not rows:
        return 0
    response = client.table(TABLE_NAME_MOVIES).insert(rows).execute()
    if response.data is None:
        raise RuntimeError(f"Insert failed: {response}")
    return len(response.data)


# Call TMDB translations endpoint and return the translations list.
def _tmdb_translations(api_key: str, movie_id: int) -> list[dict[str, Any]]:
    params = urlencode({"api_key": api_key})
    url = TMDB_TRANSLATIONS_ENDPOINT.format(movie_id=movie_id)
    url = f"{url}?{params}"
    with urlopen(url, timeout=10) as response:
        payload = json.load(response)
    translations = payload.get("translations") if isinstance(payload, dict) else None
    if not isinstance(translations, list):
        return []
    return [t for t in translations if isinstance(t, dict)]


# Call TMDB movie endpoint and return movie data.
def _tmdb_movie(api_key: str, movie_id: int) -> Optional[dict[str, Any]]:
    params = urlencode({"api_key": api_key})
    url = TMDB_MOVIE_ENDPOINT.format(movie_id=movie_id)
    url = f"{url}?{params}"
    with urlopen(url, timeout=10) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        return None
    return payload


# Pick translation data by iso_3166_1 code.
def _pick_translation(
    translations: list[dict[str, Any]], iso_3166_1: str
) -> Optional[dict[str, Any]]:
    for item in translations:
        if item.get("iso_3166_1") == iso_3166_1:
            data = item.get("data")
            if isinstance(data, dict):
                return data
            return {}
    return None


# Normalize translation field to None when empty.
def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    return value or None


# Orchestrate linking for today's JSON file.
def main() -> None:
    temp_dir = Path(__file__).resolve().parents[1] / "temp"
    today_str = _today_str()
    json_path = temp_dir / f"Scraped_{today_str}.json"

    if not json_path.exists():
        raise RuntimeError(f"Missing JSON file: {json_path}")

    rows = _load_rows(json_path)
    titles = _extract_titles(rows)

    config = _load_config()
    client = create_client(config.url, config.key)

    existing = _fetch_existing_titles(client)
    new_titles = [title for title in titles if title not in existing]
    existing_movie_ids = _fetch_existing_movie_ids(client)

    api_key = _require_env("TMDB_API_KEY")
    title_links_rows: list[dict] = []
    movies_rows: list[dict] = []
    seen_movie_ids: set[int] = set()
    looked_up = 0
    missing = 0
    skipped_existing = 0
    skipped_dupe = 0
    for title in new_titles:
        result = _tmdb_first_result(api_key, title)
        tmdb_id = result.get("id") if isinstance(result, dict) else None
        if tmdb_id is None:
            print(f"Warning: no TMDB results for '{title}'")
            missing += 1
        title_links_rows.append({"movie_title": title, "id": tmdb_id})
        if isinstance(tmdb_id, int):
            if tmdb_id in existing_movie_ids:
                print(f"Warning: movie id already exists in movies: {tmdb_id}")
                skipped_existing += 1
            elif tmdb_id in seen_movie_ids:
                print(f"Warning: duplicate movie id in this run: {tmdb_id}")
                skipped_dupe += 1
            else:
                seen_movie_ids.add(tmdb_id)
                movies_rows.append(_movie_payload(result))
        looked_up += 1
        if looked_up % PAUSE_EVERY == 0:
            time.sleep(PAUSE_SECONDS)

    inserted_links = _upsert_title_links(client, title_links_rows)
    inserted_movies = _insert_movies(client, movies_rows)

    # Fetch translations only for newly inserted movies.
    translations_updated = 0
    translations_missing = 0
    translation_calls = 0
    for movie in movies_rows:
        movie_id = movie.get("id")
        if not isinstance(movie_id, int):
            continue
        translations = _tmdb_translations(api_key, movie_id)
        en_data = _pick_translation(translations, "US")
        zh_data = (
            _pick_translation(translations, "TW")
            or _pick_translation(translations, "HK")
            or _pick_translation(translations, "CN")
        )
        update_payload = {
            "ctitle_en": _clean_text(en_data.get("title") if en_data else None),
            "tagline_en": _clean_text(en_data.get("tagline") if en_data else None),
            "ctitle_zh": _clean_text(zh_data.get("title") if zh_data else None),
            "overview_zh": _clean_text(zh_data.get("overview") if zh_data else None),
            "tagline_zh": _clean_text(zh_data.get("tagline") if zh_data else None),
        }
        if all(value is None for value in update_payload.values()):
            print(f"Warning: no translation data for movie id {movie_id}")
            translations_missing += 1
        response = (
            client.table(TABLE_NAME_MOVIES)
            .update(update_payload)
            .eq("id", movie_id)
            .execute()
        )
        if response.data is None:
            raise RuntimeError(f"Update failed for movie id {movie_id}: {response}")
        translations_updated += 1
        translation_calls += 1
        if translation_calls % PAUSE_EVERY == 0:
            time.sleep(PAUSE_SECONDS)

    # Fetch imdb_id and origin_country for newly inserted movies.
    details_updated = 0
    details_missing = 0
    details_calls = 0
    for movie in movies_rows:
        movie_id = movie.get("id")
        if not isinstance(movie_id, int):
            continue
        details = _tmdb_movie(api_key, movie_id)
        if not details:
            print(f"Warning: no details data for movie id {movie_id}")
            details_missing += 1
            continue
        origin_country = details.get("origin_country")
        if isinstance(origin_country, list):
            origin_country_value = json.dumps(origin_country, ensure_ascii=False)
        else:
            origin_country_value = None
        update_payload = {
            "imdb_id": _clean_text(details.get("imdb_id")),
            "origin_country": origin_country_value,
        }
        if all(value is None for value in update_payload.values()):
            print(f"Warning: missing imdb_id/origin_country for movie id {movie_id}")
            details_missing += 1
        response = (
            client.table(TABLE_NAME_MOVIES)
            .update(update_payload)
            .eq("id", movie_id)
            .execute()
        )
        if response.data is None:
            raise RuntimeError(f"Update failed for movie id {movie_id}: {response}")
        details_updated += 1
        details_calls += 1
        if details_calls % PAUSE_EVERY == 0:
            time.sleep(PAUSE_SECONDS)

    print(
        "Summary:",
        f"date={today_str}",
        f"input_titles={len(titles)}",
        f"existing_titles={len(existing)}",
        f"looked_up={looked_up}",
        f"missing_ids={missing}",
        f"movies_existing_skipped={skipped_existing}",
        f"movies_dupe_skipped={skipped_dupe}",
        f"title_links_inserted={inserted_links}",
        f"movies_inserted={inserted_movies}",
        f"translations_updated={translations_updated}",
        f"translations_missing={translations_missing}",
        f"details_updated={details_updated}",
        f"details_missing={details_missing}",
    )


if __name__ == "__main__":
    main()
