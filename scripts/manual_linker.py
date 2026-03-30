#!/usr/bin/env python3
"""
Manually load TMDB movie records into movies by a list of IDs.

Behavior:
- Fetches movie details by ID from TMDB /movie/{id}.
- Inserts or overwrites rows in movies depending on MODE.
- Fetches translations and updates EN/ZH fields.
"""

import json
import os
import time
from dataclasses import dataclass
from typing import Optional, Any
from urllib.parse import urlencode
from urllib.request import urlopen

from supabase import create_client

from dotenv import load_dotenv

# Load local env vars without overriding existing (e.g., GitHub Actions) values.
load_dotenv(override=False)

# Manual list of TMDB ids to load.
MOVIE_IDS = [
#    1359916,
]

# Load mode: "safe" (skip existing) or "overwrite" (upsert).
MODE = "safe"

TABLE_NAME_MOVIES = "movies"
TMDB_MOVIE_ENDPOINT = "https://api.themoviedb.org/3/movie/{movie_id}"
TMDB_TRANSLATIONS_ENDPOINT = "https://api.themoviedb.org/3/movie/{movie_id}/translations"
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


# Normalize translation field to None when empty.
def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    return value or None


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


# Convert TMDB movie response to movies table payload.
def _movie_payload(result: dict[str, Any]) -> dict[str, Any]:
    genres = result.get("genres")
    if isinstance(genres, list):
        genre_ids = [g.get("id") for g in genres if isinstance(g, dict)]
        genre_ids = [g for g in genre_ids if isinstance(g, int)]
        genre_ids_value = json.dumps(genre_ids, ensure_ascii=False)
    else:
        genre_ids_value = None
    origin_country = result.get("origin_country")
    if isinstance(origin_country, list):
        origin_country_value = json.dumps(origin_country, ensure_ascii=False)
    else:
        origin_country_value = None
    return {
        "adult": result.get("adult"),
        "backdrop_path": result.get("backdrop_path"),
        "genre_ids": genre_ids_value,
        "id": result.get("id"),
        "imdb_id": result.get("imdb_id"),
        "original_language": result.get("original_language"),
        "original_title": result.get("original_title"),
        "overview_en": result.get("overview"),
        "popularity": result.get("popularity"),
        "poster_path": result.get("poster_path"),
        "release_date": result.get("release_date"),
        "video": result.get("video"),
        "vote_average": result.get("vote_average"),
        "vote_count": result.get("vote_count"),
        "origin_country": origin_country_value,
    }


# Fetch existing movie ids from movies.
def _fetch_existing_movie_ids(client) -> set[int]:
    response = client.table(TABLE_NAME_MOVIES).select("id").execute()
    if response.data is None:
        raise RuntimeError(f"Select failed: {response}")
    existing: set[int] = set()
    for row in response.data:
        movie_id = row.get("id")
        if isinstance(movie_id, int):
            existing.add(movie_id)
    return existing


# Fetch a reasonable title for logging from existing rows.
def _fetch_existing_movie_title(client, movie_id: int) -> Optional[str]:
    response = (
        client.table(TABLE_NAME_MOVIES)
        .select("ctitle_en, ctitle_zh, original_title, overview_en")
        .eq("id", movie_id)
        .limit(1)
        .execute()
    )
    if response.data is None:
        raise RuntimeError(f"Select failed: {response}")
    if not response.data:
        return None
    row = response.data[0]
    for key in ("ctitle_en", "ctitle_zh", "original_title", "overview_en"):
        value = _clean_text(row.get(key))
        if value:
            return value
    return None


def main() -> None:
    if MODE not in {"safe", "overwrite"}:
        raise RuntimeError(f"Invalid MODE: {MODE}")

    api_key = _require_env("TMDB_API_KEY")
    config = _load_config()
    client = create_client(config.url, config.key)

    existing_ids = _fetch_existing_movie_ids(client)
    inserted = 0
    overwritten = 0
    skipped = 0
    failed = 0
    translations_missing = 0
    base_calls = 0
    translation_calls = 0

    for raw_id in MOVIE_IDS:
        try:
            movie_id = int(raw_id)
        except (TypeError, ValueError):
            print(f"Failed: id={raw_id} title=None error=invalid id")
            failed += 1
            continue

        exists = movie_id in existing_ids
        if MODE == "safe" and exists:
            title = _fetch_existing_movie_title(client, movie_id)
            print(f"Skipped: id={movie_id} title={title}")
            skipped += 1
            continue

        try:
            result = _tmdb_movie(api_key, movie_id)
        except Exception as exc:  # noqa: BLE001
            print(f"Failed: id={movie_id} title=None error={exc}")
            failed += 1
            continue

        if not result:
            print(f"Failed: id={movie_id} title=None error=no data returned")
            failed += 1
            continue

        base_calls += 1
        if base_calls % PAUSE_EVERY == 0:
            time.sleep(PAUSE_SECONDS)

        payload = _movie_payload(result)
        title = result.get("title") if isinstance(result, dict) else None

        try:
            if MODE == "overwrite":
                response = (
                    client.table(TABLE_NAME_MOVIES)
                    .upsert([payload], on_conflict="id")
                    .execute()
                )
                if response.data is None:
                    raise RuntimeError(f"Upsert failed: {response}")
            else:
                response = client.table(TABLE_NAME_MOVIES).insert([payload]).execute()
                if response.data is None:
                    raise RuntimeError(f"Insert failed: {response}")
        except Exception as exc:  # noqa: BLE001
            print(f"Failed: id={movie_id} title={title} error={exc}")
            failed += 1
            continue

        if exists:
            overwritten += 1
            status = "overwritten"
        else:
            inserted += 1
            status = "inserted"
            existing_ids.add(movie_id)

        # Fetch translations and update row.
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
            print(f"Warning: no translation data for id={movie_id}")
            translations_missing += 1
        response = (
            client.table(TABLE_NAME_MOVIES)
            .update(update_payload)
            .eq("id", movie_id)
            .execute()
        )
        if response.data is None:
            raise RuntimeError(f"Update failed for movie id {movie_id}: {response}")

        translation_calls += 1
        if translation_calls % PAUSE_EVERY == 0:
            time.sleep(PAUSE_SECONDS)

        print(f"{status.title()}: id={movie_id} title={title}")

    print(
        "Summary:",
        f"mode={MODE}",
        f"total_ids={len(MOVIE_IDS)}",
        f"inserted={inserted}",
        f"overwritten={overwritten}",
        f"skipped={skipped}",
        f"failed={failed}",
        f"translations_missing={translations_missing}",
    )


if __name__ == "__main__":
    main()
