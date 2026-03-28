#!/usr/bin/env python3
"""
Compose today's screenings into screenings_serve with enriched movie metadata.

Behavior:
- Uses Taiwan timezone to select today's screenings.
- Enriches from title_links, versions, and movies.
- Performs a full replace on screenings_serve.
"""

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo

from supabase import create_client

from dotenv import load_dotenv

# Load local env vars without overriding existing (e.g., GitHub Actions) values.
load_dotenv(override=False)

TIMEZONE = ZoneInfo("Asia/Taipei")
TABLE_NAME_SCREENINGS = "screenings"
TABLE_NAME_SCREENINGS_SERVE = "screenings_serve"
TABLE_NAME_TITLE_LINKS = "title_links"
TABLE_NAME_VERSIONS = "versions"
TABLE_NAME_MOVIES = "movies"
ALL_ROWS_SENTINEL = "00000000-0000-0000-0000-000000000000"
PAGE_SIZE = 1000
CHUNK_SIZE = 200


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


# Normalize text values; return None for empty strings.
def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    return value or None


def _chunked(values: Iterable[Any], size: int) -> list[list[Any]]:
    chunk: list[Any] = []
    chunks: list[list[Any]] = []
    for value in values:
        chunk.append(value)
        if len(chunk) >= size:
            chunks.append(chunk)
            chunk = []
    if chunk:
        chunks.append(chunk)
    return chunks


# Delete all rows from the target table.
def _delete_all_rows(client, table_name: str) -> int:
    response = (
        client.table(table_name).delete().neq("id", ALL_ROWS_SENTINEL).execute()
    )
    if response.data is None:
        raise RuntimeError(f"Delete failed: {response}")
    return len(response.data)


# Insert all rows into the target table.
def _insert_rows(client, table_name: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    response = client.table(table_name).insert(rows).execute()
    if response.data is None:
        raise RuntimeError(f"Insert failed: {response}")
    return len(response.data)


# Fetch screenings for the provided screening_date (YYYY-MM-DD).
def _fetch_screenings_for_date(client, screening_date: str) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        response = (
            client.table(TABLE_NAME_SCREENINGS)
            .select(
                "id,cinema_name,movie_name,screening_time,screening_date,movie_version,movie_length"
            )
            .eq("screening_date", screening_date)
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        if response.data is None:
            raise RuntimeError(f"Select failed: {response}")
        batch = response.data
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return rows


# Fetch title_links for provided movie titles.
def _fetch_title_links(client, titles: list[str]) -> dict[str, Any]:
    if not titles:
        return {}
    query = client.table(TABLE_NAME_TITLE_LINKS).select("movie_title,id")
    if hasattr(query, "in_"):
        results: dict[str, Any] = {}
        for chunk in _chunked(titles, CHUNK_SIZE):
            response = query.in_("movie_title", chunk).execute()
            if response.data is None:
                raise RuntimeError(f"Select failed: {response}")
            for row in response.data:
                title = row.get("movie_title")
                if title:
                    results[title] = row.get("id")
        return results
    return _fetch_all_title_links(client, titles)


def _fetch_all_title_links(client, titles: list[str]) -> dict[str, Any]:
    wanted = set(titles)
    results: dict[str, Any] = {}
    offset = 0
    while True:
        response = (
            client.table(TABLE_NAME_TITLE_LINKS)
            .select("movie_title,id")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        if response.data is None:
            raise RuntimeError(f"Select failed: {response}")
        batch = response.data
        if not batch:
            break
        for row in batch:
            title = row.get("movie_title")
            if title in wanted:
                results[title] = row.get("id")
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return results


# Fetch versions for provided version_zh values.
def _fetch_versions(client, versions: list[str]) -> dict[str, Optional[str]]:
    if not versions:
        return {}
    query = client.table(TABLE_NAME_VERSIONS).select("version_zh,version_en")
    if hasattr(query, "in_"):
        results: dict[str, Optional[str]] = {}
        for chunk in _chunked(versions, CHUNK_SIZE):
            response = query.in_("version_zh", chunk).execute()
            if response.data is None:
                raise RuntimeError(f"Select failed: {response}")
            for row in response.data:
                zh = row.get("version_zh")
                if zh:
                    results[zh] = row.get("version_en")
        return results
    return _fetch_all_versions(client, versions)


def _fetch_all_versions(client, versions: list[str]) -> dict[str, Optional[str]]:
    wanted = set(versions)
    results: dict[str, Optional[str]] = {}
    offset = 0
    while True:
        response = (
            client.table(TABLE_NAME_VERSIONS)
            .select("version_zh,version_en")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        if response.data is None:
            raise RuntimeError(f"Select failed: {response}")
        batch = response.data
        if not batch:
            break
        for row in batch:
            zh = row.get("version_zh")
            if zh in wanted:
                results[zh] = row.get("version_en")
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return results


# Fetch movies for provided TMDB IDs.
def _fetch_movies(client, tmdb_ids: list[int]) -> dict[int, dict]:
    if not tmdb_ids:
        return {}
    query = client.table(TABLE_NAME_MOVIES).select(
        "id,ctitle_en,ctitle_zh,original_language,original_title,overview_en,poster_path,release_date"
    )
    if hasattr(query, "in_"):
        results: dict[int, dict] = {}
        for chunk in _chunked(tmdb_ids, CHUNK_SIZE):
            response = query.in_("id", chunk).execute()
            if response.data is None:
                raise RuntimeError(f"Select failed: {response}")
            for row in response.data:
                movie_id = row.get("id")
                if isinstance(movie_id, int):
                    results[movie_id] = row
        return results
    return _fetch_all_movies(client, tmdb_ids)


def _fetch_all_movies(client, tmdb_ids: list[int]) -> dict[int, dict]:
    wanted = set(tmdb_ids)
    results: dict[int, dict] = {}
    offset = 0
    while True:
        response = (
            client.table(TABLE_NAME_MOVIES)
            .select(
                "id,ctitle_en,ctitle_zh,original_language,original_title,overview_en,poster_path,release_date"
            )
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        if response.data is None:
            raise RuntimeError(f"Select failed: {response}")
        batch = response.data
        if not batch:
            break
        for row in batch:
            movie_id = row.get("id")
            if isinstance(movie_id, int) and movie_id in wanted:
                results[movie_id] = row
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return results


def _compose_movie_name(
    movie: Optional[dict], fallback_name: str, lang_code: str
) -> str:
    if movie:
        key = "ctitle_en" if lang_code == "en" else "ctitle_zh"
        ctitle = _clean_text(movie.get(key))
        if ctitle:
            return ctitle
        original_language = _clean_text(movie.get("original_language"))
        original_title = _clean_text(movie.get("original_title"))
        if original_language == lang_code and original_title:
            return original_title
    return fallback_name


def main() -> None:
    today_str = _today_str()
    config = _load_config()
    client = create_client(config.url, config.key)

    screenings = _fetch_screenings_for_date(client, today_str)

    titles = sorted(
        {str(row.get("movie_name")).strip() for row in screenings if row.get("movie_name")}
    )
    versions = sorted(
        {
            str(row.get("movie_version")).strip()
            for row in screenings
            if row.get("movie_version")
        }
    )

    title_links = _fetch_title_links(client, titles)
    versions_map = _fetch_versions(client, versions)

    tmdb_ids = sorted(
        {
            tmdb_id
            for tmdb_id in title_links.values()
            if isinstance(tmdb_id, int)
        }
    )
    movies = _fetch_movies(client, tmdb_ids)

    composed_rows: list[dict] = []
    title_hits = 0
    version_hits = 0
    movie_hits = 0
    for row in screenings:
        movie_name_raw = row.get("movie_name")
        tmdb_id = title_links.get(movie_name_raw)
        if tmdb_id is not None:
            title_hits += 1

        movie = movies.get(tmdb_id) if isinstance(tmdb_id, int) else None
        if movie is not None:
            movie_hits += 1

        raw_version = row.get("movie_version")
        mapped_version = versions_map.get(raw_version) if raw_version else None
        if mapped_version is not None:
            version_hits += 1
        final_version = mapped_version if mapped_version is not None else raw_version

        composed_rows.append(
            {
                "id": row.get("id"),
                "cinema_name": row.get("cinema_name"),
                "movie_name_raw": movie_name_raw,
                "screening_time": row.get("screening_time"),
                "screening_date": row.get("screening_date"),
                "movie_version": final_version,
                "movie_name_en": _compose_movie_name(movie, movie_name_raw, "en"),
                "movie_name_zh": _compose_movie_name(movie, movie_name_raw, "zh"),
                "overview_en": movie.get("overview_en") if movie else None,
                "poster_path": movie.get("poster_path") if movie else None,
                "release_date": movie.get("release_date") if movie else None,
                "tmdb_id": tmdb_id,
                "movie_length": row.get("movie_length"),
            }
        )

    deleted_rows = _delete_all_rows(client, TABLE_NAME_SCREENINGS_SERVE)
    inserted_rows = _insert_rows(client, TABLE_NAME_SCREENINGS_SERVE, composed_rows)

    print(
        "Summary:",
        f"date={today_str}",
        f"input_rows={len(screenings)}",
        f"composed_rows={len(composed_rows)}",
        f"title_hits={title_hits}",
        f"version_hits={version_hits}",
        f"movie_hits={movie_hits}",
        f"deleted={deleted_rows}",
        f"inserted={inserted_rows}",
    )


if __name__ == "__main__":
    main()
