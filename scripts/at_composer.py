#!/usr/bin/env python3
"""
Compose today's screenings into screenings_serve with enriched movie metadata.

Behavior:
- Uses Taiwan timezone to select today's screenings.
- Enriches from title_links, versions, and movies.
- Performs a full replace on screenings_serve.
"""

import os
import json
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
TABLE_NAME_GENRES = "genres"
TABLE_NAME_LANGUAGES = "languages"
TABLE_NAME_COUNTRIES = "countries"
TABLE_NAME_CINEMAS = "cinemas"
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


# Split a list of values into fixed-size chunks.
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
        client.table(table_name)
        .delete()
        .neq("ingest_id", ALL_ROWS_SENTINEL)
        .execute()
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


# Fall back to a full scan when .in_ is unavailable.
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


# Fall back to a full scan when .in_ is unavailable.
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
        "id,ctitle_en,ctitle_zh,original_language,original_title,overview_en,"
        "poster_path,release_date,genre_ids,origin_country,tagline_en"
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


# Fall back to a full scan when .in_ is unavailable.
def _fetch_all_movies(client, tmdb_ids: list[int]) -> dict[int, dict]:
    wanted = set(tmdb_ids)
    results: dict[int, dict] = {}
    offset = 0
    while True:
        response = (
            client.table(TABLE_NAME_MOVIES)
            .select(
                "id,ctitle_en,ctitle_zh,original_language,original_title,overview_en,"
                "poster_path,release_date,genre_ids,origin_country,tagline_en"
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


# Parse list-like strings or arrays into a normalized list.
def _parse_list_field(value: Any) -> list[str]:
    # Normalize nulls to empty list early.
    if value is None:
        return []
    # Accept raw lists as-is.
    if isinstance(value, list):
        items = value
    else:
        # Coerce non-strings for parsing.
        if not isinstance(value, str):
            value = str(value)
        value = value.strip()
        # Treat empty strings as empty list.
        if not value:
            return []
        # Parse JSON-style lists when possible.
        if value.startswith("[") and value.endswith("]"):
            try:
                parsed = json.loads(value)
                items = parsed if isinstance(parsed, list) else [parsed]
            except json.JSONDecodeError:
                # Fall back to a simple comma split for malformed JSON.
                inner = value[1:-1].strip()
                if not inner:
                    return []
                items = [part.strip() for part in inner.split(",")]
        else:
            # Treat single non-list values as a one-item list.
            items = [value]
    # Clean and normalize items to non-empty strings.
    cleaned: list[str] = []
    for item in items:
        if item is None:
            continue
        if isinstance(item, str):
            text = item.strip().strip('"').strip("'")
        else:
            text = str(item).strip()
        if text:
            cleaned.append(text)
    return cleaned


# Fetch genre display names for provided genre ids.
def _fetch_genres(client, genre_ids: list[str]) -> dict[str, Optional[str]]:
    if not genre_ids:
        return {}
    query = client.table(TABLE_NAME_GENRES).select("genre_id,genre_en")
    if hasattr(query, "in_"):
        results: dict[str, Optional[str]] = {}
        for chunk in _chunked(genre_ids, CHUNK_SIZE):
            response = query.in_("genre_id", chunk).execute()
            if response.data is None:
                raise RuntimeError(f"Select failed: {response}")
            for row in response.data:
                gid = row.get("genre_id")
                if gid:
                    results[str(gid)] = row.get("genre_en")
        return results
    return _fetch_all_genres(client, genre_ids)


# Fall back to a full scan when .in_ is unavailable.
def _fetch_all_genres(client, genre_ids: list[str]) -> dict[str, Optional[str]]:
    wanted = set(str(gid) for gid in genre_ids)
    results: dict[str, Optional[str]] = {}
    offset = 0
    while True:
        response = (
            client.table(TABLE_NAME_GENRES)
            .select("genre_id,genre_en")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        if response.data is None:
            raise RuntimeError(f"Select failed: {response}")
        batch = response.data
        if not batch:
            break
        for row in batch:
            gid = row.get("genre_id")
            if gid is None:
                continue
            gid_str = str(gid)
            if gid_str in wanted:
                results[gid_str] = row.get("genre_en")
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return results


# Fetch language display names for provided language ids.
def _fetch_languages(client, language_ids: list[str]) -> dict[str, Optional[str]]:
    if not language_ids:
        return {}
    query = client.table(TABLE_NAME_LANGUAGES).select("language_id,language_en")
    if hasattr(query, "in_"):
        results: dict[str, Optional[str]] = {}
        for chunk in _chunked(language_ids, CHUNK_SIZE):
            response = query.in_("language_id", chunk).execute()
            if response.data is None:
                raise RuntimeError(f"Select failed: {response}")
            for row in response.data:
                lid = row.get("language_id")
                if lid:
                    results[str(lid)] = row.get("language_en")
        return results
    return _fetch_all_languages(client, language_ids)


# Fall back to a full scan when .in_ is unavailable.
def _fetch_all_languages(client, language_ids: list[str]) -> dict[str, Optional[str]]:
    wanted = set(str(lid) for lid in language_ids)
    results: dict[str, Optional[str]] = {}
    offset = 0
    while True:
        response = (
            client.table(TABLE_NAME_LANGUAGES)
            .select("language_id,language_en")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        if response.data is None:
            raise RuntimeError(f"Select failed: {response}")
        batch = response.data
        if not batch:
            break
        for row in batch:
            lid = row.get("language_id")
            if lid is None:
                continue
            lid_str = str(lid)
            if lid_str in wanted:
                results[lid_str] = row.get("language_en")
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return results


# Fetch country display names for provided country ids.
def _fetch_countries(client, country_ids: list[str]) -> dict[str, Optional[str]]:
    if not country_ids:
        return {}
    query = client.table(TABLE_NAME_COUNTRIES).select("country_id,country_en")
    if hasattr(query, "in_"):
        results: dict[str, Optional[str]] = {}
        for chunk in _chunked(country_ids, CHUNK_SIZE):
            response = query.in_("country_id", chunk).execute()
            if response.data is None:
                raise RuntimeError(f"Select failed: {response}")
            for row in response.data:
                cid = row.get("country_id")
                if cid:
                    results[str(cid)] = row.get("country_en")
        return results
    return _fetch_all_countries(client, country_ids)


# Fall back to a full scan when .in_ is unavailable.
def _fetch_all_countries(client, country_ids: list[str]) -> dict[str, Optional[str]]:
    wanted = set(str(cid) for cid in country_ids)
    results: dict[str, Optional[str]] = {}
    offset = 0
    while True:
        response = (
            client.table(TABLE_NAME_COUNTRIES)
            .select("country_id,country_en")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        if response.data is None:
            raise RuntimeError(f"Select failed: {response}")
        batch = response.data
        if not batch:
            break
        for row in batch:
            cid = row.get("country_id")
            if cid is None:
                continue
            cid_str = str(cid)
            if cid_str in wanted:
                results[cid_str] = row.get("country_en")
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return results


# Fetch cinema website urls for provided cinema names.
def _fetch_cinemas(client, cinema_names: list[str]) -> dict[str, Optional[str]]:
    if not cinema_names:
        return {}
    query = client.table(TABLE_NAME_CINEMAS).select("name_en,website_url")
    if hasattr(query, "in_"):
        results: dict[str, Optional[str]] = {}
        for chunk in _chunked(cinema_names, CHUNK_SIZE):
            response = query.in_("name_en", chunk).execute()
            if response.data is None:
                raise RuntimeError(f"Select failed: {response}")
            for row in response.data:
                name = row.get("name_en")
                if name:
                    results[name] = row.get("website_url")
        return results
    return _fetch_all_cinemas(client, cinema_names)


# Fall back to a full scan when .in_ is unavailable.
def _fetch_all_cinemas(client, cinema_names: list[str]) -> dict[str, Optional[str]]:
    wanted = set(cinema_names)
    results: dict[str, Optional[str]] = {}
    offset = 0
    while True:
        response = (
            client.table(TABLE_NAME_CINEMAS)
            .select("name_en,website_url")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        if response.data is None:
            raise RuntimeError(f"Select failed: {response}")
        batch = response.data
        if not batch:
            break
        for row in batch:
            name = row.get("name_en")
            if name in wanted:
                results[name] = row.get("website_url")
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return results


# Compose movie names based on translations and fallbacks.
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


# Compose and load screenings_serve for today's screenings.
def main() -> None:
    # Build today's date and connect to Supabase.
    today_str = _today_str()
    config = _load_config()
    client = create_client(config.url, config.key)

    # Fetch today's screenings payload.
    screenings = _fetch_screenings_for_date(client, today_str)

    # Collect unique titles and versions for lookup tables.
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

    # Resolve title links and version translations.
    title_links = _fetch_title_links(client, titles)
    versions_map = _fetch_versions(client, versions)

    # Resolve TMDB movie ids and fetch movie metadata.
    tmdb_ids = sorted(
        {
            tmdb_id
            for tmdb_id in title_links.values()
            if isinstance(tmdb_id, int)
        }
    )
    movies = _fetch_movies(client, tmdb_ids)

    # Collect ids for genre/language/country lookups.
    genre_ids: list[str] = []
    language_ids: list[str] = []
    country_ids: list[str] = []
    for movie in movies.values():
        genre_ids.extend(_parse_list_field(movie.get("genre_ids")))
        language_id = _clean_text(movie.get("original_language"))
        if language_id:
            language_ids.append(language_id)
        country_ids.extend(_parse_list_field(movie.get("origin_country")))

    # Resolve genre, language, and country display names.
    genres_map = _fetch_genres(client, sorted(set(genre_ids)))
    languages_map = _fetch_languages(client, sorted(set(language_ids)))
    countries_map = _fetch_countries(client, sorted(set(country_ids)))

    # Resolve cinema website urls by cinema name.
    cinemas_map = _fetch_cinemas(
        client,
        sorted({str(row.get("cinema_name")).strip() for row in screenings if row.get("cinema_name")}),
    )

    # Compose output rows for screenings_serve.
    composed_rows: list[dict] = []
    title_hits = 0
    version_hits = 0
    movie_hits = 0
    genre_hits = 0
    language_hits = 0
    country_hits = 0
    cinema_hits = 0
    for row in screenings:
        # Resolve the TMDB id for the current title.
        movie_name_raw = row.get("movie_name")
        tmdb_id = title_links.get(movie_name_raw)
        if tmdb_id is not None:
            title_hits += 1

        # Resolve the movie metadata record by tmdb id.
        movie = movies.get(tmdb_id) if isinstance(tmdb_id, int) else None
        if movie is not None:
            movie_hits += 1

        # Translate movie version if possible.
        raw_version = row.get("movie_version")
        mapped_version = versions_map.get(raw_version) if raw_version else None
        if mapped_version is not None:
            version_hits += 1
        final_version = mapped_version if mapped_version is not None else raw_version

        # Build enriched movie fields.
        genres_value = None
        orig_language_value = None
        orig_country_value = None
        tagline_en_value = None
        if movie:
            # Map genre ids to display names.
            genre_names = [
                genres_map.get(genre_id)
                for genre_id in _parse_list_field(movie.get("genre_ids"))
                if genres_map.get(genre_id)
            ]
            if genre_names:
                genres_value = ", ".join(genre_names)
                genre_hits += 1

            # Map original language to display name.
            language_id = _clean_text(movie.get("original_language"))
            if language_id:
                orig_language_value = languages_map.get(language_id)
                if orig_language_value:
                    language_hits += 1

            # Map origin countries to display names.
            country_names = [
                countries_map.get(country_id)
                for country_id in _parse_list_field(movie.get("origin_country"))
                if countries_map.get(country_id)
            ]
            if country_names:
                orig_country_value = ", ".join(country_names)
                country_hits += 1

            # Copy the remaining movie fields.
            tagline_en_value = _clean_text(movie.get("tagline_en"))

        # Resolve the cinema link from the cinema name.
        cinema_name = row.get("cinema_name")
        cinema_link_value = cinemas_map.get(cinema_name) if cinema_name else None
        if cinema_link_value:
            cinema_hits += 1

        # Compose final output row for screenings_serve.
        composed_rows.append(
            {
                "ingest_id": row.get("id"),
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
                "genres": genres_value,
                "orig_language": orig_language_value,
                "orig_country": orig_country_value,
                "cinema_link": cinema_link_value,
                "tagline_en": tagline_en_value,
            }
        )

    # Replace all rows with the newly composed payload.
    deleted_rows = _delete_all_rows(client, TABLE_NAME_SCREENINGS_SERVE)
    inserted_rows = _insert_rows(client, TABLE_NAME_SCREENINGS_SERVE, composed_rows)

    # Print summary stats for observability.
    print(
        "Summary:",
        f"date={today_str}",
        f"input_rows={len(screenings)}",
        f"composed_rows={len(composed_rows)}",
        f"title_hits={title_hits}",
        f"version_hits={version_hits}",
        f"movie_hits={movie_hits}",
        f"genre_hits={genre_hits}",
        f"language_hits={language_hits}",
        f"country_hits={country_hits}",
        f"cinema_hits={cinema_hits}",
        f"deleted={deleted_rows}",
        f"inserted={inserted_rows}",
    )


if __name__ == "__main__":
    main()
