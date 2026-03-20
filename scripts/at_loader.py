#!/usr/bin/env python3
"""
Load today's Scraped_YYYY-MM-DD.json into Supabase screenings_mini table.

Behavior:
- Uses Taiwan timezone to pick today's file.
- Performs a full replace: deletes all rows, then inserts new rows.
"""

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from supabase import create_client

from dotenv import load_dotenv

# Load local env vars without overriding existing (e.g., GitHub Actions) values.
load_dotenv(override=False)

TIMEZONE = ZoneInfo("Asia/Taipei")
TABLE_NAME = "screenings_mini"
ALL_ROWS_SENTINEL = "00000000-0000-0000-0000-000000000000"

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


# Delete all rows from the target table.
def _delete_all_rows(client) -> int:
    response = client.table(TABLE_NAME).delete().neq("id", ALL_ROWS_SENTINEL).execute()
    if response.data is None:
        raise RuntimeError(f"Delete failed: {response}")
    return len(response.data)


# Insert all rows into the target table.
def _insert_rows(client, rows: list[dict]) -> int:
    if not rows:
        return 0
    response = client.table(TABLE_NAME).insert(rows).execute()
    if response.data is None:
        raise RuntimeError(f"Insert failed: {response}")
    return len(response.data)


# Orchestrate load for today's JSON file.
def main() -> None:
    temp_dir = Path(__file__).resolve().parents[1] / "temp"
    today_str = _today_str()
    json_path = temp_dir / f"Scraped_{today_str}.json"

    if not json_path.exists():
        raise RuntimeError(f"Missing JSON file: {json_path}")

    rows = _load_rows(json_path)
    config = _load_config()
    client = create_client(config.url, config.key)

    deleted = _delete_all_rows(client)
    inserted = _insert_rows(client, rows)

    print(
        "Summary:",
        f"date={today_str}",
        f"input_rows={len(rows)}",
        f"deleted={deleted}",
        f"inserted={inserted}",
        f"table={TABLE_NAME}",
    )


if __name__ == "__main__":
    main()
