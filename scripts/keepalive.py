import os
from dataclasses import dataclass

from supabase import create_client


# check whether env variables exist

def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@dataclass(frozen=True)
class KeepaliveTarget:
    name: str
    supabase_url: str
    supabase_key: str
    table: str
    comment: str


def _build_targets() -> list[KeepaliveTarget]:
    return [
        KeepaliveTarget(
            name="Wenkaishui-frontend",
            supabase_url=_require_env("SUPABASE_URL"),
            supabase_key=_require_env("SUPABASE_SECRET_KEY"),
            table="integ_test_1",
            comment="autocreated by keepalive",
        ),
        KeepaliveTarget(
            name="Wenkaishui-MO",
            supabase_url=_require_env("SUPABASE_URL_MO"),
            supabase_key=_require_env("SUPABASE_SECRET_KEY_MO"),
            table="integ_test_1",
            comment="autocreated by keepalive",
        ),
    ]


def _insert_keepalive(target: KeepaliveTarget) -> None:
    client = create_client(target.supabase_url, target.supabase_key)
    response = client.table(target.table).insert({"comment": target.comment}).execute()

    if response.data is None:
        raise RuntimeError(f"Insert failed for {target.name}: {response}")

    inserted = response.data[0] if response.data else None
    print(f"Inserted row for {target.name}: {inserted}")


def main() -> None:
    targets = _build_targets()
    for target in targets:
        _insert_keepalive(target)
    print(f"Keepalive completed for {len(targets)} target(s).")


# so that only run main() if this file is executed directly
if __name__ == "__main__":
    main()
