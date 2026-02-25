import os
from datetime import datetime, timezone

from supabase import create_client

#. check whether env variables exist
def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def main() -> None:
    supabase_url = _require_env("SUPABASE_URL")
    supabase_key = _require_env("SUPABASE_SECRET_KEY")
    comment = os.getenv("COMMENT")

    if not comment: # default comment added if not provided
        now = datetime.now(timezone.utc).isoformat()
        comment = f"integ_test_insert at {now}"

    client = create_client(supabase_url, supabase_key)
    response = client.table("integ_test_1").insert({"comment": comment}).execute()

    if response.data is None:
        raise RuntimeError(f"Insert failed: {response}")

    inserted = response.data[0] if response.data else None
    print(f"Inserted row: {inserted}") #. prints so result visible in logs

#. so that only run main() if this file is executed directly
if __name__ == "__main__":
    main()
