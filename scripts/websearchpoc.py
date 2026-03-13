"""
Web search POC for movie schedule lookup.

This file is intentionally simple and self-contained. It shows how to call the
OpenAI Responses API with the web_search tool enabled and a domain hint.
"""

import json
import os
from datetime import datetime
from pathlib import Path

from openai import OpenAI


def build_prompt() -> str:
    return """
Please search for all the movie screenings on a given day with these parameters:
day: 2026‑02‑28 (今天)
location: Taiwan, 高雄 (Kaohsiung)
cinema: 高雄夢時代秀泰影城
movie: '犯罪101' (Crime 101). 
Provide a list of showtimes along with the date, cinema name.
Only search within this page: https://www.atmovies.com.tw/showtime/t07707/a07/ as this page already shows the information for the right city, right cinema.
Additionally provide a link to the website where you found the showtimes and explain your path on the website step by step.
In case you cannot find the showtimes on the website, explain what was the reason for that.
Return the result as simple text in English in a concise manner. Do not use line breaks.
""".strip()


def main() -> None:
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    prompt = build_prompt()

    model = "gpt-5.1"
    tools = [
        {
            "type": "web_search",
             "filters": {"allowed_domains": ["atmovies.com.tw"]},
            "search_context_size": "medium",
        }
    ]

    timestamp = datetime.now().astimezone().isoformat()
    log_dir = Path(__file__).resolve().parent.parent / "logs"
    log_path = log_dir / "websearchpoc.log"

    # The web_search tool lets the model retrieve live web results.
    # The `allowed_domains` filter nudges it to prefer atmovies.com.tw.
    try:
        response = client.responses.create(
            model=model,
            input=prompt,
            tools=tools,
        )
        result_text = response.output_text
    except Exception as exc:  # noqa: BLE001
        result_text = f"ERROR: {exc}"
        raise
    finally:
        log_dir.mkdir(parents=True, exist_ok=True)
        metadata = json.dumps({"model": model, "tools": tools}, ensure_ascii=False)
        log_block = f"{timestamp}\n{prompt}\n{result_text}\n{metadata}\n\n"
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(log_block)

    print(result_text)


if __name__ == "__main__":
    main()
