import os
import time
import json
from datetime import date
from pathlib import Path

import requests
from dotenv import load_dotenv


def load_api_key() -> str:
    """Load the API key from the .env file and return it."""
    load_dotenv()
    key = os.getenv("API_KEY_FX")
    if not key:
        raise ValueError("API_KEY_FX not found in .env")
    return key


def get_output_folder() -> Path:
    """
    Returns the absolute path of the json output folder.
    Ensures it exists and is created relative to the project root.
    """
    script_dir = Path(__file__).parent.resolve()
    json_dir = script_dir.parent / "jsons" / "fx_jsons"
    json_dir.mkdir(parents=True, exist_ok=True)
    return json_dir


def fetch_year_data(year: int, access_key: str) -> dict:
    """
    Request full-year FX timeframe data for the given year
    and return the parsed JSON response.
    """
    endpoint = "timeframe"
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    url = (
        f"https://api.exchangerate.host/{endpoint}"
        f"?access_key={access_key}"
        f"&start_date={start_date}"
        f"&end_date={end_date}"
    )

    response = requests.get(url)
    response.raise_for_status()  # cron-friendly: fail job if API fails
    return response.json()


def save_json(data: dict, filepath: Path) -> None:
    """Save a JSON dictionary to disk with formatting."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def run_historical_backfill():
    """
    Fetch historical FX data from 1999 until last completed year.
    This function is suitable for cron execution or one-off backfills.
    """
    access_key = load_api_key()
    json_dir = get_output_folder()

    start_year = 1999
    current_year = date.today().year

    print("Starting historical FX data backfill...")

    for year in range(start_year, current_year):
        print(f"[{year}] Downloading...")
        data = fetch_year_data(year, access_key)

        output_file = json_dir / f"usd_other_fx_rates_{year}.json"
        save_json(data, output_file)

        print(f"[{year}] Saved → {output_file.name}")
        time.sleep(0.5)  # Rate-limit friendly

    print("Historical backfill completed.")


if __name__ == "__main__":
    run_historical_backfill()
