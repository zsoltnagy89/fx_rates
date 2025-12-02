import os
from dotenv import load_dotenv
from datetime import date, timedelta
import requests
import json
from pathlib import Path
import pandas as pd


def load_api_key() -> str:
    """
    Load the FX API key from the .env file and validate that it exists.
    """
    load_dotenv()
    key = os.getenv("API_KEY_FX")
    if not key:
        raise ValueError("API_KEY_FX not found in .env")
    return key


def resolve_paths():
    """
    Return important project paths in a way that always works,
    regardless of where the script is executed from.
    """
    script_dir = Path(__file__).parent.resolve()
    json_dir = script_dir.parent / "jsons" / "fx_jsons"
    json_dir.mkdir(parents=True, exist_ok=True)
    return json_dir


def fetch_timeframe_data(api_key: str, start_date: str, end_date: str):
    """
    Perform a historical timeframe request to exchangerate.host.
    """
    endpoint = "timeframe"
    url = (
        f"https://api.exchangerate.host/{endpoint}"
        f"?access_key={api_key}&start_date={start_date}&end_date={end_date}"
    )
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def save_json(data: dict, output_path: Path):
    """
    Save the downloaded JSON into the designated output directory.
    """
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def qc_check(json_path: Path):
    """
    Basic QC check: validate date continuity inside the 'quotes' object.
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    quotes = data.get("quotes", {})
    df = pd.DataFrame.from_dict(quotes, orient="index")

    if df.empty:
        print("Warning: DataFrame is empty. No quotes found.")
        return

    print("Starting date:", df.index[0])
    print("Ending date:", df.index[-1])

    max_gap = 0
    for i in range(1, len(df.index)):
        diff = (pd.to_datetime(df.index[i]) - pd.to_datetime(df.index[i - 1])).days
        max_gap = max(max_gap, diff)

    print("Maximum date gap detected:", max_gap)


def main(do_qc: bool = True):
    """
    Main orchestrator function.
    Designed for cron, Airflow, Prefect, systemd timers, or manual CLI execution.
    """

    print("Starting FX daily ingestion pipeline...")

    # --- Load API key ---
    api_key = load_api_key()
    print("API key loaded successfully.")

    # --- Resolve paths ---
    json_dir = resolve_paths()

    # --- Define timeframe (full year up to yesterday) ---
    yesterday = date.today() - timedelta(days=1)
    start_date = f"{yesterday.year}-01-01"
    end_date = str(yesterday)

    print(f"Fetching exchange rates for: {start_date} → {end_date}")

    # --- Fetch data ---
    data = fetch_timeframe_data(api_key, start_date, end_date)

    # --- Save output ---
    output_file = json_dir / f"usd_other_fx_rates_{yesterday.year}.json"
    save_json(data, output_file)

    print(f"Saved JSON file to: {output_file}")

    # --- Optional QC ---
    if do_qc:
        print("Running QC check...")
        qc_check(output_file)

    print("FX ingestion completed successfully.")


if __name__ == "__main__":
    # Allow cron to call this file directly
    main()
