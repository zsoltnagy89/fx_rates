import os
from dotenv import load_dotenv
import time
from datetime import date
import requests
import json
from pathlib import Path

# Load .env
load_dotenv()
access_key = os.getenv("API_KEY_FX")
if not access_key:
    raise ValueError("API_KEY_FX not found in .env")

print("API KEY loaded:", access_key is not None)

# Endpoint
endpoint = 'timeframe'

# Create JSON folder in projects root
script_dir = Path(__file__).parent.resolve()
json_dir = script_dir.parent / "jsons" / "fx_jsons"
json_dir.mkdir(parents=True, exist_ok=True)

# Define timeframe of interest
start_year = 1999
current_year = date.today().year

for year in range(start_year, current_year):
    print(f"Year {year} is started!")
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    response = requests.get(
        f'https://api.exchangerate.host/{endpoint}?access_key={access_key}&start_date={start_date}&end_date={end_date}'
    )
    data_tf = response.json()

    output_file = json_dir / f"usd_other_fx_rates_{year}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data_tf, f, indent=4, ensure_ascii=False)

    print(f"Year {year} is finished!")
    # Wait 0.5 sec to avoid rate limit
    time.sleep(0.5)
