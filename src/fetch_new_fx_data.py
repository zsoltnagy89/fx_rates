import os
from dotenv import load_dotenv
from datetime import date, timedelta
import requests
import json
from pathlib import Path
import pandas as pd

# Load .env
load_dotenv()
access_key = os.getenv("API_KEY_FX")
if not access_key:
    raise ValueError("API_KEY_FX not found in .env")

print("API KEY loaded:", access_key is not None)

# Endpoint
endpoint = 'timeframe'

# Path settings -- always resolve paths relative to this script
script_dir = Path(__file__).parent.resolve()
json_dir = script_dir.parent / "jsons" / "fx_jsons"
json_dir.mkdir(parents=True, exist_ok=True)

# Define timeframe: full year until yesterday
yesterday = date.today() - timedelta(days=1)
start_date = f"{yesterday.year}-01-01"
end_date = f"{yesterday}"

# Request historical FX timeframe data
response = requests.get(
    "https://api.exchangerate.host/" +
    endpoint +
    "?access_key=" + access_key +
    "&start_date=" + start_date +
    "&end_date=" + str(end_date)
)

data_tf = response.json()

# Save the downloaded JSON to the project's data folder
output_file = json_dir / f"usd_other_fx_rates_{yesterday.year}.json"
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(data_tf, f, indent=4, ensure_ascii=False)

# Reloading the saved JSON allows us to validate the file content and perform basic quality checks.
with open(output_file, "r", encoding="utf-8") as f:
    data = json.load(f)

# We transform the 'quotes' dictionary into a Pandas DataFrame to easily inspect time continuity.
quotes = data["quotes"]
df = pd.DataFrame.from_dict(quotes, orient="index")

# Quick QC
print("Starting date: ", df.index[0])
print("Ending date: ", df.index[-1])

# Calculate maximum date gap
delta_day = 0
for i in range(1, len(df.index)):
    diff = (pd.to_datetime(df.index[i]) - pd.to_datetime(df.index[i-1])).days
    delta_day = max(delta_day, diff)

print("Maximum difference between dates in dataframe:", delta_day)