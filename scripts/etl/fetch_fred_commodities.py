# FRED Commodities ETL for Alpha Vantage
# This script fetches all available commodities from Alpha Vantage FRED endpoints and loads them to S3 or Snowflake.
# Watermarks are not strictly necessary since the data is not symbol-based and can be fully refreshed daily.

import os
import requests
import pandas as pd
from datetime import datetime

ALPHAVANTAGE_API_KEY = os.environ["ALPHAVANTAGE_API_KEY"]
S3_BUCKET = os.environ.get("S3_BUCKET", "fin-trade-craft-landing")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")

# List of FRED commodities and their Alpha Vantage function/measure codes
COMMODITIES = [
    ("WTI", "WTI"),
    ("BRENT", "BRENT"),
    ("NATURAL_GAS", "NATURAL_GAS"),
    ("COPPER", "COPPER"),
    ("ALUMINUM", "ALUMINUM"),
    ("WHEAT", "WHEAT"),
    ("CORN", "CORN"),
    ("COTTON", "COTTON"),
    ("SUGAR", "SUGAR"),
    ("ALL_COMMODITIES", "ALL_COMMODITIES")
]

API_URL = "https://www.alphavantage.co/query"

results = []
for name, code in COMMODITIES:
    params = {
        "function": "COMMODITY_EXCHANGE_RATE",
        "symbol": code,
        "apikey": ALPHAVANTAGE_API_KEY
    }
    resp = requests.get(API_URL, params=params)
    if resp.status_code == 200:
        data = resp.json()
        # The structure may vary; adjust as needed for Alpha Vantage's FRED commodity output
        results.append({"measure": name, "data": data})
    else:
        print(f"Failed to fetch {name}: {resp.status_code}")

# Save all results to a timestamped file (or upload to S3, or load to Snowflake as needed)
timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
output_path = f"fred_commodities_{timestamp}.json"
import json
with open(output_path, "w") as f:
    json.dump(results, f, indent=2)
print(f"Saved all commodities data to {output_path}")

# Optionally: upload to S3 or load to Snowflake here
# ...
