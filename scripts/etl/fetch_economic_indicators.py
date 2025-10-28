"""
Fetch Economic Indicators from Alpha Vantage and upload to S3 (full-refresh, schema-driven)
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path
import pandas as pd
import requests
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
S3_BUCKET = os.getenv("S3_BUCKET") or "fin-trade-craft-landing"
S3_PREFIX = os.getenv("S3_ECONOMIC_INDICATORS_PREFIX", "economic_indicators/")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

BASE_URL = "https://www.alphavantage.co/query"
API_DELAY_SECONDS = 0.8

# Economic indicator configs (function_name, interval, display_name, maturity)
ECONOMIC_INDICATOR_CONFIGS = {
    "REAL_GDP": ("quarterly", "Real GDP", None),
    "REAL_GDP_PER_CAPITA": ("quarterly", "Real GDP Per Capita", None),
    "TREASURY_YIELD_10YEAR": ("daily", "Treasury Yield 10 Year", "10year"),
    "TREASURY_YIELD_3MONTH": ("daily", "Treasury Yield 3 Month", "3month"),
    "TREASURY_YIELD_2YEAR": ("daily", "Treasury Yield 2 Year", "2year"),
    "TREASURY_YIELD_5YEAR": ("daily", "Treasury Yield 5 Year", "5year"),
    "TREASURY_YIELD_7YEAR": ("daily", "Treasury Yield 7 Year", "7year"),
    "TREASURY_YIELD_30YEAR": ("daily", "Treasury Yield 30 Year", "30year"),
    "FEDERAL_FUNDS_RATE": ("daily", "Federal Funds Rate", None),
    "CPI": ("monthly", "Consumer Price Index", None),
    "INFLATION": ("monthly", "Inflation Rate", None),
    "RETAIL_SALES": ("monthly", "Retail Sales", None),
    "DURABLES": ("monthly", "Durable Goods Orders", None),
    "UNEMPLOYMENT": ("monthly", "Unemployment Rate", None),
    "NONFARM_PAYROLL": ("monthly", "Total Nonfarm Payroll", None),
}

# S3 upload helper
def upload_to_s3(csv_content, indicator_name, function_name, maturity, interval):
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    maturity_part = f"_{maturity}" if maturity else ""
    s3_key = f"{S3_PREFIX}{indicator_name}_{function_name}{maturity_part}_{interval}_{timestamp}.csv"
    s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=csv_content.encode("utf-8"), ContentType="text/csv")
    print(f"✅ Uploaded {indicator_name} to s3://{S3_BUCKET}/{s3_key}")
    return s3_key

def fetch_economic_indicator(function_key):
    interval, display_name, maturity = ECONOMIC_INDICATOR_CONFIGS[function_key]
    if function_key.startswith("TREASURY_YIELD_"):
        actual_function = "TREASURY_YIELD"
        params = {
            "function": actual_function,
            "interval": interval,
            "maturity": maturity,
            "datatype": "json",
            "apikey": API_KEY,
        }
    else:
        actual_function = function_key
        params = {
            "function": actual_function,
            "interval": interval,
            "datatype": "json",
            "apikey": API_KEY,
        }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()
    if "data" not in data or not data["data"]:
        print(f"No data for {display_name}")
        return None
    df = pd.DataFrame(data["data"])
    df["indicator_name"] = display_name
    df["function_name"] = actual_function
    df["maturity"] = maturity
    df["interval"] = interval
    df["name"] = data.get("name", display_name)
    df["unit"] = data.get("unit", "")
    df["run_id"] = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    # Convert value to float, handle NaN
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"]).dt.date
    # Reorder columns to match schema
    columns = [
        "indicator_name", "function_name", "maturity", "date", "interval", "unit", "value", "name", "run_id"
    ]
    df = df[columns]
    return df

def main():
    indicators = list(ECONOMIC_INDICATOR_CONFIGS.keys())
    for function_key in indicators:
        print(f"Fetching {function_key}...")
        df = fetch_economic_indicator(function_key)
        if df is not None and not df.empty:
            csv_content = df.to_csv(index=False)
            upload_to_s3(csv_content, df.iloc[0]["indicator_name"], df.iloc[0]["function_name"], df.iloc[0]["maturity"], df.iloc[0]["interval"])

if __name__ == "__main__":
    main()
