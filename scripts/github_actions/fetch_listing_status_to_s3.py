import os
import sys
import csv
import requests
import boto3
from datetime import datetime

def main():
    api_key = os.environ.get("ALPHAVANTAGE_API_KEY")
    bucket = os.environ.get("S3_BUCKET")
    s3_prefix = os.environ.get("S3_LISTING_STATUS_PREFIX", "listing_status/")
    region = os.environ.get("AWS_REGION", "us-east-1")
    if not api_key or not bucket:
        print("Missing ALPHAVANTAGE_API_KEY or S3_BUCKET env var", file=sys.stderr)
        sys.exit(2)

    url = f"https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={api_key}"
    print(f"Fetching LISTING_STATUS from {url}")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    content = resp.text
    # Validate CSV header
    lines = content.splitlines()
    if not lines or not lines[0].lower().startswith("symbol"):
        print("Unexpected response from Alpha Vantage", file=sys.stderr)
        sys.exit(3)

    today = datetime.utcnow().strftime("%Y%m%d")
    key = f"{s3_prefix}listing_status_{today}.csv"
    print(f"Uploading to s3://{bucket}/{key}")
    s3 = boto3.client("s3", region_name=region)
    s3.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))
    print("Upload complete.")

if __name__ == "__main__":
    main()