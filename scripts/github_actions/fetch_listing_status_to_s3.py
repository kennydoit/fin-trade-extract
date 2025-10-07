import os
import sys
import csv
import requests
import boto3
import time
from datetime import datetime

def fetch_listing_status(api_key, state="active"):
    """
    Fetch listing status from Alpha Vantage API.
    
    Args:
        api_key: Alpha Vantage API key
        state: "active" for currently listed stocks, "delisted" for delisted stocks
    
    Returns:
        Response text content
    """
    base_url = "https://www.alphavantage.co/query"
    params = {
        "function": "LISTING_STATUS",
        "apikey": api_key
    }
    
    if state == "delisted":
        params["state"] = "delisted"
    
    url = f"{base_url}?" + "&".join([f"{k}={v}" for k, v in params.items()])
    print(f"🔄 Fetching {state.upper()} stocks from Alpha Vantage...")
    print(f"📡 URL: {url}")
    
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    content = resp.text
    
    # Validate CSV header
    lines = content.splitlines()
    if not lines or not lines[0].lower().startswith("symbol"):
        print(f"❌ Unexpected response from Alpha Vantage for {state} stocks", file=sys.stderr)
        print(f"Response preview: {content[:200]}...", file=sys.stderr)
        return None
    
    print(f"✅ Successfully fetched {len(lines)-1} {state} symbols")
    return content

def upload_to_s3(s3_client, bucket, key, content):
    """Upload content to S3."""
    print(f"📤 Uploading to s3://{bucket}/{key}")
    s3_client.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))
    print(f"✅ Upload complete: {len(content.splitlines())-1} records")

def main():
    api_key = os.environ.get("ALPHAVANTAGE_API_KEY")
    bucket = os.environ.get("S3_BUCKET")
    s3_prefix = os.environ.get("S3_LISTING_STATUS_PREFIX", "listing_status/")
    region = os.environ.get("AWS_REGION", "us-east-1")
    
    if not api_key or not bucket:
        print("❌ Missing ALPHAVANTAGE_API_KEY or S3_BUCKET env var", file=sys.stderr)
        sys.exit(2)

    s3 = boto3.client("s3", region_name=region)
    today = datetime.utcnow().strftime("%Y%m%d")
    
    # Track processing results
    results = {"active": False, "delisted": False}
    
    print("=" * 60)
    print("📋 LISTING STATUS ETL EXTRACTION")
    print("=" * 60)
    print(f"🗓️ Date: {today}")
    print(f"🪣 S3 Bucket: {bucket}")
    print(f"📁 S3 Prefix: {s3_prefix}")
    print("🔄 Fetching both active and delisted stocks automatically")
    
    # 1) Fetch ACTIVE (currently listed) stocks
    print("\n" + "=" * 40)
    print("📈 FETCHING ACTIVE STOCKS")
    print("=" * 40)
    
    try:
        active_content = fetch_listing_status(api_key, state="active")
        if active_content:
            key = f"{s3_prefix}listing_status_active_{today}.csv"
            upload_to_s3(s3, bucket, key, active_content)
            results["active"] = True
        else:
            print("❌ Failed to fetch active stocks")
    except Exception as e:
        print(f"❌ Error fetching active stocks: {e}", file=sys.stderr)
    
    # Rate limiting between API calls
    print("\n⏱️ Waiting 12 seconds between API calls (rate limiting)...")
    time.sleep(12)
    
    # 2) Fetch DELISTED stocks (Alpha Vantage returns all delisted stocks automatically)
    print("\n" + "=" * 40)
    print("📉 FETCHING DELISTED STOCKS")
    print("=" * 40)
    print("📋 Alpha Vantage will return all available delisted stocks")
    
    try:
        delisted_content = fetch_listing_status(api_key, state="delisted")
        if delisted_content:
            key = f"{s3_prefix}listing_status_delisted_{today}.csv"
            upload_to_s3(s3, bucket, key, delisted_content)
            results["delisted"] = True
        else:
            print("❌ Failed to fetch delisted stocks")
    except Exception as e:
        print(f"❌ Error fetching delisted stocks: {e}", file=sys.stderr)
    
    # Final summary
    print("\n" + "=" * 60)
    print("📊 LISTING STATUS EXTRACTION SUMMARY")
    print("=" * 60)
    print(f"📈 Active stocks: {'✅ SUCCESS' if results['active'] else '❌ FAILED'}")
    print(f"📉 Delisted stocks: {'✅ SUCCESS' if results['delisted'] else '❌ FAILED'}")
    
    # Exit with appropriate code
    if results["active"] and results["delisted"]:
        print("🎉 All extractions completed successfully!")
        sys.exit(0)
    elif results["active"] or results["delisted"]:
        print("⚠️ Partial success - some extractions failed")
        sys.exit(1)
    else:
        print("❌ All extractions failed")
        sys.exit(3)

if __name__ == "__main__":
    main()