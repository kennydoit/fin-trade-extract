import argparse
import os
import sys
import time
import json
import csv
from datetime import datetime

import boto3
import requests


LISTING_STATUS_URL = "https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={apikey}"


def fetch_full_universe(api_key: str):
    url = LISTING_STATUS_URL.format(apikey=api_key)
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    content = resp.text
    rows = list(csv.DictReader(content.splitlines()))
    # Filter stocks and non-empty symbols
    symbols = sorted({r["symbol"] for r in rows if r.get("symbol") and ("stock" in (r.get("assetType") or "").lower())})
    return symbols


def batched(seq, size):
    batch = []
    for item in seq:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--function-name", required=True)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    args = parser.parse_args()

    api_key = os.environ.get("ALPHAVANTAGE_API_KEY")
    if not api_key:
        print("ALPHAVANTAGE_API_KEY is not set", file=sys.stderr)
        sys.exit(2)

    symbols = fetch_full_universe(api_key)
    print(f"Fetched {len(symbols)} symbols (stocks)")

    client = boto3.client("lambda")
    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    total_processed = 0
    for i, chunk in enumerate(batched(symbols, args.batch_size), start=1):
        payload = {
            "request_id": f"gh-actions-{run_id}-{i}",
            "symbols": chunk,
            "options": {"run_id": run_id, "orchestrator": "github-actions"},
        }
        print(f"Invoking {args.function_name} batch #{i} size={len(chunk)}")
        resp = client.invoke(
            FunctionName=args.function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )
        body = resp["Payload"].read()
        try:
            data = json.loads(body)
        except Exception:
            data = {}
        symbols_processed = data.get("symbols_processed") or chunk
        success_count = data.get("success_count", len(symbols_processed))
        total_processed += len(symbols_processed)
        print(f"Batch #{i}: processed={len(symbols_processed)} success={success_count} total={total_processed}")
        time.sleep(args.sleep_seconds)

    print(f"Completed. Total processed: {total_processed}")


if __name__ == "__main__":
    main()
