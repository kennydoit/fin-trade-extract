# Fin-Trade-Craft Data Pipeline: AWS → Snowflake

## Architecture Overview

**Goal:** Pull stock market data from Alpha Vantage (20k API calls/day) into Snowflake with minimal infrastructure and cost.

**Flow:**
1. **EventBridge (AWS)** triggers Lambda on a schedule (e.g., 18 runs/day).
2. **Lambda (AWS)** batches API calls (~1,000 per run), writes compressed Parquet files to S3.
3. **S3 (AWS)** acts as a transient landing zone (files expire after 7–30 days).
4. **Snowpipe (Snowflake)** auto-ingests new S3 objects into Snowflake tables.
5. **Snowflake** becomes the system of record for analytics.

---

## Design Principles

- **Batching:** Aggregate many API calls into a single Parquet file (target a few MB–100 MB per object).
- **Lifecycle management:** S3 lifecycle rules delete old landing files (keeps cost near zero).
- **Auto-ingest:** Snowpipe removes the need to run compute warehouses for loading.
- **Least-privilege IAM:** Lambda has only S3 write + CloudWatch + Parameter Store read permissions.

---

## AWS Components

- **Lambda**
  - Batches Alpha Vantage API calls.
  - Config: 512 MB memory, 15 min timeout, ~18 runs/day.
  - Secrets stored via SSM Parameter Store or env vars.
- **S3**
  - Landing bucket for Parquet files.
  - Lifecycle rule deletes files after 7–30 days.
- **EventBridge**
  - Cron triggers Lambda.
- **IAM**
  - Execution role for Lambda with least-privilege policies.

---

## Snowflake Components

- **STORAGE INTEGRATION**
  - Provides secure read-only access to S3 bucket.
- **STAGE + FILE FORMAT (PARQUET)**
  - Defines external file location and format.
- **PIPE (AUTO_INGEST=TRUE)**
  - Links S3 event notifications → Snowflake-managed SQS → table ingestion.
- **Tables**
  - Internal Snowflake tables for each dataset (OHLCV, fundamentals, news, etc.).

---

## Cost Breakdown

### AWS
- **Lambda**
  - ~18 runs/day × 15 min/run × 512 MB = 270k GB-seconds/month.
  - Free tier covers 400k GB-seconds → **$0/mo** (possibly ~$2 if memory doubled).
- **S3**
  - Storage: transient (<100 GB × $0.023/GB-month) → **pennies**.
  - Requests: negligible.
- **Secrets Manager (optional)**
  - $0.40/secret/month (avoid if using Parameter Store).
- **Parameter Store (preferred)**
  - Free (small KMS decrypt cost per request, pennies).

**AWS subtotal:** **$0–$3/month**

---

### Snowflake (Standard Edition)
- **Storage**
  - $23/TB-month → 100 GB ≈ **$2.30/month**.
- **Snowpipe ingestion**
  - Batching to ~1,500 files/month.
  - 0.06 credits per 1,000 files = 0.09 credits.
  - At $2/credit = **$0.18/month**.
  - Even with 30k files/month = ~**$3.60/month**.
- **Warehouses**
  - **$0 for loading** (Snowpipe handles ingest).
  - Only pay for queries/transformations (XS = 1 credit/hr, billed per-second).

**Snowflake subtotal:** **$2–$5/month**

---

## Total Cost (excluding Alpha Vantage subscription)

- **AWS:** $0–$3/month  
- **Snowflake:** $2–$5/month  
- **Combined:** **~$3–$8/month**

---

## Key Notes

- Alpha Vantage Premium (75 calls/min) is already covered by your subscription plan.
- Real cost drivers are Snowflake storage (tiny at 100 GB) and Snowpipe (file-count dependent).
- Keep file counts low (batching) to avoid unnecessary Snowpipe credits.
- Monitoring:
  - AWS: CloudWatch Logs for Lambda, S3 metrics.
  - Snowflake: `PIPE_USAGE_HISTORY`, `LOAD_HISTORY`, `STORAGE_USAGE`.

---
