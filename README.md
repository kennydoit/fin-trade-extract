lambda-functions/
├── common/                          # Shared utilities
│   ├── adaptive_rate_limiter.py     # Smart rate limiting
│   ├── parameter_store.py           # AWS SSM integration  
│   ├── s3_data_writer.py           # S3 output utilities
│   └── symbol_id_calculator.py     # Deterministic symbol IDs
├── overview-extractor/              # Overview extractor function
│   ├── lambda_function.py          # Main Lambda handler
│   ├── test-events.json            # Test event templates
│   └── README.md                   # Function documentation
├── requirements.txt                 # Python dependencies
└── __init__.py                     # Module initialization

deployment/                         # Deployment automation
├── deploy-overview-extractor.ps1   # PowerShell deployment script
├── lambda-execution-role-policy.json # IAM policy
├── lambda-trust-policy.json       # IAM trust relationship
└── README.md                      # Deployment guide
```
s3://fin-trade-craft-landing/
├── business_data/overview/YYYYMMDD_HHMMSS_runid.parquet
└── landing_data/overview/YYYYMMDD_HHMMSS_runid.json.gz
# fin-trade-extract

## Overview
This repository is now fully automated using GitHub Actions. All ETL steps—including fetching Alpha Vantage LISTING_STATUS, uploading to S3, and loading into Snowflake—are orchestrated by workflows in `.github/workflows/`.

## How It Works
- **GitHub Actions**: Triggers on schedule or manually to run the ETL pipeline.
- **Python Scripts**: Located in `scripts/github_actions/`, these handle API calls, S3 uploads, and Snowflake SQL execution.
- **No AWS Lambda**: All previous Lambda-based logic has been removed. No Lambda deployment or management is required.

## Key Files
- `.github/workflows/overview_full_universe.yml`: Main workflow for ETL automation.
- `scripts/github_actions/fetch_listing_status_to_s3.py`: Fetches LISTING_STATUS and uploads to S3.
- `scripts/github_actions/snowflake_run_sql_file.py`: Loads data into Snowflake.
- `snowflake/runbooks/`: Contains SQL runbooks for Snowflake loads.

## Setup
1. **Configure GitHub Secrets**: Add the following secrets to your repo:
   - `ALPHAVANTAGE_API_KEY`
   - `AWS_REGION`
   - `AWS_ROLE_TO_ASSUME`
   - `SNOWFLAKE_ACCOUNT`
   - `SNOWFLAKE_USER`
   - `SNOWFLAKE_PASSWORD`
   - `SNOWFLAKE_WAREHOUSE`
   - `SNOWFLAKE_DATABASE`
   - `SNOWFLAKE_SCHEMA`
2. **Edit workflow inputs** as needed in `.github/workflows/overview_full_universe.yml`.

## Running the Pipeline
- Trigger the workflow manually in GitHub or let it run on schedule.
- Monitor workflow runs in the GitHub Actions tab.

## Contributing
- All new ETL logic should be added as Python scripts and orchestrated via GitHub Actions.
- Lambda functions and related files are deprecated and should not be used.

---

For questions or help, open an issue in this repository.