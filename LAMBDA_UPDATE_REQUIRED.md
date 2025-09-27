# Lambda Function Update Required

Your Lambda function currently outputs Parquet files, but the Snowpipes are configured for CSV files.

## Required Changes:

1. **Change output format from Parquet to CSV**
2. **Update S3 key structure** to match the Snowpipe stages:
   - `s3://fin-trade-craft-landing/overview/`
   - `s3://fin-trade-craft-landing/time-series/`
   - `s3://fin-trade-craft-landing/income-statement/`
   - etc.

3. **Ensure proper CSV formatting** with headers matching table columns

## Example CSV Output Format:

For Overview data:
```csv
SYMBOL_ID,SYMBOL,ASSET_TYPE,NAME,DESCRIPTION,CIK,EXCHANGE,CURRENCY,COUNTRY,SECTOR,INDUSTRY,ADDRESS,OFFICIAL_SITE,FISCAL_YEAR_END,MARKET_CAPITALIZATION,EBITDA,PE_RATIO,PEG_RATIO,BOOK_VALUE,DIVIDEND_PER_SHARE,DIVIDEND_YIELD,EPS,REVENUE_PER_SHARE_TTM,PROFIT_MARGIN,OPERATING_MARGIN_TTM,RETURN_ON_ASSETS_TTM,RETURN_ON_EQUITY_TTM,REVENUE_TTM,GROSS_PROFIT_TTM,DILUTED_EPS_TTM,QUARTERLY_EARNINGS_GROWTH_YOY,QUARTERLY_REVENUE_GROWTH_YOY,ANALYST_TARGET_PRICE,TRAILING_PE,FORWARD_PE,PRICE_TO_SALES_RATIO_TTM,PRICE_TO_BOOK_RATIO,EV_TO_REVENUE,EV_TO_EBITDA,BETA,WEEK_52_HIGH,WEEK_52_LOW,DAY_50_MOVING_AVERAGE,DAY_200_MOVING_AVERAGE,SHARES_OUTSTANDING,DIVIDEND_DATE,EX_DIVIDEND_DATE,API_RESPONSE_STATUS
1,AAPL,Common Stock,Apple Inc,Apple Inc. designs...,320193,NASDAQ,USD,USA,Technology,Consumer Electronics,...
```

## File Naming Convention:
- Use timestamps: `overview_20250926_143052.csv`
- Include date/time for uniqueness
- Snowpipe will auto-detect new files

## Column Order Important:
CSV columns must match the table column order exactly since Snowpipes don't use column name matching.