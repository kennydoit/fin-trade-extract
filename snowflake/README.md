# Snowflake Configuration for fin-trade-extract Pipeline

This directory contains all Snowflake-related configuration for the Lambda → S3 → Snowpipe → Snowflake data pipeline.

## Structure

```
snowflake/
├── schema/
│   ├── 01_database_setup.sql          # Database, schema, warehouse creation
│   ├── 02_tables.sql                  # All table definitions
│   ├── 03_storage_integration.sql     # S3 integration setup
│   ├── 04_snowpipe.sql               # Automated data loading
│   └── 05_views.sql                  # Analytical views and transformations
├── maintenance/
│   ├── data_quality_checks.sql       # Data validation queries
│   └── performance_monitoring.sql    # Performance and cost monitoring
└── README.md                         # This file
```

## Setup Process

1. **Database Setup**: Create database, schemas, and compute resources
2. **Table Creation**: Define all Alpha Vantage data tables
3. **Storage Integration**: Configure S3 access from Snowflake
4. **Snowpipe Setup**: Configure automated data ingestion
5. **Views and Transformations**: Create analytical layers

## Cost Optimization

- **Warehouse Auto-Suspend**: 60 seconds idle time
- **Micro Warehouse**: X-Small (1 credit/hour) for batch processing
- **Snowpipe**: Pay-per-use serverless ingestion
- **Clustering**: Strategic clustering on high-volume tables
- **Data Retention**: 1-day time travel (minimum for cost optimization)

## Data Flow

```
Alpha Vantage API → Lambda Function → S3 (Parquet/JSON) → Snowpipe → Snowflake Tables → Views
```