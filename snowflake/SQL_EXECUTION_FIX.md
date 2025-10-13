# SQL Execution Bug Fix - TEMPORARY vs TRANSIENT Tables

## 🐛 Problem Encountered

When running `load_cash_flow_from_s3.sql` through the GitHub Actions workflow, you saw this error:

```
Error executing statement: 002003 (42S02): SQL compilation error:
Object 'CASH_FLOW_STAGE' does not exist or not authorized.
```

This occurred on the data quality check statements AFTER the COPY INTO completed successfully.

## 🔍 Root Cause Analysis

### The Issue
The SQL file originally used:
```sql
CREATE OR REPLACE TEMPORARY TABLE CASH_FLOW_STAGE (...)
```

**TEMPORARY tables** in Snowflake:
- Exist only within the **current session**
- Are dropped when the session ends OR when creating a new cursor
- Are **session-scoped**, not connection-scoped

### The GitHub Actions Execution Flow
The `snowflake_run_sql_file.py` helper script:
1. Splits SQL file by semicolons into separate statements
2. Executes each statement using `cursor.execute(statement)`
3. Creates a **new cursor** for each statement in some cases

**Result:** The temporary table created in one statement execution doesn't exist when the next statement tries to query it!

### Why It Worked for Balance Sheet
The balance sheet SQL used:
```sql
CREATE OR REPLACE TRANSIENT TABLE FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET_STAGING (...)
```

**TRANSIENT tables**:
- Persist across cursor executions within the same connection
- Are **connection-scoped**, not session-scoped
- Survive until explicitly dropped or connection closes
- Still offer performance benefits (no Fail-safe period = lower costs)

## ✅ Solution Applied

Changed cash flow SQL from TEMPORARY to TRANSIENT:

```sql
-- OLD (BROKEN)
CREATE OR REPLACE TEMPORARY TABLE CASH_FLOW_STAGE (...)

-- NEW (FIXED)
CREATE OR REPLACE TRANSIENT TABLE FIN_TRADE_EXTRACT.RAW.CASH_FLOW_STAGE (...)
```

### Additional Improvements
Also added **fully qualified table names** throughout for clarity:
```sql
-- All references now use full path
FROM FIN_TRADE_EXTRACT.RAW.CASH_FLOW_STAGE
COPY INTO FIN_TRADE_EXTRACT.RAW.CASH_FLOW_STAGE
DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.CASH_FLOW_STAGE
```

## 📊 When to Use Each Table Type

| Table Type | Scope | Use Case | Fail-safe | Cost |
|------------|-------|----------|-----------|------|
| **PERMANENT** | Database | Production data, long-term storage | 7 days | Highest |
| **TRANSIENT** | Database | Staging, ETL intermediate tables | 0 days | Medium |
| **TEMPORARY** | Session | Single-query intermediate results | 0 days | Lowest |

### Our ETL Pattern
- **PERMANENT**: `CASH_FLOW`, `BALANCE_SHEET` (final destination tables)
- **TRANSIENT**: `CASH_FLOW_STAGE`, `BALANCE_SHEET_STAGING` (ETL staging tables)
- **TEMPORARY**: Not used in GitHub Actions workflows (session issues)

## 🎯 Best Practices for GitHub Actions SQL

1. **Never use TEMPORARY tables** in SQL files executed via `snowflake_run_sql_file.py`
2. **Always use TRANSIENT** for staging/intermediate tables in ETL
3. **Use fully qualified names** for clarity and debugging
4. **Drop staging tables** at the end to clean up (happens automatically on connection close anyway)

## 🔧 Testing Verification

After this fix, the workflow should:
1. ✅ Create CASH_FLOW_STAGE as TRANSIENT table
2. ✅ Load data via COPY INTO
3. ✅ Run data quality checks successfully
4. ✅ Execute MERGE into target table
5. ✅ Drop staging table
6. ✅ Complete without errors

## 📚 Related Documentation
- Snowflake Table Types: https://docs.snowflake.com/en/user-guide/tables-temp-transient
- Fail-safe & Time Travel: https://docs.snowflake.com/en/user-guide/data-time-travel
- Our implementation: `snowflake/load_cash_flow_from_s3.sql`
