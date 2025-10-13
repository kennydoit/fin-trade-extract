# Snowflake Naming Conventions

## Overview
This document defines the standardized naming conventions for all Snowflake objects in the FIN_TRADE_EXTRACT project. Consistent naming improves code maintainability, reduces confusion, and makes automation easier.

---

## General Principles

1. **UPPER_CASE with underscores** for all database objects (Snowflake standard)
2. **Descriptive and concise** - name should explain purpose
3. **Avoid abbreviations** unless industry-standard (e.g., ETL, API, S3)
4. **Singular nouns** for tables (e.g., `SYMBOL` not `SYMBOLS`)
5. **Prefix with scope** when necessary (e.g., `ETL_WATERMARKS` not just `WATERMARKS`)

---

## Database Structure

### Databases
```
FIN_TRADE_EXTRACT
```
- Pattern: `{PROJECT}_{PURPOSE}`
- Use underscores to separate words
- Describes the overall data domain

### Schemas
```
FIN_TRADE_EXTRACT.RAW
FIN_TRADE_EXTRACT.ANALYTICS  (future)
FIN_TRADE_EXTRACT.STAGING    (future)
```
- Pattern: `{LAYER}`
- Single word when possible
- Common layers: `RAW`, `STAGING`, `ANALYTICS`, `REPORTING`

---

## Tables

### Permanent Tables (Production Data)
```
SYMBOL
BALANCE_SHEET
CASH_FLOW
INCOME_STATEMENT
ETL_WATERMARKS
```

**Pattern:** `{ENTITY_NAME}`
- Singular noun (represents one row = one instance)
- Describes the business entity or fact
- No prefixes unless necessary for clarity
- Examples:
  - ✅ `BALANCE_SHEET` - Clear what it contains
  - ✅ `ETL_WATERMARKS` - Prefix clarifies it's metadata
  - ❌ `TBL_BALANCE_SHEET` - Redundant prefix
  - ❌ `BALANCE_SHEETS` - Use singular

### Transient Tables (ETL Staging)
```
BALANCE_SHEET_STAGING
CASH_FLOW_STAGING
INCOME_STATEMENT_STAGING
```

**Pattern:** `{TABLE_NAME}_STAGING`
- Base name matches the target permanent table
- Suffix `_STAGING` indicates temporary/intermediate data
- Used during ETL to stage data before MERGE
- Automatically dropped after ETL completes
- Examples:
  - ✅ `CASH_FLOW_STAGING` - Stages data for `CASH_FLOW` table
  - ❌ `CASH_FLOW_STAGE` - Conflicts with stage object naming
  - ❌ `TEMP_CASH_FLOW` - Less clear purpose

### Temporary Tables (Session-Scoped)
```
❌ AVOID in GitHub Actions workflows
```

**Why:** Temporary tables are session-scoped and may not persist across cursor executions when SQL is split by semicolons.

**Use instead:** TRANSIENT tables with fully qualified names

---

## Stages (External Data Sources)

### S3 Stages
```
BALANCE_SHEET_STAGE
CASH_FLOW_STAGE
INCOME_STATEMENT_STAGE
SYMBOL_STAGE
```

**Pattern:** `{TABLE_NAME}_STAGE`
- Base name matches the target table
- Suffix `_STAGE` indicates external stage pointing to S3
- One stage per data source/table
- Examples:
  - ✅ `CASH_FLOW_STAGE` → points to `s3://.../cash_flow/`
  - ❌ `S3_CASH_FLOW_STAGE` - Redundant prefix (all stages are external)
  - ❌ `CASH_FLOW_S3` - Less clear it's a stage object

### Stage Naming Mapping
| Stage Name | S3 Path | Target Table |
|------------|---------|--------------|
| `BALANCE_SHEET_STAGE` | `s3://fin-trade-craft-landing/balance_sheet/` | `BALANCE_SHEET` |
| `CASH_FLOW_STAGE` | `s3://fin-trade-craft-landing/cash_flow/` | `CASH_FLOW` |
| `INCOME_STATEMENT_STAGE` | `s3://fin-trade-craft-landing/income_statement/` | `INCOME_STATEMENT` |
| `SYMBOL_STAGE` | `s3://fin-trade-craft-landing/symbols/` | `SYMBOL` |

---

## Warehouses

### Compute Warehouses
```
FIN_TRADE_WH
FIN_TRADE_WH_LARGE  (future, for heavy queries)
```

**Pattern:** `{PROJECT}_WH[_{SIZE}]`
- Suffix `_WH` indicates warehouse
- Optional size suffix for multiple warehouses
- Examples:
  - ✅ `FIN_TRADE_WH` - Default warehouse
  - ✅ `FIN_TRADE_WH_LARGE` - For heavy analytics
  - ❌ `WAREHOUSE_1` - Not descriptive

---

## Roles

### Functional Roles
```
ETL_ROLE
ANALYST_ROLE
ADMIN_ROLE
```

**Pattern:** `{FUNCTION}_ROLE`
- Describes the function/purpose
- Suffix `_ROLE` for clarity
- Examples:
  - ✅ `ETL_ROLE` - For ETL processes
  - ✅ `ANALYST_ROLE` - For read-only analytics
  - ❌ `ROLE_ETL` - Inconsistent ordering

---

## Integration Objects

### Storage Integrations
```
FIN_TRADE_S3_INTEGRATION
```

**Pattern:** `{PROJECT}_{SYSTEM}_INTEGRATION`
- Describes what system is being integrated
- Suffix `_INTEGRATION` for clarity

---

## Columns

### Standard Column Patterns

#### Primary Keys
```sql
SYMBOL VARCHAR(20)              -- Natural key
SYMBOL_ID NUMBER(38,0)          -- Surrogate key (hash-based)
```

#### Foreign Keys
```sql
SYMBOL VARCHAR(20)              -- References SYMBOL.SYMBOL
SYMBOL_ID NUMBER(38,0)          -- References SYMBOL.SYMBOL_ID
```

#### Dates
```sql
FISCAL_DATE_ENDING DATE         -- Business date
LOAD_DATE DATE                  -- ETL metadata
DELISTING_DATE DATE             -- Business event date
```

#### Timestamps
```sql
LAST_SUCCESSFUL_RUN TIMESTAMP_NTZ  -- ETL metadata
CREATED_AT TIMESTAMP_NTZ           -- Audit trail
UPDATED_AT TIMESTAMP_NTZ           -- Audit trail
```

#### Status/Type Columns
```sql
STATUS VARCHAR(20)              -- e.g., 'Active', 'Inactive'
ASSET_TYPE VARCHAR(50)          -- e.g., 'Stock', 'ETF'
PERIOD_TYPE VARCHAR(20)         -- e.g., 'annual', 'quarterly'
API_ELIGIBLE VARCHAR(3)         -- e.g., 'YES', 'NO', 'DEL'
```

#### Metrics (Financial Data)
```sql
TOTAL_ASSETS NUMBER(20,2)
OPERATING_CASHFLOW NUMBER(38,2)
NET_INCOME NUMBER(20,2)
```
- Use descriptive names from source (Alpha Vantage API field names)
- Precision based on data size: (20,2) for most financials, (38,2) for very large values

---

## File Naming (SQL Scripts)

### Loader Scripts
```
load_balance_sheet_from_s3.sql
load_cash_flow_from_s3.sql
load_income_statement_from_s3.sql
```

**Pattern:** `load_{table_name}_from_{source}.sql`
- Lowercase with underscores
- Verb prefix describes action
- Clear source indication

### Diagnostic/Runbook Scripts
```
diagnose_balance_sheet_staleness.sql
verify_watermark_updates.sql
check_data_quality.sql
```

**Pattern:** `{verb}_{description}.sql`
- Lowercase with underscores
- Action verb (diagnose, verify, check, fix)
- Descriptive purpose

---

## S3 Path Naming

### S3 Bucket Structure
```
s3://fin-trade-craft-landing/
  ├── balance_sheet/
  ├── cash_flow/
  ├── income_statement/
  └── symbols/
```

**Pattern:** `{table_name}/`
- Lowercase with underscores
- Matches target table name (lowercase)
- One prefix per data type

---

## Naming Conflicts to Avoid

### ❌ Same Name for Stage and Staging Table
```sql
-- DON'T DO THIS:
CREATE STAGE CASH_FLOW_STAGE ...
CREATE TABLE CASH_FLOW_STAGE ...  -- Conflict!
```

### ✅ Correct Pattern
```sql
-- DO THIS:
CREATE STAGE CASH_FLOW_STAGE ...        -- External S3 stage
CREATE TABLE CASH_FLOW_STAGING ...      -- Transient staging table
CREATE TABLE CASH_FLOW ...              -- Final permanent table
```

---

## Summary Table

| Object Type | Pattern | Example | Notes |
|-------------|---------|---------|-------|
| **Database** | `{PROJECT}_{PURPOSE}` | `FIN_TRADE_EXTRACT` | All caps |
| **Schema** | `{LAYER}` | `RAW` | Single word |
| **Permanent Table** | `{ENTITY}` | `BALANCE_SHEET` | Singular noun |
| **Transient Table** | `{TABLE}_STAGING` | `CASH_FLOW_STAGING` | -ING suffix |
| **External Stage** | `{TABLE}_STAGE` | `CASH_FLOW_STAGE` | -E suffix |
| **Warehouse** | `{PROJECT}_WH` | `FIN_TRADE_WH` | WH suffix |
| **Role** | `{FUNCTION}_ROLE` | `ETL_ROLE` | ROLE suffix |
| **Integration** | `{PROJECT}_{SYSTEM}_INTEGRATION` | `FIN_TRADE_S3_INTEGRATION` | Full description |
| **SQL File** | `{verb}_{table}_{source}.sql` | `load_cash_flow_from_s3.sql` | Lowercase |
| **S3 Prefix** | `{table_name}/` | `cash_flow/` | Lowercase |

---

## Enforcement

### Code Review Checklist
- [ ] All table names are singular
- [ ] Staging tables use `_STAGING` suffix
- [ ] Stage objects use `_STAGE` suffix
- [ ] No name conflicts between stages and tables
- [ ] Columns follow standard patterns
- [ ] SQL files use lowercase with underscores
- [ ] Fully qualified names used in production code

### Automated Validation
Future: Add linting rules to detect naming convention violations

---

## References
- Snowflake Naming Best Practices: https://docs.snowflake.com/en/user-guide/object-identifiers
- Project-specific conventions documented here
- Updates require: Architecture review + documentation update
