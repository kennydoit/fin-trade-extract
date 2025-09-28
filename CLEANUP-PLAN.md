# Repository Cleanup Plan

## 📁 **KEEP - Essential Production Files**

### PowerShell Tools (Production)
- `simple-toolkit.ps1` - PRIMARY production tool (ASCII-only, tested working)
- `load-extended-companies.ps1` - Sector-based mass loading (successful)

### SQL Solutions (Final Working)
- `staging-table-approach.sql` - FINAL successful data loading solution
- `snowflake/schema/` - Core schema files (keep entire directory)

### Documentation
- `LOCAL-SETUP-GUIDE.md` - Setup instructions
- `OPTIMIZATION-COMPLETE.md` - Success summary
- `fin-trade-craft-files/` - Original extractor programs (keep entire directory)

### Core Infrastructure
- `lambda-package/` - Lambda function code (keep entire directory)
- `deployment/` - Deployment configs (keep entire directory)

## 🗑️ **REMOVE - Experimental/Duplicate Files**

### Duplicate PowerShell Tools
- `enhanced-load-data.ps1` - Superseded by simple-toolkit.ps1
- `lambda-toolkit.ps1` - Experimental version
- `bulk-load-overview.ps1` - Early version
- `load-overview-data.ps1` - Older version
- `quick-bulk-load.ps1` - Experimental
- `direct-load.ps1` - Basic version
- `simple-load.ps1` - Early version
- `simple-load-start.ps1` - Starter version
- `monitor-pipeline.ps1` - Diagnostic tool no longer needed
- `diagnose-lambda-issues.ps1` - Debug tool no longer needed

### Deployment Tools (Experimental)
- `deploy-enhanced-pipeline.ps1` - Experimental
- `deploy-guide.ps1` - Basic version
- `deploy-lambda-with-deps.ps1` - Experimental
- `deploy-no-pandas.ps1` - Test version
- `redeploy-lambda.ps1` - Basic redeploy
- `simple-redeploy.ps1` - Minimal version
- `setup-snowflake-pipeline.ps1` - Original version
- `setup-snowflake-pipeline-fixed.ps1` - Fixed version (keep one)

### Test/Debug Tools
- `test-api-key.ps1` - One-time test
- `fix-lambda-api-key.ps1` - One-time fix

### SQL Files (Experimental/Failed Attempts)
- `align-with-original-schema.sql` - Intermediate attempt (superseded by staging-table-approach.sql)
- `add-all-missing-columns.sql` - Failed approach
- `fix-column-mapping.sql` - Failed approach
- `fix-missing-column.sql` - Intermediate attempt
- `fix-snowflake-ingestion.sql` - Early attempt
- `fix-stage-ingestion.sql` - Intermediate attempt
- `load-specific-files.sql` - Failed approach
- `diagnose-copy-issues.sql` - Debug file
- `debug-table-csv-mismatch.sql` - Debug file
- `debug-loading-issues.sql` - Debug file
- `debug-snowflake-pipeline.sql` - Debug file
- `create-s3-stage.sql` - Intermediate fix
- `check-table-structure.sql` - Debug query
- `check-column-names.sql` - Debug query

### All Test JSON Files (25 files)
- All `*.json` files in root directory - These are test payloads and responses

### Experimental SQL Files
- `cleanup-*.sql` - One-time cleanup scripts
- `fix-*.sql` - Intermediate fix attempts
- `immediate-fix.sql` - One-time fix
- `enterprise-overview-pipeline.sql` - Complex attempt
- `snowflake-pipeline-diagnostics.sql` - Debug script
- `update-snowpipes-staging.sql` - Intermediate attempt
- `verify-snowflake-data.sql` - Verification script
- `monitor-overview-loading.sql` - Monitoring script

### Documentation (Intermediate)
- `SNOWFLAKE-INGESTION-FIX.md` - Intermediate docs
- `SNOWFLAKE_CSV_SETUP_GUIDE.md` - Setup guide (superseded)

## 📊 **Summary**
- **Keep**: ~15 essential files + core directories
- **Remove**: ~60+ experimental/duplicate files
- **Space saved**: Significant cleanup while preserving all working solutions