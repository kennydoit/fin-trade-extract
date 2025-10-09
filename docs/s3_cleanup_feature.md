# S3 Cleanup Feature Implementation

## Overview
Added robust S3 bucket cleanup functionality to all data extractors to prevent accumulation of old files and ensure clean extraction runs.

## Feature Details

### **Improved S3 Cleanup Method**
- **Handles Large File Counts**: Uses pagination to delete thousands of files (AWS limit: 1000 objects per delete request)
- **Batch Processing**: Processes deletions in batches for efficiency
- **Progress Tracking**: Shows deletion progress with running totals
- **Error Resilience**: Continues extraction even if cleanup fails
- **Complete Coverage**: Deletes ALL files in the specified S3 prefix

### **Implementation Across Extractors**

#### 1. **Company Overview Extractor** (Class-based)
- **File**: `scripts/github_actions/fetch_company_overview_bulk.py`
- **Method**: `cleanup_s3_bucket(self)`
- **S3 Prefix**: `company_overview/`
- **Called From**: `run_bulk_extraction()` method (first step)

#### 2. **Time Series Extractor** (Functional)
- **File**: `scripts/github_actions/fetch_time_series_bulk.py`
- **Function**: `cleanup_s3_bucket(bucket, s3_prefix, region)`
- **S3 Prefix**: `time_series/`
- **Called From**: `main()` function (before symbol processing)

#### 3. **Balance Sheet Extractor** (Class-based)
- **File**: `scripts/github_actions/fetch_balance_sheet_bulk.py`
- **Method**: `cleanup_s3_bucket(self)`
- **S3 Prefix**: `balance_sheet/`
- **Called From**: `run_bulk_extraction()` method (first step)

## Benefits

### **Cost Optimization**
- ✅ Prevents accumulation of thousands of old files
- ✅ Reduces S3 storage costs for trial/development environments
- ✅ Ensures clean state for each extraction run

### **Data Quality**
- ✅ Eliminates confusion from multiple versions of the same data
- ✅ Ensures Snowflake COPY operations process only current files
- ✅ Simplifies debugging and data validation

### **Operational Efficiency**
- ✅ Handles thousands of files without AWS API limits
- ✅ Shows clear progress during cleanup operations
- ✅ Graceful error handling - won't break extraction if cleanup fails
- ✅ Fast batch deletion (up to 1000 files per request)

## Usage

### **Automatic Activation**
All extractors now automatically clean their respective S3 prefixes before starting data extraction:

```bash
# Company Overview - cleans company_overview/ prefix
python scripts/github_actions/fetch_company_overview_bulk.py

# Time Series - cleans time_series/ prefix  
python scripts/github_actions/fetch_time_series_bulk.py

# Balance Sheet - cleans balance_sheet/ prefix
python scripts/github_actions/fetch_balance_sheet_bulk.py
```

### **Example Output**
```
🧹 Cleaning up S3 bucket before extraction...
🗑️ Deleting batch of 1000 files from S3...
✅ Deleted 1000 files (total deleted: 1000)
🗑️ Deleting batch of 1000 files from S3...
✅ Deleted 1000 files (total deleted: 2000)
🗑️ Deleting batch of 347 files from S3...
✅ Deleted 347 files (total deleted: 2347)
✅ Successfully deleted 2347 files from s3://fin-trade-craft-landing/company_overview/
```

## Configuration

### **Environment Variables**
Each extractor uses standard S3 configuration:
- `S3_BUCKET`: Target S3 bucket (default: `fin-trade-craft-landing`)
- `S3_*_PREFIX`: Data type specific prefix
- `AWS_REGION`: AWS region (default: `us-east-1`)

### **S3 Prefixes by Data Type**
- **Company Overview**: `company_overview/`
- **Time Series**: `time_series/`
- **Balance Sheet**: `balance_sheet/`

## Error Handling

### **Graceful Degradation**
- If cleanup fails, extraction continues with warning
- No extraction failure due to cleanup issues
- Clear error logging for troubleshooting

### **AWS Limits Respected**
- Pagination handles unlimited file counts
- Batch size respects 1000 object AWS limit
- Efficient API usage for large-scale cleanup

## Validation

### **Testing Completed**
- ✅ Company Overview extractor - confirmed working
- ✅ Time Series extractor - cleanup added
- ✅ Balance Sheet extractor - cleanup added

### **Expected Results**
- S3 buckets start clean for each extraction run
- No accumulation of old files
- Reduced S3 storage costs
- Cleaner Snowflake data loading process

## Maintenance

### **Future Considerations**
- Monitor S3 costs to validate storage savings
- Consider retention policies for production environments
- May want to add selective cleanup (e.g., keep last N days) for production
- Monitor cleanup performance for very large file counts (>10K files)