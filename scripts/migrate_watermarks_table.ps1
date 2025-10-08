# ============================================================================
# ETL Watermarks Table Migration Script
# 
# This script runs the SQL migration to add fiscal date columns to ETL_WATERMARKS table.
# ============================================================================

Write-Host "🔧 Starting ETL_WATERMARKS table migration..." -ForegroundColor Cyan

# Check if snowsql is available
if (-not (Get-Command snowsql -ErrorAction SilentlyContinue)) {
    Write-Host "❌ snowsql not found. Please install SnowSQL CLI or run the SQL file manually in Snowflake." -ForegroundColor Red
    Write-Host "SQL file location: snowflake/migrations/add_fiscal_date_columns_to_watermarks.sql" -ForegroundColor Yellow
    exit 1
}

# Run the migration
try {
    Write-Host "📄 Executing migration SQL..." -ForegroundColor Yellow
    snowsql -c fin_trade_extract -f "snowflake/migrations/add_fiscal_date_columns_to_watermarks.sql"
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Migration completed successfully!" -ForegroundColor Green
        Write-Host "🎉 ETL_WATERMARKS table now supports fiscal date tracking and delisting intelligence!" -ForegroundColor Green
    } else {
        Write-Host "❌ Migration failed. Please check the output above for errors." -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "❌ Error running migration: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

Write-Host "`n📋 Next steps:" -ForegroundColor Cyan
Write-Host "1. Your company overview processing should now work without column errors" -ForegroundColor White
Write-Host "2. The enhanced watermarking system will track fiscal dates and delisting status" -ForegroundColor White
Write-Host "3. Future processing will benefit from intelligent delisted stock exclusion" -ForegroundColor White