# ============================================================================
# COMPLETE PIPELINE DEPLOYMENT: Deploy Watermarking and Analytics
# PowerShell script to deploy the enhanced data pipeline features
# ============================================================================

Write-Host "🚀 DEPLOYING ENHANCED FINANCIAL DATA PIPELINE" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green

# Step 1: First run the immediate fix to load all data
Write-Host "`n📋 STEP 1: Running immediate fix to load all 20 companies..." -ForegroundColor Yellow
Write-Host "Please run immediate-fix.sql in Snowflake Web UI first" -ForegroundColor Cyan
Write-Host "File contains SQL to load all companies and fix SYMBOL_ID format" -ForegroundColor White

Write-Host "`n⏸️  Press Enter after running immediate-fix.sql in Snowflake..." -ForegroundColor Yellow
Read-Host

# Step 2: Display watermarking deployment
Write-Host "`n📊 STEP 2: Deploy Watermarking Strategy..." -ForegroundColor Yellow
Write-Host "File: 05_watermarking_strategy.sql" -ForegroundColor Cyan
Write-Host "This adds:" -ForegroundColor White
Write-Host "  ✅ Timestamp and batch tracking columns" -ForegroundColor Green
Write-Host "  ✅ Watermark control table" -ForegroundColor Green  
Write-Host "  ✅ Enhanced loading procedures" -ForegroundColor Green
Write-Host "  ✅ Change detection views" -ForegroundColor Green
Write-Host "  ✅ Data freshness monitoring" -ForegroundColor Green

# Step 3: Display analytics deployment  
Write-Host "`n📈 STEP 3: Deploy Analytics Views..." -ForegroundColor Yellow
Write-Host "File: 06_analytics_views.sql" -ForegroundColor Cyan
Write-Host "This creates:" -ForegroundColor White
Write-Host "  ✅ Current financial snapshot view" -ForegroundColor Green
Write-Host "  ✅ Market cap rankings and classifications" -ForegroundColor Green
Write-Host "  ✅ Sector performance analysis" -ForegroundColor Green
Write-Host "  ✅ Investment screening views (dividend, growth, value stocks)" -ForegroundColor Green
Write-Host "  ✅ Business KPI summary dashboard" -ForegroundColor Green

# Step 4: Display alerting deployment
Write-Host "`n🚨 STEP 4: Deploy Alerting System..." -ForegroundColor Yellow  
Write-Host "File: 07_alerting_system.sql" -ForegroundColor Cyan
Write-Host "This provides:" -ForegroundColor White
Write-Host "  ✅ Alert configuration management" -ForegroundColor Green
Write-Host "  ✅ Automated freshness alerts" -ForegroundColor Green
Write-Host "  ✅ Data quality monitoring" -ForegroundColor Green
Write-Host "  ✅ Volume anomaly detection" -ForegroundColor Green
Write-Host "  ✅ Health dashboard and statistics" -ForegroundColor Green

# Display next steps
Write-Host "`n🎯 NEXT STEPS:" -ForegroundColor Magenta
Write-Host "=============" -ForegroundColor Magenta
Write-Host ""

Write-Host "1. 📁 Deploy Files in Snowflake (in this order):" -ForegroundColor Cyan
Write-Host "   • 05_watermarking_strategy.sql" -ForegroundColor White
Write-Host "   • 06_analytics_views.sql" -ForegroundColor White  
Write-Host "   • 07_alerting_system.sql" -ForegroundColor White

Write-Host "`n2. 🧪 Test the Enhanced Pipeline:" -ForegroundColor Cyan
Write-Host "   • CALL LOAD_OVERVIEW_WITH_WATERMARKS();" -ForegroundColor White
Write-Host "   • CALL RUN_ALL_ALERT_CHECKS();" -ForegroundColor White
Write-Host "   • SELECT * FROM CURRENT_FINANCIAL_SNAPSHOT LIMIT 10;" -ForegroundColor White

Write-Host "`n3. 📊 Explore Analytics Views:" -ForegroundColor Cyan
Write-Host "   • MARKET_CAP_RANKINGS - See company rankings" -ForegroundColor White
Write-Host "   • SECTOR_PERFORMANCE - Analyze sectors" -ForegroundColor White  
Write-Host "   • HIGH_QUALITY_DIVIDEND_STOCKS - Find dividend opportunities" -ForegroundColor White
Write-Host "   • BUSINESS_KPI_SUMMARY - Overall portfolio metrics" -ForegroundColor White

Write-Host "`n4. 🔍 Monitor System Health:" -ForegroundColor Cyan
Write-Host "   • SYSTEM_HEALTH_OVERVIEW - Overall pipeline status" -ForegroundColor White
Write-Host "   • ACTIVE_ALERTS_DASHBOARD - Current alerts" -ForegroundColor White
Write-Host "   • DATA_QUALITY_DASHBOARD - Data quality metrics" -ForegroundColor White

Write-Host "`n5. 🔄 Set Up Scheduling:" -ForegroundColor Cyan  
Write-Host "   • Create Snowflake tasks to run alert checks hourly" -ForegroundColor White
Write-Host "   • Schedule Lambda to run daily for fresh data" -ForegroundColor White
Write-Host "   • Set up email notifications from alert system" -ForegroundColor White

Write-Host "`n📋 DEPLOYMENT SUMMARY:" -ForegroundColor Green
Write-Host "======================" -ForegroundColor Green
Write-Host "✅ Immediate Fix: Load all 20 companies" -ForegroundColor Green
Write-Host "✅ Watermarking: Track data lineage and changes" -ForegroundColor Green  
Write-Host "✅ Analytics: Business-ready investment insights" -ForegroundColor Green
Write-Host "✅ Alerting: Automated monitoring and notifications" -ForegroundColor Green
Write-Host "✅ Quality: Data completeness and freshness tracking" -ForegroundColor Green

Write-Host "`n🎉 Your enterprise-grade financial data pipeline is ready!" -ForegroundColor Green
Write-Host "You now have a complete serverless data architecture from Lambda → S3 → Snowflake with:" -ForegroundColor White
Write-Host "• Real-time data ingestion via Snowpipe" -ForegroundColor Gray
Write-Host "• Comprehensive watermarking and change tracking" -ForegroundColor Gray  
Write-Host "• Investment screening and portfolio analytics" -ForegroundColor Gray
Write-Host "• Automated monitoring and alerting" -ForegroundColor Gray
Write-Host "• Data quality and freshness validation" -ForegroundColor Gray