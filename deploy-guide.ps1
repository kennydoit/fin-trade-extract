# Enhanced Financial Data Pipeline Deployment Guide
# Run this to get instructions for deploying watermarking and analytics

Write-Host "Enhanced Financial Data Pipeline Deployment" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green

Write-Host "`nStep 1: Load All Companies (immediate-fix.sql)" -ForegroundColor Yellow
Write-Host "Run immediate-fix.sql in Snowflake to load all 20 companies" -ForegroundColor White

Write-Host "`nStep 2: Deploy Watermarking (05_watermarking_strategy.sql)" -ForegroundColor Yellow  
Write-Host "Adds data tracking and change detection capabilities" -ForegroundColor White

Write-Host "`nStep 3: Deploy Analytics (06_analytics_views.sql)" -ForegroundColor Yellow
Write-Host "Creates business-ready investment screening views" -ForegroundColor White

Write-Host "`nStep 4: Deploy Alerting (07_alerting_system.sql)" -ForegroundColor Yellow
Write-Host "Enables automated monitoring and notifications" -ForegroundColor White

Write-Host "`nNext Actions:" -ForegroundColor Cyan
Write-Host "1. Run immediate-fix.sql in Snowflake first" -ForegroundColor White
Write-Host "2. Then run the other SQL files in order" -ForegroundColor White
Write-Host "3. Test with: CALL RUN_ALL_ALERT_CHECKS();" -ForegroundColor White
Write-Host "4. Explore: SELECT * FROM CURRENT_FINANCIAL_SNAPSHOT;" -ForegroundColor White

Write-Host "`nYour enterprise data pipeline will have:" -ForegroundColor Green
Write-Host "- Complete data loading with all 20 companies" -ForegroundColor Gray
Write-Host "- Watermarking for change tracking" -ForegroundColor Gray
Write-Host "- Investment analytics and screening" -ForegroundColor Gray
Write-Host "- Automated monitoring and alerts" -ForegroundColor Gray