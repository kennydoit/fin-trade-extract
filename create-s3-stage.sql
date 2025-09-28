-- ============================================================================
-- EMERGENCY S3_STAGE FIX - Create Missing Stage
-- Quick fix to create the missing S3_STAGE that COPY INTO commands expect
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;

-- Create the S3_STAGE that the COPY INTO commands are looking for
CREATE OR REPLACE STAGE S3_STAGE
STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
URL = 's3://fin-trade-craft-landing/'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
COMMENT = 'Main S3 stage for COPY INTO operations';

-- Test that the stage was created successfully
SHOW STAGES LIKE 'S3_STAGE';

-- List files to verify connectivity
LIST @S3_STAGE/overview/ PATTERN = '.*\.csv';