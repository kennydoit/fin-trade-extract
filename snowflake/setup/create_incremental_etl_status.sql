-- ============================================================================
-- Create INCREMENTAL_ETL_STATUS table for tracking processing status
-- ============================================================================
USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- Create table to track incremental ETL processing status
CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.INCREMENTAL_ETL_STATUS (
    ID                  NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    SYMBOL              VARCHAR(20) NOT NULL,
    DATA_TYPE           VARCHAR(50) NOT NULL,
    LAST_PROCESSED_AT   TIMESTAMP_NTZ NOT NULL,
    SUCCESS             BOOLEAN NOT NULL,
    ERROR_MESSAGE       VARCHAR(1000),
    PROCESSING_MODE     VARCHAR(20) DEFAULT 'incremental',
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Index for fast lookups
    CONSTRAINT UK_ETL_STATUS_SYMBOL_TYPE_TIME UNIQUE (SYMBOL, DATA_TYPE, LAST_PROCESSED_AT)
)
COMMENT = 'Tracks incremental ETL processing status for all data types'
CLUSTER BY (DATA_TYPE, LAST_PROCESSED_AT, SYMBOL);

-- Create view for latest processing status per symbol/data_type
CREATE OR REPLACE VIEW FIN_TRADE_EXTRACT.RAW.LATEST_ETL_STATUS AS
SELECT 
    symbol,
    data_type,
    last_processed_at,
    success,
    error_message,
    processing_mode,
    created_at
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY symbol, data_type ORDER BY last_processed_at DESC) as rn
    FROM FIN_TRADE_EXTRACT.RAW.INCREMENTAL_ETL_STATUS
) 
WHERE rn = 1;

SELECT 'INCREMENTAL_ETL_STATUS table and view created successfully!' as status;