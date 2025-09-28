-- ============================================================================
-- ALERTING SYSTEM: Automated Monitoring and Notifications (FIXED VERSION)
-- Creates stored procedures for automated alerts on data quality and freshness
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE WAREHOUSE FIN_TRADE_WH;
USE SCHEMA RAW;

-- ============================================================================
-- STEP 1: Create Alert Configuration Table
-- ============================================================================

CREATE OR REPLACE TABLE ALERT_CONFIGURATIONS (
    ALERT_ID INTEGER AUTOINCREMENT,
    ALERT_NAME VARCHAR(100),
    ALERT_TYPE VARCHAR(50), -- DATA_FRESHNESS, DATA_QUALITY, PROCESS_FAILURE, VOLUME_ANOMALY
    THRESHOLD_VALUE FLOAT,
    THRESHOLD_OPERATOR VARCHAR(10), -- '>', '<', '=', '>=', '<='  
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    ALERT_FREQUENCY_MINUTES INTEGER DEFAULT 60, -- How often to check
    LAST_ALERT_TIME TIMESTAMP_NTZ,
    ALERT_DESCRIPTION VARCHAR(500),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Insert default alert configurations
INSERT INTO ALERT_CONFIGURATIONS (ALERT_NAME, ALERT_TYPE, THRESHOLD_VALUE, THRESHOLD_OPERATOR, ALERT_DESCRIPTION, ALERT_FREQUENCY_MINUTES) VALUES
    ('Stale Data Alert', 'DATA_FRESHNESS', 48, '>', 'Alert when data is older than 48 hours', 120),
    ('Low Data Completeness', 'DATA_QUALITY', 80, '<', 'Alert when data completeness falls below 80%', 60),
    ('Process Failure Alert', 'PROCESS_FAILURE', 0, '>', 'Alert on any failed processing batches', 30),
    ('Low Record Volume', 'VOLUME_ANOMALY', 15, '<', 'Alert when record count is below expected threshold', 60),
    ('High Record Volume', 'VOLUME_ANOMALY', 30, '>', 'Alert when record count is unexpectedly high', 60);

-- ============================================================================
-- STEP 2: Create Alert History Table
-- ============================================================================

CREATE OR REPLACE TABLE ALERT_HISTORY (
    ALERT_HISTORY_ID INTEGER AUTOINCREMENT,
    ALERT_ID INTEGER,
    ALERT_NAME VARCHAR(100),
    ALERT_TYPE VARCHAR(50),
    ALERT_MESSAGE VARCHAR(1000),
    ALERT_VALUE FLOAT,
    THRESHOLD_VALUE FLOAT,
    ALERT_SEVERITY VARCHAR(20), -- INFO, WARNING, CRITICAL
    ALERT_STATUS VARCHAR(20), -- TRIGGERED, RESOLVED, ACKNOWLEDGED
    BATCH_ID VARCHAR(50),
    SYMBOL VARCHAR(10),
    TRIGGERED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    RESOLVED_AT TIMESTAMP_NTZ,
    ACKNOWLEDGED_BY VARCHAR(100),
    ACKNOWLEDGED_AT TIMESTAMP_NTZ
);

-- ============================================================================
-- STEP 3: Data Freshness Alert Function
-- ============================================================================

CREATE OR REPLACE PROCEDURE CHECK_DATA_FRESHNESS_ALERTS()
RETURNS INTEGER
LANGUAGE SQL
AS
$$
DECLARE
    alert_count INTEGER DEFAULT 0;
    max_hours_threshold FLOAT;
    current_max_hours FLOAT;
    alert_message STRING;
BEGIN
    -- Get the data freshness threshold
    SELECT THRESHOLD_VALUE INTO max_hours_threshold
    FROM ALERT_CONFIGURATIONS 
    WHERE ALERT_TYPE = 'DATA_FRESHNESS' AND IS_ACTIVE = TRUE
    LIMIT 1;
    
    -- Check current data freshness
    SELECT MAX(DATEDIFF('hour', LOAD_TIMESTAMP, CURRENT_TIMESTAMP)) INTO current_max_hours
    FROM OVERVIEW;
    
    -- If data is stale beyond threshold, create alert
    IF (current_max_hours > max_hours_threshold) THEN
        SET alert_message = 'Data freshness alert: Data is ' || current_max_hours || ' hours old, exceeding threshold of ' || max_hours_threshold || ' hours';
        
        INSERT INTO ALERT_HISTORY (ALERT_ID, ALERT_NAME, ALERT_TYPE, ALERT_MESSAGE, ALERT_VALUE, THRESHOLD_VALUE, ALERT_SEVERITY, ALERT_STATUS)
        SELECT 
            ALERT_ID,
            ALERT_NAME,
            ALERT_TYPE,
            alert_message,
            current_max_hours,
            THRESHOLD_VALUE,
            CASE WHEN current_max_hours > THRESHOLD_VALUE * 2 THEN 'CRITICAL' ELSE 'WARNING' END,
            'TRIGGERED'
        FROM ALERT_CONFIGURATIONS 
        WHERE ALERT_TYPE = 'DATA_FRESHNESS' AND IS_ACTIVE = TRUE;
        
        SET alert_count = alert_count + 1;
        
        -- Update last alert time
        UPDATE ALERT_CONFIGURATIONS 
        SET LAST_ALERT_TIME = CURRENT_TIMESTAMP
        WHERE ALERT_TYPE = 'DATA_FRESHNESS' AND IS_ACTIVE = TRUE;
    END IF;
    
    RETURN alert_count;
END;
$$;

-- ============================================================================
-- STEP 4: Data Quality Alert Function
-- ============================================================================

CREATE OR REPLACE PROCEDURE CHECK_DATA_QUALITY_ALERTS()
RETURNS INTEGER
LANGUAGE SQL
AS
$$
DECLARE
    alert_count INTEGER DEFAULT 0;
    completeness_threshold FLOAT;
    current_completeness FLOAT;
    alert_message STRING;
    batch_id_val STRING;
BEGIN
    -- Get the data quality threshold
    SELECT THRESHOLD_VALUE INTO completeness_threshold
    FROM ALERT_CONFIGURATIONS 
    WHERE ALERT_TYPE = 'DATA_QUALITY' AND IS_ACTIVE = TRUE
    LIMIT 1;
    
    -- Check current data completeness for latest batch
    SELECT 
        BATCH_ID,
        (COUNT(CASE WHEN NAME IS NOT NULL THEN 1 END) * 100.0 / COUNT(*)) 
    INTO batch_id_val, current_completeness
    FROM OVERVIEW 
    WHERE BATCH_ID = (SELECT MAX(BATCH_ID) FROM OVERVIEW)
    GROUP BY BATCH_ID;
    
    -- If completeness is below threshold, create alert
    IF (current_completeness < completeness_threshold) THEN
        SET alert_message = 'Data quality alert: Data completeness is ' || ROUND(current_completeness, 1) || '%, below threshold of ' || completeness_threshold || '% for batch ' || batch_id_val;
        
        INSERT INTO ALERT_HISTORY (ALERT_ID, ALERT_NAME, ALERT_TYPE, ALERT_MESSAGE, ALERT_VALUE, THRESHOLD_VALUE, ALERT_SEVERITY, ALERT_STATUS, BATCH_ID)
        SELECT 
            ALERT_ID,
            ALERT_NAME,
            ALERT_TYPE,
            alert_message,
            current_completeness,
            THRESHOLD_VALUE,
            CASE WHEN current_completeness < THRESHOLD_VALUE * 0.5 THEN 'CRITICAL' ELSE 'WARNING' END,
            'TRIGGERED',
            batch_id_val
        FROM ALERT_CONFIGURATIONS 
        WHERE ALERT_TYPE = 'DATA_QUALITY' AND IS_ACTIVE = TRUE;
        
        SET alert_count = alert_count + 1;
        
        UPDATE ALERT_CONFIGURATIONS 
        SET LAST_ALERT_TIME = CURRENT_TIMESTAMP
        WHERE ALERT_TYPE = 'DATA_QUALITY' AND IS_ACTIVE = TRUE;
    END IF;
    
    RETURN alert_count;
END;
$$;

-- ============================================================================
-- STEP 5: Volume Anomaly Alert Function
-- ============================================================================

CREATE OR REPLACE PROCEDURE CHECK_VOLUME_ANOMALY_ALERTS()
RETURNS INTEGER
LANGUAGE SQL
AS
$$
DECLARE
    alert_count INTEGER DEFAULT 0;
    low_threshold FLOAT;
    high_threshold FLOAT;
    current_volume INTEGER;
    alert_message STRING;
    batch_id_val STRING;
BEGIN
    -- Get volume thresholds
    SELECT THRESHOLD_VALUE INTO low_threshold
    FROM ALERT_CONFIGURATIONS 
    WHERE ALERT_TYPE = 'VOLUME_ANOMALY' AND ALERT_NAME = 'Low Record Volume' AND IS_ACTIVE = TRUE;
    
    SELECT THRESHOLD_VALUE INTO high_threshold
    FROM ALERT_CONFIGURATIONS 
    WHERE ALERT_TYPE = 'VOLUME_ANOMALY' AND ALERT_NAME = 'High Record Volume' AND IS_ACTIVE = TRUE;
    
    -- Check current volume for latest batch
    SELECT COUNT(*), MAX(BATCH_ID) INTO current_volume, batch_id_val
    FROM OVERVIEW 
    WHERE BATCH_ID = (SELECT MAX(BATCH_ID) FROM OVERVIEW);
    
    -- Check for low volume
    IF (current_volume < low_threshold) THEN
        SET alert_message = 'Volume anomaly alert: Record count is ' || current_volume || ', below expected threshold of ' || low_threshold || ' for batch ' || batch_id_val;
        
        INSERT INTO ALERT_HISTORY (ALERT_ID, ALERT_NAME, ALERT_TYPE, ALERT_MESSAGE, ALERT_VALUE, THRESHOLD_VALUE, ALERT_SEVERITY, ALERT_STATUS, BATCH_ID)
        SELECT 
            ALERT_ID,
            ALERT_NAME,
            ALERT_TYPE,
            alert_message,
            current_volume,
            THRESHOLD_VALUE,
            'WARNING',
            'TRIGGERED',
            batch_id_val
        FROM ALERT_CONFIGURATIONS 
        WHERE ALERT_TYPE = 'VOLUME_ANOMALY' AND ALERT_NAME = 'Low Record Volume' AND IS_ACTIVE = TRUE;
        
        SET alert_count = alert_count + 1;
    END IF;
    
    -- Check for high volume
    IF (current_volume > high_threshold) THEN
        SET alert_message = 'Volume anomaly alert: Record count is ' || current_volume || ', above expected threshold of ' || high_threshold || ' for batch ' || batch_id_val;
        
        INSERT INTO ALERT_HISTORY (ALERT_ID, ALERT_NAME, ALERT_TYPE, ALERT_MESSAGE, ALERT_VALUE, THRESHOLD_VALUE, ALERT_SEVERITY, ALERT_STATUS, BATCH_ID)
        SELECT 
            ALERT_ID,
            ALERT_NAME,
            ALERT_TYPE,
            alert_message,
            current_volume,
            THRESHOLD_VALUE,
            'INFO',
            'TRIGGERED',
            batch_id_val
        FROM ALERT_CONFIGURATIONS 
        WHERE ALERT_TYPE = 'VOLUME_ANOMALY' AND ALERT_NAME = 'High Record Volume' AND IS_ACTIVE = TRUE;
        
        SET alert_count = alert_count + 1;
    END IF;
    
    RETURN alert_count;
END;
$$;

-- ============================================================================
-- STEP 6: Simplified Alert Summary View (Instead of Complex Stored Procedure)
-- ============================================================================

-- Create a view that summarizes all alert checks
CREATE OR REPLACE VIEW ALERT_CHECK_SUMMARY AS
SELECT 
    'ALERT_SUMMARY' as CHECK_TYPE,
    
    -- Data Freshness Check
    CASE 
        WHEN MAX(DATEDIFF('hour', o.LOAD_TIMESTAMP, CURRENT_TIMESTAMP)) > 
             (SELECT THRESHOLD_VALUE FROM ALERT_CONFIGURATIONS WHERE ALERT_TYPE = 'DATA_FRESHNESS' AND IS_ACTIVE = TRUE LIMIT 1)
        THEN 1 ELSE 0 
    END as FRESHNESS_ALERTS_TRIGGERED,
    
    -- Data Quality Check  
    CASE 
        WHEN (COUNT(CASE WHEN o.NAME IS NOT NULL THEN 1 END) * 100.0 / COUNT(*)) < 
             (SELECT THRESHOLD_VALUE FROM ALERT_CONFIGURATIONS WHERE ALERT_TYPE = 'DATA_QUALITY' AND IS_ACTIVE = TRUE LIMIT 1)
        THEN 1 ELSE 0 
    END as QUALITY_ALERTS_TRIGGERED,
    
    -- Volume Check
    CASE 
        WHEN COUNT(*) < (SELECT THRESHOLD_VALUE FROM ALERT_CONFIGURATIONS WHERE ALERT_TYPE = 'VOLUME_ANOMALY' AND ALERT_NAME = 'Low Record Volume' AND IS_ACTIVE = TRUE LIMIT 1)
        OR COUNT(*) > (SELECT THRESHOLD_VALUE FROM ALERT_CONFIGURATIONS WHERE ALERT_TYPE = 'VOLUME_ANOMALY' AND ALERT_NAME = 'High Record Volume' AND IS_ACTIVE = TRUE LIMIT 1)
        THEN 1 ELSE 0 
    END as VOLUME_ALERTS_TRIGGERED,
    
    -- Current Values
    MAX(DATEDIFF('hour', o.LOAD_TIMESTAMP, CURRENT_TIMESTAMP)) as CURRENT_DATA_AGE_HOURS,
    ROUND((COUNT(CASE WHEN o.NAME IS NOT NULL THEN 1 END) * 100.0 / COUNT(*)), 1) as CURRENT_COMPLETENESS_PCT,
    COUNT(*) as CURRENT_RECORD_COUNT,
    MAX(o.BATCH_ID) as LATEST_BATCH_ID,
    
    CURRENT_TIMESTAMP as CHECK_TIMESTAMP
    
FROM OVERVIEW o
WHERE o.BATCH_ID = (SELECT MAX(BATCH_ID) FROM OVERVIEW);

-- Simplified stored procedure to run basic alert logic
CREATE OR REPLACE PROCEDURE RUN_ALL_ALERT_CHECKS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result_message STRING;
    total_alerts INTEGER DEFAULT 0;
    freshness_alert INTEGER;
    quality_alert INTEGER; 
    volume_alert INTEGER;
BEGIN
    -- Get alert summary from view
    SELECT 
        FRESHNESS_ALERTS_TRIGGERED + QUALITY_ALERTS_TRIGGERED + VOLUME_ALERTS_TRIGGERED,
        FRESHNESS_ALERTS_TRIGGERED,
        QUALITY_ALERTS_TRIGGERED,
        VOLUME_ALERTS_TRIGGERED
    INTO total_alerts, freshness_alert, quality_alert, volume_alert
    FROM ALERT_CHECK_SUMMARY;
    
    SET result_message = 'Alert check completed. Total alerts triggered: ' || total_alerts || 
                        ' (Freshness: ' || freshness_alert || 
                        ', Quality: ' || quality_alert || 
                        ', Volume: ' || volume_alert || ')';
    
    RETURN result_message;
END;
$$;

-- ============================================================================
-- STEP 7: Alert Dashboard Views
-- ============================================================================

-- Active Alerts Dashboard
CREATE OR REPLACE VIEW ACTIVE_ALERTS_DASHBOARD AS
SELECT 
    ah.ALERT_HISTORY_ID,
    ah.ALERT_NAME,
    ah.ALERT_TYPE,
    ah.ALERT_MESSAGE,
    ah.ALERT_SEVERITY,
    ah.ALERT_VALUE,
    ah.THRESHOLD_VALUE,
    ah.BATCH_ID,
    ah.TRIGGERED_AT,
    DATEDIFF('hour', ah.TRIGGERED_AT, CURRENT_TIMESTAMP) as HOURS_SINCE_TRIGGERED,
    CASE 
        WHEN ah.ALERT_SEVERITY = 'CRITICAL' THEN 1
        WHEN ah.ALERT_SEVERITY = 'WARNING' THEN 2
        ELSE 3
    END as SEVERITY_PRIORITY
FROM ALERT_HISTORY ah
WHERE ah.ALERT_STATUS = 'TRIGGERED'
  AND ah.TRIGGERED_AT >= DATEADD('day', -7, CURRENT_TIMESTAMP) -- Last 7 days
ORDER BY SEVERITY_PRIORITY ASC, ah.TRIGGERED_AT DESC;

-- Alert Summary Statistics
CREATE OR REPLACE VIEW ALERT_STATISTICS AS
SELECT 
    ALERT_TYPE,
    COUNT(*) as TOTAL_ALERTS_7_DAYS,
    COUNT(CASE WHEN ALERT_SEVERITY = 'CRITICAL' THEN 1 END) as CRITICAL_ALERTS,
    COUNT(CASE WHEN ALERT_SEVERITY = 'WARNING' THEN 1 END) as WARNING_ALERTS,
    COUNT(CASE WHEN ALERT_STATUS = 'TRIGGERED' THEN 1 END) as ACTIVE_ALERTS,
    COUNT(CASE WHEN ALERT_STATUS = 'RESOLVED' THEN 1 END) as RESOLVED_ALERTS,
    AVG(DATEDIFF('hour', TRIGGERED_AT, COALESCE(RESOLVED_AT, CURRENT_TIMESTAMP))) as AVG_RESOLUTION_TIME_HOURS,
    MAX(TRIGGERED_AT) as LAST_ALERT_TIME
FROM ALERT_HISTORY
WHERE TRIGGERED_AT >= DATEADD('day', -7, CURRENT_TIMESTAMP)
GROUP BY ALERT_TYPE
ORDER BY TOTAL_ALERTS_7_DAYS DESC;

-- System Health Overview
CREATE OR REPLACE VIEW SYSTEM_HEALTH_OVERVIEW AS
SELECT 
    'DATA_PIPELINE_HEALTH' as METRIC_CATEGORY,
    
    -- Data Freshness
    DATEDIFF('hour', MAX(o.LOAD_TIMESTAMP), CURRENT_TIMESTAMP) as HOURS_SINCE_LAST_DATA,
    CASE 
        WHEN DATEDIFF('hour', MAX(o.LOAD_TIMESTAMP), CURRENT_TIMESTAMP) <= 24 THEN 'HEALTHY'
        WHEN DATEDIFF('hour', MAX(o.LOAD_TIMESTAMP), CURRENT_TIMESTAMP) <= 48 THEN 'WARNING'
        ELSE 'CRITICAL'
    END as DATA_FRESHNESS_STATUS,
    
    -- Processing Status
    (SELECT PROCESSING_STATUS FROM DATA_WATERMARKS WHERE SOURCE_SYSTEM = 'LAMBDA_ALPHA_VANTAGE' AND TABLE_NAME = 'OVERVIEW') as LAST_PROCESSING_STATUS,
    
    -- Record Counts
    COUNT(DISTINCT o.SYMBOL) as UNIQUE_COMPANIES_LOADED,
    COUNT(*) as TOTAL_RECORDS_LOADED,
    
    -- Data Quality Score
    ROUND((COUNT(CASE WHEN o.NAME IS NOT NULL THEN 1 END) * 100.0 / COUNT(*)), 1) as DATA_COMPLETENESS_PCT,
    
    -- Alert Status
    (SELECT COUNT(*) FROM ACTIVE_ALERTS_DASHBOARD WHERE ALERT_SEVERITY = 'CRITICAL') as ACTIVE_CRITICAL_ALERTS,
    (SELECT COUNT(*) FROM ACTIVE_ALERTS_DASHBOARD WHERE ALERT_SEVERITY = 'WARNING') as ACTIVE_WARNING_ALERTS,
    
    CURRENT_TIMESTAMP as HEALTH_CHECK_TIME
    
FROM OVERVIEW o
WHERE o.BATCH_ID = (SELECT MAX(BATCH_ID) FROM OVERVIEW);

SELECT 'ALERTING SYSTEM COMPLETE (FIXED VERSION)' as status;
SELECT 'Run: CALL RUN_ALL_ALERT_CHECKS(); to test the alerting system' as next_step;
SELECT 'Query ACTIVE_ALERTS_DASHBOARD or SYSTEM_HEALTH_OVERVIEW for monitoring' as monitoring_tip;