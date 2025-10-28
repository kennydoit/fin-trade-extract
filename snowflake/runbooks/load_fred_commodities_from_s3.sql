
-- Set S3 bucket and prefix
set s3_bucket = 'fin-trade-craft-landing';
set s3_prefix = 'commodities/';

-- List of commodities
set commodities = array_construct('WTI', 'BRENT', 'NATURAL_GAS', 'COPPER', 'ALUMINUM', 'WHEAT', 'CORN', 'COTTON', 'SUGAR', 'ALL_COMMODITIES');

-- For each commodity, create table, truncate, and load all matching CSVs
-- (Loads all files for each commodity; adjust pattern if you want only the latest)


-- Create a single table for all commodities
CREATE TABLE IF NOT EXISTS FRED_COMMODITIES (
	COMMODITY STRING,
	DATE DATE,
	VALUE FLOAT,
	UPDATE_FREQUENCY STRING
);
TRUNCATE TABLE FRED_COMMODITIES;

-- Load all CSVs for all commodities (defaulting UPDATE_FREQUENCY to 'MONTHLY')
COPY INTO FRED_COMMODITIES
FROM (
	SELECT
		t.$1 AS COMMODITY,
		t.$2::DATE AS DATE,
		t.$3::FLOAT AS VALUE,
		'MONTHLY' AS UPDATE_FREQUENCY
	FROM @~/commodities_stage (PATTERN => '.*.csv') t
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
FORCE=TRUE;
