# Script to create ETL_FRED_WATERMARKS table in Snowflake
import os
import snowflake.connector

account = os.environ["SNOWFLAKE_ACCOUNT"]
user = os.environ["SNOWFLAKE_USER"]
warehouse = os.environ["SNOWFLAKE_WAREHOUSE"]
database = os.environ["SNOWFLAKE_DATABASE"]
schema = os.environ["SNOWFLAKE_SCHEMA"]
private_key_path = "snowflake_rsa_key.der"

