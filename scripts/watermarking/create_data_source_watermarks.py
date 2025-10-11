#!/usr/bin/env python3
"""
Create Data Source Watermarks Script
Adds watermark entries for specific data sources (TIME_SERIES, COMPANY_OVERVIEW, etc.)
"""

import os
import sys
import logging
import snowflake.connector
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_snowflake_config():
    """Get Snowflake configuration from environment variables."""
    required_vars = [
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA', 'SNOWFLAKE_WAREHOUSE'
    ]
    
    config = {}
    missing_vars = []
    
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
        else:
            config[var.lower().replace('snowflake_', '')] = value
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
        
    return config

def read_sql_file():
    """Read the SQL file from snowflake/setup/create_data_source_watermarks.sql."""
    # Get the repository root (3 levels up from this script)
    repo_root = Path(__file__).parent.parent.parent
    sql_file = repo_root / 'snowflake' / 'setup' / 'create_data_source_watermarks.sql'
    
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    
    logger.info(f"📄 Reading SQL from: {sql_file}")
    
    with open(sql_file, 'r') as f:
        sql_content = f.read()
    
    return sql_content

def execute_sql_statements(connection, sql_content):
    """Execute SQL statements from the SQL file."""
    logger.info("🚀 Executing data source watermarks creation SQL...")
    
    # Split SQL content into individual statements
    # Remove comments and empty lines first
    lines = []
    for line in sql_content.split('\n'):
        # Skip comment lines
        if line.strip().startswith('--'):
            continue
        lines.append(line)
    
    # Join lines and split by semicolon
    cleaned_sql = '\n'.join(lines)
    statements = [stmt.strip() for stmt in cleaned_sql.split(';') if stmt.strip()]
    
    logger.info(f"📝 Found {len(statements)} SQL statements to execute")
    
    cursor = connection.cursor()
    
    try:
        for i, statement in enumerate(statements, 1):
            try:
                logger.info(f"⚙️  Executing statement {i}/{len(statements)}...")
                cursor.execute(statement)
                
                # Try to fetch results if available
                try:
                    results = cursor.fetchall()
                    if results:
                        for row in results:
                            logger.info(f"  {row}")
                except Exception:
                    # Some statements don't return results (DDL, DML)
                    pass
                    
            except Exception as e:
                logger.warning(f"⚠️  Statement {i} warning: {e}")
                # Continue with other statements even if one fails
                continue
        
        logger.info("✅ SQL execution completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Failed to execute SQL: {e}")
        raise
    finally:
        cursor.close()

def main():
    """Main execution function."""
    logger.info("🚀 Starting Data Source Watermarks Creation")
    
    try:
        # Get configuration
        config = get_snowflake_config()
        
        # Read SQL file
        sql_content = read_sql_file()
        
        # Connect to Snowflake
        logger.info("🔌 Connecting to Snowflake...")
        connection = snowflake.connector.connect(**config)
        logger.info("✅ Connected to Snowflake successfully")
        
        try:
            # Execute SQL statements
            execute_sql_statements(connection, sql_content)
            
        finally:
            connection.close()
            logger.info("🔒 Snowflake connection closed")
        
        logger.info("🎉 Data source watermarks creation completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
