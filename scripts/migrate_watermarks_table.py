#!/usr/bin/env python3
"""
ETL Watermarks Table Migration Script

This script adds the missing fiscal date columns to the existing ETL_WATERMARKS table
to support enhanced watermarking features including fiscal date tracking and delisting intelligence.
"""

import os
import sys
from pathlib import Path
import snowflake.connector
from typing import Dict

def get_snowflake_config() -> Dict[str, str]:
    """Get Snowflake configuration from environment variables."""
    required_vars = [
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA', 'SNOWFLAKE_WAREHOUSE'
    ]
    
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        print(f"❌ Missing required environment variables: {missing_vars}")
        print("Please set these environment variables and try again.")
        sys.exit(1)
    
    return {
        'account': os.environ['SNOWFLAKE_ACCOUNT'],
        'user': os.environ['SNOWFLAKE_USER'],
        'password': os.environ['SNOWFLAKE_PASSWORD'],
        'database': os.environ['SNOWFLAKE_DATABASE'],
        'schema': os.environ['SNOWFLAKE_SCHEMA'],
        'warehouse': os.environ['SNOWFLAKE_WAREHOUSE']
    }

def run_migration():
    """Run the ETL watermarks table migration."""
    print("🔧 Starting ETL_WATERMARKS table migration...")
    
    # Get Snowflake configuration
    try:
        config = get_snowflake_config()
    except SystemExit:
        return False
    
    # Read migration SQL
    sql_file = Path(__file__).parent.parent / "snowflake" / "migrations" / "add_fiscal_date_columns_to_watermarks.sql"
    if not sql_file.exists():
        print(f"❌ Migration SQL file not found: {sql_file}")
        return False
    
    with open(sql_file, 'r') as f:
        migration_sql = f.read()
    
    # Connect to Snowflake and run migration
    try:
        print("📄 Connecting to Snowflake...")
        conn = snowflake.connector.connect(**config)
        cursor = conn.cursor()
        
        # Split SQL by semicolon and execute each statement
        statements = [stmt.strip() for stmt in migration_sql.split(';') if stmt.strip()]
        
        for i, statement in enumerate(statements, 1):
            # Skip comments-only statements
            if statement.startswith('--') or not statement:
                continue
                
            print(f"📋 Executing statement {i}/{len(statements)}...")
            try:
                cursor.execute(statement)
                results = cursor.fetchall()
                
                # Print results for informational statements
                if results and any(str(col).upper() in ['MESSAGE', 'STATUS', 'COMPLETION_MESSAGE'] for col in results[0] if col):
                    for row in results:
                        print(f"  ℹ️  {row[0]}")
                elif results and len(results) <= 10:  # Print small result sets
                    for row in results:
                        print(f"  📊 {row}")
                        
            except Exception as e:
                if "already exists" in str(e).lower() or "if not exists" in statement.lower():
                    print(f"  ⚠️  Column already exists (safe to ignore): {e}")
                else:
                    print(f"  ❌ Error in statement: {e}")
                    raise
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ Migration completed successfully!")
        print("🎉 ETL_WATERMARKS table now supports fiscal date tracking and delisting intelligence!")
        return True
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        return False

def main():
    """Main function."""
    success = run_migration()
    
    if success:
        print("\n📋 Next steps:")
        print("1. Your company overview processing should now work without column errors")
        print("2. The enhanced watermarking system will track fiscal dates and delisting status")
        print("3. Future processing will benefit from intelligent delisted stock exclusion")
        sys.exit(0)
    else:
        print("\n❌ Migration failed. Please check the errors above and try again.")
        sys.exit(1)

if __name__ == "__main__":
    main()