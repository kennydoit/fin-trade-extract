import os
import sys
import snowflake.connector

def run_sql_file(sql_path, ctx):
    with open(sql_path, 'r') as f:
        sql = f.read()
    
    # Remove comment lines first
    lines = []
    for line in sql.split('\n'):
        line = line.strip()
        if line and not line.startswith('--'):
            lines.append(line)
    
    # Join back and split on semicolons
    clean_sql = ' '.join(lines)
    
    # Split on semicolons and filter out empty statements
    statements = []
    for stmt in clean_sql.split(';'):
        stmt = stmt.strip()
        if stmt:
            statements.append(stmt)
    
    for stmt in statements:
        print(f"Executing: {stmt[:80]}{'...' if len(stmt) > 80 else ''}")
        try:
            ctx.cursor().execute(stmt)
        except Exception as e:
            print(f"Error executing statement: {e}")
            raise

def main():
    sql_path = sys.argv[1] if len(sys.argv) > 1 else None
    if not sql_path or not os.path.exists(sql_path):
        print(f"SQL file not found: {sql_path}", file=sys.stderr)
        sys.exit(1)
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"]
    )
    try:
        run_sql_file(sql_path, conn)
    finally:
        conn.close()

if __name__ == "__main__":
    main()