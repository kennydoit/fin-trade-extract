import os
import sys
import snowflake.connector

def run_sql_file(sql_path, ctx):
    print(f"📄 Reading SQL file: {sql_path}")
    with open(sql_path, 'r') as f:
        sql = f.read()

    # Split on semicolons and filter out empty/comment-only statements
    statements = []
    for stmt in sql.split(';'):
        stmt = stmt.strip()
        if not stmt:
            continue
        # Remove comments and check if there's actual SQL content
        lines = [line.strip() for line in stmt.split('\n') if line.strip() and not line.strip().startswith('--')]
        if lines:
            statements.append(stmt)

    print(f"🔢 Found {len(statements)} SQL statements to execute")
    
    for i, stmt in enumerate(statements, 1):
        print(f"📋 Executing statement {i}/{len(statements)}: {stmt[:80]}{'...' if len(stmt) > 80 else ''}")
        try:
            result = ctx.cursor().execute(stmt)
            print(f"✅ Statement {i} completed successfully")
            
            # For SELECT statements, show row count
            if stmt.strip().upper().startswith('SELECT'):
                rows = result.fetchall()
                print(f"📊 Returned {len(rows)} rows")
            
            # For COPY statements, show copy results
            elif 'COPY INTO' in stmt.upper():
                rows = result.fetchall()
                if rows:
                    print(f"📥 COPY result: {rows[0]}")
                
        except Exception as e:
            print(f"❌ ERROR executing statement {i}: {e}")
            print(f"🔍 Failed statement: {stmt}")
            print(f"💡 This error caused the SQL execution to fail completely")
            print(f"🚨 All previous extraction work was wasted due to this SQL error")
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