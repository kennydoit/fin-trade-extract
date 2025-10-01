import os
import sys
import datetime as dt
import snowflake.connector


SQL_PATH = "snowflake/runbooks/full_load_overview_merge.sql"


def main():
    acct = os.environ.get("SNOWFLAKE_ACCOUNT")
    user = os.environ.get("SNOWFLAKE_USER")
    pwd = os.environ.get("SNOWFLAKE_PASSWORD")
    wh = os.environ.get("SNOWFLAKE_WAREHOUSE", "FIN_TRADE_WH")
    db = os.environ.get("SNOWFLAKE_DATABASE", "FIN_TRADE_EXTRACT")
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "RAW")
    load_date = os.environ.get("LOAD_DATE") or dt.date.today().isoformat()

    if not all([acct, user, pwd]):
        print("Missing Snowflake credentials env vars", file=sys.stderr)
        sys.exit(2)

    # Read SQL script
    with open(SQL_PATH, "r", encoding="utf-8") as f:
        sql = f.read()

    # Override USE statements and parameter lines via session commands
    # We'll run parameterized commands at the top before the rest
    prelude = f"""
    USE DATABASE {db};
    USE SCHEMA {schema};
    USE WAREHOUSE {wh};
    SET LOAD_DATE = '{load_date}';
    SET BATCH_ID = 'FULL_LOAD_' || REPLACE($LOAD_DATE, '-', '');
    SET SOURCE_FILE_NAME = 'full_load';
    """

    composed = prelude + "\n" + sql

    conn = snowflake.connector.connect(
        account=acct,
        user=user,
        password=pwd,
        warehouse=wh,
        database=db,
        schema=schema,
    )
    try:
        cur = conn.cursor()
        try:
            for stmt in [s.strip() for s in composed.split(";") if s.strip()]:
                cur.execute(stmt)
        finally:
            cur.close()
    finally:
        conn.close()

    print("Snowflake full-load merge completed.")


if __name__ == "__main__":
    main()
