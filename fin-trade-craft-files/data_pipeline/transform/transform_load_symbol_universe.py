#!/usr/bin/env python3
"""Load symbol universes into the transformed schema.

This module creates and appends records to the ``transformed.symbol_universes``
table. It accepts input data either as a CSV file path, a SQL query string, or a
:class:`pandas.DataFrame`. Two additional columns are automatically generated
for each load:

* ``universe_id`` – a simple readable ID based on source type (S######, C######, D######)
* ``load_date_time`` – the current timestamp in UTC

The required input columns are ``symbol``, ``exchange`` and ``asset_type``. A
universe name must also be supplied. By default the function appends rows to the
existing table. When the ``start_fresh`` flag is set, the table is dropped and
recreated before loading the new records.

The table schema is created as::

    transformed.symbol_universes(
        universe_id       VARCHAR,     -- Simple format: S123456, C123456, D123456
        universe_name     VARCHAR,
        symbol            VARCHAR,
        exchange          VARCHAR,
        asset_type        VARCHAR,
        load_date_time    TIMESTAMPTZ,
        symbol_id         BIGINT,
        symbol_universe_id BIGSERIAL PRIMARY KEY
    )

The first six columns appear in the order requested and are followed by helpful
identifiers such as ``symbol_id`` (from ``listing_status``) and a row
identifier ``symbol_universe_id``.
"""

from __future__ import annotations

import argparse
import random
import sys
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Allow imports from the repository root
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager


def _generate_simple_universe_id(data_source_type: str, db: PostgresDatabaseManager) -> str:
    """Generate a simple universe ID based on data source type.

    Parameters
    ----------
    data_source_type:
        Type of data source: 'S' for SQL, 'C' for CSV, 'D' for DataFrame
    db:
        Database manager for checking uniqueness

    Returns
    -------
    str
        Universe ID in format: {type}{6-digit-number} (e.g., 'S123456', 'C987654')
    """
    max_attempts = 100

    for _attempt in range(max_attempts):
        # Generate 6-digit random number
        number = random.randint(100000, 999999)
        universe_id = f"{data_source_type}{number}"

        # Check if ID already exists
        check_query = "SELECT COUNT(*) FROM transformed.symbol_universes WHERE universe_id = %s"
        try:
            result = db.fetch_query(check_query, (universe_id,))
            if result and result[0][0] == 0:  # ID doesn't exist
                return universe_id
        except Exception:
            # If table doesn't exist yet, the ID is definitely unique
            return universe_id

    # Enhanced fallback: Use timestamp + microseconds + attempt counter for guaranteed uniqueness
    now = datetime.now()
    timestamp_micro = f"{int(now.timestamp())}{now.microsecond:06d}"

    # Try timestamp-based IDs with incrementing suffix
    for suffix in range(100):
        # Take last 5 digits of timestamp+microseconds + 1 digit suffix
        unique_suffix = f"{timestamp_micro[-5:]}{suffix}"[:6].zfill(6)
        universe_id = f"{data_source_type}{unique_suffix}"

        # Final check for timestamp-based ID
        check_query = "SELECT COUNT(*) FROM transformed.symbol_universes WHERE universe_id = %s"
        try:
            result = db.fetch_query(check_query, (universe_id,))
            if result and result[0][0] == 0:
                return universe_id
        except Exception:
            return universe_id

    # Ultimate fallback - this should never happen
    import uuid
    fallback_id = str(uuid.uuid4()).replace('-', '')[:6]
    return f"{data_source_type}{fallback_id}"


def _ensure_table(db: PostgresDatabaseManager) -> None:
    """Create ``transformed.symbol_universes`` if it does not exist."""
    if not db.table_exists("symbol_universes", "transformed"):
        db.execute_query("CREATE SCHEMA IF NOT EXISTS transformed")
        create_sql = """
            CREATE TABLE transformed.symbol_universes (
                universe_id VARCHAR(10) NOT NULL,
                universe_name VARCHAR NOT NULL,
                symbol VARCHAR NOT NULL,
                exchange VARCHAR NOT NULL,
                asset_type VARCHAR NOT NULL,
                load_date_time TIMESTAMPTZ NOT NULL,
                symbol_id BIGINT,
                symbol_universe_id BIGSERIAL PRIMARY KEY
            )
        """
        db.execute_query(create_sql)


def _recreate_table(db: PostgresDatabaseManager) -> None:
    """Drop and recreate ``transformed.symbol_universes``."""
    db.execute_query("DROP TABLE IF EXISTS transformed.symbol_universes")
    _ensure_table(db)


def load_symbol_universe(
    data: pd.DataFrame | str | Path,
    universe_name: str,
    *,
    start_fresh: bool = False,
) -> str:
    """Load a symbol universe into the database.

    Parameters
    ----------
    data:
        Either a DataFrame, path to a CSV file, or a SQL query string
        containing three columns: ``symbol``, ``exchange`` and ``asset_type``.
    universe_name:
        Name of the universe to apply to all rows.
    start_fresh:
        When ``True`` the ``transformed.symbol_universes`` table will be
        dropped and recreated before loading the new data.

    Returns
    -------
    str
        The generated ``universe_id`` applied to the loaded rows.
        Format: S######, C######, or D###### based on data source type.
    """

    # Determine data source type and load data
    if isinstance(data, (str | Path)):
        if Path(str(data)).exists():
            df = pd.read_csv(str(data))
            source_type = 'C'  # CSV
        else:
            db_tmp = PostgresDatabaseManager()
            db_tmp.connect()
            try:
                df = db_tmp.fetch_dataframe(str(data))
                source_type = 'S'  # SQL
            finally:
                db_tmp.close()
    elif isinstance(data, pd.DataFrame):
        df = data.copy()
        source_type = 'D'  # DataFrame
    else:
        raise TypeError(
            "data must be a pandas DataFrame, path to CSV, or SQL query string"
        )

    required_cols = {"symbol", "exchange", "asset_type"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Input data missing required columns: {missing}")

    load_time = datetime.now(timezone.utc)  # noqa: UP017

    db = PostgresDatabaseManager()
    db.connect()
    try:
        if start_fresh:
            _recreate_table(db)
        else:
            _ensure_table(db)

        # Generate simple universe ID based on source type
        universe_id = _generate_simple_universe_id(source_type, db)

        insert_rows: list[Sequence[object]] = []
        for _, row in df.iterrows():
            symbol = row["symbol"]
            symbol_id = db.get_symbol_id(symbol)
            insert_rows.append(
                (
                    universe_id,  # Now a simple string like 'S123456'
                    universe_name,
                    symbol,
                    row["exchange"],
                    row["asset_type"],
                    load_time,
                    symbol_id,
                )
            )

        insert_sql = """
            INSERT INTO transformed.symbol_universes (
                universe_id,
                universe_name,
                symbol,
                exchange,
                asset_type,
                load_date_time,
                symbol_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        db.execute_many(insert_sql, insert_rows)
        print(  # noqa: T201
            f"Loaded {len(insert_rows)} rows into transformed.symbol_universes with universe_id {universe_id}"
        )
        return universe_id
    finally:
        db.close()


def _parse_args(argv: Iterable[str]) -> tuple[Path | None, str | None, str, bool]:
    """Parse command line arguments."""

    parser = argparse.ArgumentParser(
        description="Load symbol universe data into transformed.symbol_universes",
    )
    src_group = parser.add_mutually_exclusive_group(required=True)
    src_group.add_argument("--csv", type=Path, help="CSV file containing symbol data")
    src_group.add_argument(
        "--sql",
        help="SQL query returning columns symbol, exchange and asset_type",
    )
    parser.add_argument("--universe-name", required=True, help="Name of the universe")
    parser.add_argument(
        "--start-fresh",
        action="store_true",
        help="Drop and recreate table before loading",
    )
    args = parser.parse_args(list(argv))
    return args.csv, args.sql, args.universe_name, args.start_fresh


def main(argv: Iterable[str] | None = None) -> None:
    """Command line entry point."""
    csv_path, sql_query, universe_name, start_fresh = _parse_args(argv or sys.argv[1:])
    if csv_path is not None:
        load_symbol_universe(csv_path, universe_name, start_fresh=start_fresh)
    else:
        load_symbol_universe(sql_query, universe_name, start_fresh=start_fresh)


def load_universe_from_query(sql_query: str, universe_name: str, start_fresh: bool = False) -> str:
    """Load a symbol universe using a SQL query.

    Parameters
    ----------
    sql_query:
        SQL query returning columns symbol, exchange and asset_type
    universe_name:
        Name of the universe to apply to all rows
    start_fresh:
        When True, drop and recreate table before loading

    Returns
    -------
    str
        The generated universe_id applied to the loaded rows (format: S######)
    """
    print(f"Loading SQL query universe: {universe_name}")  # noqa: T201

    try:
        universe_id = load_symbol_universe(
            data=sql_query,
            universe_name=universe_name,
            start_fresh=start_fresh,
        )
        print(f"✅ Successfully loaded universe with ID: {universe_id}")  # noqa: T201
        return universe_id
    except Exception as e:
        print(f"❌ Error loading universe: {e}")  # noqa: T201
        raise


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    # Behavior:
    #   * If script is invoked with no CLI args -> run the SQL-based universe loader.
    #   * If args are provided -> use argparse driven CLI (no test execution).
    # This prevents the argparse error that occurred after successfully running the
    # test function and then calling main() without required flags.
    if len(sys.argv) == 1:
        # Define SQL query and universe name
        sql_query = """
        SELECT symbol, exchange, asset_type
        FROM transformed.company_master
        WHERE (ipo_date < '2020-01-01')
          AND status = 'Active'
          AND asset_type = 'Stock'
          AND description IS NOT NULL
          AND industry IS NOT NULL
          AND sector IS NOT NULL
          AND (
                balance_sheet_count > 5
                OR income_statement_count > 5
                OR (
                    cash_flow_count > 5
                    AND earnings_call_transcript_count > 5
                )
              )
          AND time_series_daily_adjusted_count > 500
          AND symbol IN (
                SELECT DISTINCT symbol
                FROM extracted.cash_flow
                WHERE report_type = 'annual'
                  AND fiscal_date_ending >= '2019-01-01'
                  AND fiscal_date_ending < '2020-01-01'
                  AND net_income >= 1000000000
          );
        """
        universe_name = "IPO_Before_2020_All_Fundamentals_GT500_OCHLV_GT1B_Net_Income"

        # Single function call with SQL query and universe name as arguments
        load_universe_from_query(sql_query, universe_name, start_fresh=False)

        # Uncomment to test the CSV-based universe instead of / in addition to SQL
        # csv_path = (
        #     Path(__file__).parent.parent
        #     / "symbol_universes"
        #     / "ipo_before_2020_all_fundamentals_GT500_OCHLV_500MM_to_1B_Net_Income.csv"
        # )
        # universe_name_csv = "IPO_Before_2020_All_Fundamentals_GT500_OCHLV_500MM_to_1B_Net_Income"
        # load_symbol_universe(csv_path, universe_name_csv, start_fresh=False)
    else:
        main()
