#!/usr/bin/env python3
"""
Database Summary View Generator
Creates a comprehensive database_summary view showing statistics for all tables in the extracted schema.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from db.postgres_database_manager import PostgresDatabaseManager


def create_database_summary_view():
    """Create a comprehensive database_summary view with statistics for all tables."""

    with PostgresDatabaseManager() as db:


        # Define all tables in the extracted schema
        tables_config = [
            {
                'table_name': 'balance_sheet',
                'has_symbol': True,
                'has_date': True,
                'date_column': 'fiscal_date_ending',
                'description': 'Company balance sheet data (annual/quarterly)'
            },
            {
                'table_name': 'cash_flow',
                'has_symbol': True,
                'has_date': True,
                'date_column': 'fiscal_date_ending',
                'description': 'Company cash flow statements (annual/quarterly)'
            },
            {
                'table_name': 'commodities',
                'has_symbol': False,
                'has_date': False,
                'date_column': None,
                'description': 'Commodities master data'
            },
            {
                'table_name': 'commodities_daily',
                'has_symbol': False,
                'has_date': True,
                'date_column': 'date',
                'description': 'Daily commodities price data'
            },
            {
                'table_name': 'earnings_call_transcripts',
                'has_symbol': True,
                'has_date': False,
                'date_column': None,
                'description': 'Earnings call transcript data by quarter'
            },
            {
                'table_name': 'economic_indicators',
                'has_symbol': False,
                'has_date': False,
                'date_column': None,
                'description': 'Economic indicators master data'
            },
            {
                'table_name': 'economic_indicators_daily',
                'has_symbol': False,
                'has_date': True,
                'date_column': 'date',
                'description': 'Daily economic indicators time series data'
            },
            {
                'table_name': 'income_statement',
                'has_symbol': True,
                'has_date': True,
                'date_column': 'fiscal_date_ending',
                'description': 'Company income statements (annual/quarterly)'
            },
            {
                'table_name': 'insider_transactions',
                'has_symbol': True,
                'has_date': True,
                'date_column': 'transaction_date',
                'description': 'Insider trading transactions'
            },
            {
                'table_name': 'listing_status',
                'has_symbol': True,
                'has_date': True,
                'date_column': 'ipo_date',
                'description': 'Stock listing status and exchange information'
            },
            {
                'table_name': 'overview',
                'has_symbol': True,
                'has_date': False,
                'date_column': None,
                'description': 'Company overview and fundamental data'
            },
            {
                'table_name': 'time_series_daily_adjusted',
                'has_symbol': True,
                'has_date': True,
                'date_column': 'date',
                'description': 'Daily adjusted stock price time series'
            }
        ]

        # Build the UNION ALL query for comprehensive database summary
        union_parts = []

        for _i, config in enumerate(tables_config):
            table_name = config['table_name']
            has_symbol = config['has_symbol']
            has_date = config['has_date']
            date_column = config['date_column']
            description = config['description']

            # Build base SELECT for each table
            if has_symbol and has_date:
                # Tables with both symbol and date columns
                select_part = f"""
        SELECT
            '{table_name}' as table_name,
            '{description}' as description,
            COUNT(*) as record_count,
            COUNT(DISTINCT symbol_id) as unique_symbols,
            MIN(symbol) as first_symbol,
            MAX(symbol) as last_symbol,
            MIN({date_column}) as earliest_date,
            MAX({date_column}) as latest_date,
            COUNT(DISTINCT {date_column}) as unique_dates
        FROM extracted.{table_name}"""
            elif has_symbol and not has_date:
                # Tables with symbol but no date (like overview)
                select_part = f"""
        SELECT
            '{table_name}' as table_name,
            '{description}' as description,
            COUNT(*) as record_count,
            COUNT(DISTINCT symbol_id) as unique_symbols,
            MIN(symbol) as first_symbol,
            MAX(symbol) as last_symbol,
            NULL::date as earliest_date,
            NULL::date as latest_date,
            NULL::bigint as unique_dates
        FROM extracted.{table_name}"""
            elif not has_symbol and has_date:
                # Tables with date but no symbol (like commodities_daily, economic_indicators_daily)
                select_part = f"""
        SELECT
            '{table_name}' as table_name,
            '{description}' as description,
            COUNT(*) as record_count,
            NULL::bigint as unique_symbols,
            NULL::varchar as first_symbol,
            NULL::varchar as last_symbol,
            MIN({date_column}) as earliest_date,
            MAX({date_column}) as latest_date,
            COUNT(DISTINCT {date_column}) as unique_dates
        FROM extracted.{table_name}"""
            else:
                # Tables with neither symbol nor date (like commodities, economic_indicators master tables)
                select_part = f"""
        SELECT
            '{table_name}' as table_name,
            '{description}' as description,
            COUNT(*) as record_count,
            NULL::bigint as unique_symbols,
            NULL::varchar as first_symbol,
            NULL::varchar as last_symbol,
            NULL::date as earliest_date,
            NULL::date as latest_date,
            NULL::bigint as unique_dates
        FROM extracted.{table_name}"""

            union_parts.append(select_part)

        # Combine all parts with UNION ALL
        master_view_sql = f"""
        CREATE OR REPLACE VIEW extracted.database_summary AS
        {' UNION ALL '.join(union_parts)}
        ORDER BY table_name;
        """

        # Add comment to the view
        comment_sql = """
        COMMENT ON VIEW extracted.database_summary IS
        'Comprehensive database summary showing record counts, symbol ranges, date ranges, and statistics for all tables in the extracted schema. Updated automatically when tables change.';
        """

        try:
            # Create the view
            db.execute_query(master_view_sql)

            # Add comment
            db.execute_query(comment_sql)

            # Test the view by showing results

            test_query = """
            SELECT
                table_name,
                description,
                record_count,
                unique_symbols,
                first_symbol,
                last_symbol,
                earliest_date,
                latest_date,
                unique_dates
            FROM extracted.database_summary
            ORDER BY record_count DESC;
            """

            result = db.fetch_query(test_query)


            total_records = 0
            for row in result:
                table_name = row[0]
                description = row[1]
                record_count = row[2] or 0
                row[3] or 0
                row[4] or '-'
                row[5] or '-'
                earliest_date = row[6]
                latest_date = row[7]
                row[8] or 0

                total_records += record_count

                # Format date range
                if earliest_date and latest_date:
                    f"{earliest_date} to {latest_date}"[:20]
                else:
                    pass



            # Show additional detailed statistics

            # Count tables by type
            sum(1 for t in tables_config if t['has_symbol'])
            sum(1 for t in tables_config if t['has_date'])
            sum(1 for t in tables_config if not t['has_date'])



        except Exception:
            raise


def main():
    """Main function to create the database summary view."""

    try:
        create_database_summary_view()

    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    main()
