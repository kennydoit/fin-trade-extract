"""
Database monitoring utility for fin-trade-craft PostgreSQL database.

This script provides comprehensive database statistics including:
- Row counts for all tables
- Last update timestamps
- Data quality metrics
- Schema information
- Null value analysis
- Storage usage statistics
"""

import sys
from datetime import datetime
from pathlib import Path

# Add the parent directory to the path so we can import from db
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager


class DatabaseMonitor:
    """Monitor and analyze PostgreSQL database health and statistics."""

    def __init__(self):
        self.db_manager = PostgresDatabaseManager()

    def get_all_table_names(self):
        """Get list of all tables in the database."""
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """
            result = db.fetch_query(query)
            return [row[0] for row in result] if result else []

    def get_table_row_count(self, table_name):
        """Get row count for a specific table."""
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            query = f"SELECT COUNT(*) FROM {table_name}"
            result = db.fetch_query(query)
            return result[0][0] if result else 0

    def get_table_last_update(self, table_name):
        """Get last update timestamp for a table (looks for date/timestamp columns)."""
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            # First, identify date/timestamp columns
            date_column_query = """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s
                AND table_schema = 'public'
                AND (data_type IN ('date', 'timestamp', 'timestamp with time zone', 'timestamp without time zone')
                     OR column_name IN ('date', 'updated_at', 'created_at', 'last_modified'))
                ORDER BY
                    CASE
                        WHEN column_name = 'updated_at' THEN 1
                        WHEN column_name = 'last_modified' THEN 2
                        WHEN column_name = 'date' THEN 3
                        WHEN column_name = 'created_at' THEN 4
                        ELSE 5
                    END
            """
            date_columns = db.fetch_query(date_column_query, (table_name,))

            if not date_columns:
                return None, None

            # Use the first (most relevant) date column
            date_column = date_columns[0][0]

            # Get the maximum date from that column
            max_date_query = f"SELECT MAX({date_column}) FROM {table_name}"
            result = db.fetch_query(max_date_query)

            if result and result[0][0]:
                return result[0][0], date_column
            return None, date_column

    def get_table_schema_info(self, table_name):
        """Get basic schema information for a table."""
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            query = """
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_name = %s
                AND table_schema = 'public'
                ORDER BY ordinal_position
            """
            result = db.fetch_query(query, (table_name,))
            return result if result else []

    def get_table_null_analysis(self, table_name):
        """Analyze null values in a table."""
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            # Get all columns
            columns_query = """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s
                AND table_schema = 'public'
                ORDER BY ordinal_position
            """
            columns = db.fetch_query(columns_query, (table_name,))

            if not columns:
                return {}

            # Build query to count nulls for each column
            null_checks = []
            for col_name, _ in columns:
                null_checks.append(
                    f"SUM(CASE WHEN {col_name} IS NULL THEN 1 ELSE 0 END) as {col_name}_nulls"
                )

            null_query = f"SELECT {', '.join(null_checks)} FROM {table_name}"
            result = db.fetch_query(null_query)

            if result:
                null_counts = {}
                for i, (col_name, _) in enumerate(columns):
                    null_counts[col_name] = result[0][i]
                return null_counts

            return {}

    def get_table_size_info(self, table_name):
        """Get storage size information for a table."""
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            query = """
                SELECT
                    pg_size_pretty(pg_total_relation_size(%s)) as total_size,
                    pg_size_pretty(pg_relation_size(%s)) as table_size,
                    pg_size_pretty(pg_total_relation_size(%s) - pg_relation_size(%s)) as index_size
            """
            result = db.fetch_query(
                query, (table_name, table_name, table_name, table_name)
            )
            return result[0] if result else (None, None, None)

    def get_api_response_status_summary(self, table_name):
        """Get API response status summary for tables that have this column."""
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            # Check if table has api_response_status column
            status_col_query = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                AND table_schema = 'public'
                AND column_name = 'api_response_status'
            """
            has_status = db.fetch_query(status_col_query, (table_name,))

            if not has_status:
                return {}

            # Get status breakdown
            status_query = f"""
                SELECT api_response_status, COUNT(*) as count
                FROM {table_name}
                GROUP BY api_response_status
                ORDER BY count DESC
            """
            result = db.fetch_query(status_query)

            return dict(result) if result else {}

    def generate_comprehensive_report(self):
        """Generate a comprehensive database monitoring report."""

        tables = self.get_all_table_names()

        if not tables:
            return

        # Summary statistics
        total_rows = 0
        table_summary = []

        for table_name in tables:
            row_count = self.get_table_row_count(table_name)
            last_update, date_column = self.get_table_last_update(table_name)
            total_size, table_size, index_size = self.get_table_size_info(table_name)

            total_rows += row_count

            # Calculate days since last update
            days_since_update = None
            if last_update:
                if isinstance(last_update, datetime):
                    days_since_update = (datetime.now() - last_update).days
                else:
                    # Assume it's a date
                    days_since_update = (datetime.now().date() - last_update).days

            table_summary.append(
                {
                    "Table": table_name,
                    "Rows": f"{row_count:,}",
                    "Last Update": (
                        last_update.strftime("%Y-%m-%d") if last_update else "N/A"
                    ),
                    "Days Ago": (
                        days_since_update if days_since_update is not None else "N/A"
                    ),
                    "Size": total_size or "N/A",
                }
            )

        # Print summary table


        # Detailed analysis for each table

        for table_name in tables:
            self.print_table_details(table_name)

    def print_table_details(self, table_name):
        """Print detailed analysis for a specific table."""

        # Basic stats
        row_count = self.get_table_row_count(table_name)
        last_update, date_column = self.get_table_last_update(table_name)
        total_size, table_size, index_size = self.get_table_size_info(table_name)


        # API response status breakdown
        api_status = self.get_api_response_status_summary(table_name)
        if api_status:
            for _status, count in api_status.items():
                (count / row_count * 100) if row_count > 0 else 0

        # Null analysis (for key columns only to avoid clutter)
        null_analysis = self.get_table_null_analysis(table_name)
        if null_analysis and row_count > 0:
            # Show only columns with significant null rates or important columns
            important_nulls = {}
            for col, null_count in null_analysis.items():
                null_rate = null_count / row_count * 100
                if null_rate > 5 or col in ["symbol", "name", "date", "value", "price"]:
                    important_nulls[col] = f"{null_count:,} ({null_rate:.1f}%)"

            if important_nulls:
                for _col, _null_info in important_nulls.items():
                    pass

    def get_data_freshness_report(self):
        """Generate a report focused on data freshness."""

        tables = self.get_all_table_names()
        freshness_data = []

        for table_name in tables:
            last_update, date_column = self.get_table_last_update(table_name)
            row_count = self.get_table_row_count(table_name)

            if last_update:
                if isinstance(last_update, datetime):
                    days_old = (datetime.now() - last_update).days
                else:
                    days_old = (datetime.now().date() - last_update).days

                # Determine freshness status
                if days_old == 0:
                    status = "🟢 Fresh"
                elif days_old <= 7:
                    status = "🟡 Recent"
                elif days_old <= 30:
                    status = "🟠 Stale"
                else:
                    status = "🔴 Old"

                freshness_data.append(
                    {
                        "Table": table_name,
                        "Last Update": last_update.strftime("%Y-%m-%d"),
                        "Days Old": days_old,
                        "Status": status,
                        "Rows": f"{row_count:,}",
                    }
                )
            else:
                freshness_data.append(
                    {
                        "Table": table_name,
                        "Last Update": "N/A",
                        "Days Old": "N/A",
                        "Status": "⚫ No Date",
                        "Rows": f"{row_count:,}",
                    }
                )

        # Sort by days old (most recent first)
        freshness_data.sort(
            key=lambda x: x["Days Old"] if isinstance(x["Days Old"], int) else 999
        )


    def quick_status(self):
        """Print a quick status overview."""
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            # Get all tables first
            tables_query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """
            tables_result = db.fetch_query(tables_query)
            tables = [row[0] for row in tables_result] if tables_result else []

            # Get row counts for all tables in one connection
            table_counts = []
            total_rows = 0

            for table in tables:
                count_query = f"SELECT COUNT(*) FROM {table}"
                result = db.fetch_query(count_query)
                count = result[0][0] if result else 0
                total_rows += count
                table_counts.append((table, count))


        # Show top 3 tables by row count
        table_counts.sort(key=lambda x: x[1], reverse=True)

        for _table, _count in table_counts[:3]:
            pass


def main():
    """Main function to run database monitoring."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Monitor fin-trade-craft PostgreSQL database"
    )
    parser.add_argument("--quick", action="store_true", help="Show quick status only")
    parser.add_argument(
        "--freshness", action="store_true", help="Show data freshness report"
    )
    parser.add_argument("--table", type=str, help="Show details for specific table")
    parser.add_argument(
        "--full", action="store_true", help="Show comprehensive report (default)"
    )

    args = parser.parse_args()

    monitor = DatabaseMonitor()

    try:
        if args.quick:
            monitor.quick_status()
        elif args.freshness:
            monitor.get_data_freshness_report()
        elif args.table:
            monitor.print_table_details(args.table)
        else:
            # Default to full report
            monitor.generate_comprehensive_report()

    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    main()
