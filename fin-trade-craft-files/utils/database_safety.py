#!/usr/bin/env python3
"""
Database Safety Utilities - Prevent CASCADE disasters and provide safe operations
"""

import sys
from datetime import datetime
from pathlib import Path

# Add the parent directory to the path so we can import from db
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager


class DatabaseSafetyManager:
    """Provides safety utilities for database operations"""

    def __init__(self, enable_backups=True, enable_checks=True):
        self.db_manager = PostgresDatabaseManager()
        self.enable_backups = enable_backups
        self.enable_checks = enable_checks

    def create_safety_backup(self, table_name, operation_name="unknown"):
        """Create a safety backup before potentially destructive operations"""
        if not self.enable_backups:
            return None

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_table_name = f"safety_backup_{table_name}_{operation_name}_{timestamp}"

        try:
            with self.db_manager as db:
                # Check if source table exists
                if not db.table_exists(table_name):
                    return None

                # Get record count
                count_result = db.fetch_query(f"SELECT COUNT(*) FROM extracted.{table_name}")
                record_count = count_result[0][0] if count_result else 0

                if record_count == 0:
                    return None

                # Create backup table
                backup_sql = f"""
                    CREATE TABLE extracted.{backup_table_name} AS
                    SELECT * FROM extracted.{table_name}
                """
                db.execute_query(backup_sql)

                # Verify backup
                backup_count_result = db.fetch_query(f"SELECT COUNT(*) FROM extracted.{backup_table_name}")
                backup_count = backup_count_result[0][0] if backup_count_result else 0

                if backup_count != record_count:
                    raise Exception(f"Backup verification failed: {backup_count} != {record_count}")

                return backup_table_name

        except Exception as e:
            raise e

    def verify_no_cascade_risk(self, table_name):
        """Check if table has dependent data that could be affected by CASCADE operations"""
        if not self.enable_checks:
            return True

        try:
            with self.db_manager as db:
                # Find all tables that reference this table
                dependent_tables_query = """
                    SELECT
                        tc.table_name as dependent_table,
                        kcu.column_name as dependent_column
                    FROM
                        information_schema.table_constraints AS tc
                        JOIN information_schema.key_column_usage AS kcu
                          ON tc.constraint_name = kcu.constraint_name
                          AND tc.table_schema = kcu.table_schema
                        JOIN information_schema.constraint_column_usage AS ccu
                          ON ccu.constraint_name = tc.constraint_name
                          AND ccu.table_schema = tc.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND ccu.table_name = %s
                    AND tc.table_schema = 'extracted'
                """

                # Get dependent tables with their record counts
                dependent_info = []
                dependent_tables = db.fetch_query(dependent_tables_query, [table_name])

                for row in dependent_tables:
                    dependent_table = row[0]
                    dependent_column = row[1]

                    # Get actual record count for dependent table
                    count_result = db.fetch_query(f"SELECT COUNT(*) FROM extracted.{dependent_table}")
                    record_count = count_result[0][0] if count_result else 0

                    if record_count > 0:
                        dependent_info.append((dependent_table, dependent_column, record_count))

                if dependent_info:
                    total_dependent_records = 0
                    for _dep_table, _dep_column, count in dependent_info:
                        total_dependent_records += count

                    return False
                return True

        except Exception:
            return False

    def safe_delete_table_data(self, table_name, operation_name="data_replacement"):
        """Safely delete all data from a table without CASCADE risks"""

        # Step 1: Create safety backup
        backup_name = self.create_safety_backup(table_name, operation_name)

        # Step 2: Verify no CASCADE risks
        is_safe = self.verify_no_cascade_risk(table_name)

        if not is_safe:
            user_input = input("⚠️ CASCADE risk detected. Continue anyway? (y/N): ")
            if user_input.lower() != 'y':
                if backup_name:
                    self.cleanup_backup(backup_name)
                return False

        # Step 3: Perform safe DELETE (not TRUNCATE)
        try:
            with self.db_manager as db:
                # Use DELETE instead of TRUNCATE to avoid CASCADE
                delete_sql = f"DELETE FROM extracted.{table_name}"
                db.execute_query(delete_sql)

                # Verify deletion
                count_result = db.fetch_query(f"SELECT COUNT(*) FROM extracted.{table_name}")
                remaining_count = count_result[0][0] if count_result else 0

                return remaining_count == 0

        except Exception as e:
            if backup_name:
                pass
            raise e

    def restore_from_backup(self, table_name, backup_table_name):
        """Restore table data from a safety backup"""

        try:
            with self.db_manager as db:
                # Verify backup exists
                if not db.table_exists(backup_table_name):
                    raise Exception(f"Backup table {backup_table_name} does not exist")

                # Clear current table
                db.execute_query(f"DELETE FROM extracted.{table_name}")

                # Restore from backup
                restore_sql = f"""
                    INSERT INTO extracted.{table_name}
                    SELECT * FROM extracted.{backup_table_name}
                """
                db.execute_query(restore_sql)

                # Verify restoration
                original_count = db.fetch_query(f"SELECT COUNT(*) FROM extracted.{backup_table_name}")[0][0]
                restored_count = db.fetch_query(f"SELECT COUNT(*) FROM extracted.{table_name}")[0][0]

                if original_count == restored_count:
                    return True
                raise Exception(f"Restoration verification failed: {original_count} != {restored_count}")

        except Exception as e:
            raise e

    def cleanup_backup(self, backup_table_name):
        """Remove a safety backup table"""
        try:
            with self.db_manager as db:
                if db.table_exists(backup_table_name):
                    db.execute_query(f"DROP TABLE extracted.{backup_table_name}")
                else:
                    pass
        except Exception:
            pass

    def list_safety_backups(self):
        """List all safety backup tables"""
        try:
            with self.db_manager as db:
                backup_tables_query = """
                    SELECT table_name,
                           pg_size_pretty(pg_total_relation_size('extracted.'||table_name)) as size
                    FROM information_schema.tables
                    WHERE table_schema = 'extracted'
                    AND table_name LIKE 'safety_backup_%'
                    ORDER BY table_name
                """

                backups = db.fetch_query(backup_tables_query)

                if backups:
                    for table_name, _size in backups:
                        # Get record count
                        count_result = db.fetch_query(f"SELECT COUNT(*) FROM extracted.{table_name}")
                        count_result[0][0] if count_result else 0
                else:
                    pass

        except Exception:
            pass

    def verify_table_integrity(self, table_name):
        """Verify table integrity and foreign key constraints"""
        try:
            with self.db_manager as db:
                # Check for broken foreign key references

                # This is a simplified integrity check - in practice you'd want more sophisticated checks

                # Check if table exists and has expected structure
                if db.table_exists(table_name):
                    count_result = db.fetch_query(f"SELECT COUNT(*) FROM extracted.{table_name}")
                    count_result[0][0] if count_result else 0
                    return True
                return False

        except Exception:
            return False


def main():
    """Test the safety manager"""

    # List current backups
    safety_manager = DatabaseSafetyManager()
    safety_manager.list_safety_backups()

    # Check CASCADE risk for listing_status
    safety_manager_2 = DatabaseSafetyManager()
    safety_manager_2.verify_no_cascade_risk('listing_status')


if __name__ == "__main__":
    main()
