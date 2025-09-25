import sqlite3
from pathlib import Path

import pandas as pd


class DatabaseManager:
    """A class to manage SQLite database connections and queries."""

    def __init__(self, db_path):
        self.db_path = Path(db_path)
        self.connection = None

        # Create the db directory if it doesn't exist
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self):
        """Connect to the SQLite database."""
        self.connection = sqlite3.connect(str(self.db_path))
        # Enable foreign key constraints
        self.connection.execute("PRAGMA foreign_keys = ON")

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()

    def execute_query(self, query, params=None):
        """Execute a query against the database."""
        if not self.connection:
            raise Exception("Database connection is not established.")

        cursor = self.connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        self.connection.commit()

        return cursor.fetchall()

    def execute_many(self, query, params_list):
        """Execute a query with multiple parameter sets."""
        if not self.connection:
            raise Exception("Database connection is not established.")

        cursor = self.connection.cursor()
        cursor.executemany(query, params_list)
        self.connection.commit()

        return cursor.rowcount

    def fetch_query(self, query, params=None):
        """Execute a SELECT query and return results."""
        if not self.connection:
            raise Exception("Database connection is not established.")

        cursor = self.connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        return cursor.fetchall()

    def fetch_dataframe(self, query, params=None):
        """Execute a SELECT query and return results as a pandas DataFrame."""
        if not self.connection:
            raise Exception("Database connection is not established.")

        return pd.read_sql_query(query, self.connection, params=params)

    def initialize_schema(self, schema_file_path):
        """Initialize the database schema from a SQL file."""
        if not self.connection:
            self.connect()

        schema_path = Path(schema_file_path)
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        with schema_path.open() as file:
            schema_sql = file.read()

        # Split the SQL file into individual statements and execute them
        statements = [stmt.strip() for stmt in schema_sql.split(";") if stmt.strip()]

        for statement in statements:
            if statement:
                self.connection.execute(statement)

        self.connection.commit()

    def table_exists(self, table_name):
        """Check if a table exists in the database."""
        if not self.connection:
            raise Exception("Database connection is not established.")

        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name=?
        """,
            (table_name,),
        )

        return cursor.fetchone() is not None

    def get_table_info(self, table_name):
        """Get column information for a table."""
        if not self.connection:
            raise Exception("Database connection is not established.")

        cursor = self.connection.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")

        return cursor.fetchall()

    def __enter__(self):
        """Context manager entry."""
        if not self.connection:
            self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
