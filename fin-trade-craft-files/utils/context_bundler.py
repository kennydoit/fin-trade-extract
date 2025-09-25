"""
Generates a portable bundle from a PostgreSQL schema:

schema.sql (via pg_dump)
data_dictionary.md (information_schema + pg_catalog)
samples/*.csv and *.parquet (random samples per table)
erd/erd.dot and erd/erd.png (if graphviz/dot available)
Usage (example):

python context_bundler.py \
--db-url "postgresql+psycopg://USER:PASS@HOST:5432/fin_trade_craft" \
--schema extracted \
--output ./utils/context_bundle \
--sample-rows 200 \
--pg-dump-path pg_dump \
--skip-parquet false
"""

from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
from collections.abc import Iterable
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2 import sql

log = logging.getLogger(__name__)


def str2bool(value: str) -> bool:
    """Convert common string representations to a boolean."""

    return value.lower() in {"1", "true", "t", "yes", "y"}


def sanitize_db_url(db_url: str) -> str:
    """Remove SQLAlchemy-only driver info from a PostgreSQL URL."""

    if db_url.startswith("postgresql+"):
        return "postgresql" + db_url[db_url.index("://") :]
    return db_url


def run_pg_dump(
    db_url: str, schema: str, output_dir: Path, pg_dump_path: str = "pg_dump"
) -> None:
    """Dump schema using pg_dump."""

    schema_file = output_dir / "schema.sql"
    cmd = [
        pg_dump_path,
        "--schema-only",
        f"--schema={schema}",
        f"--dbname={db_url}",
    ]
    log.info("Running pg_dump: %s", " ".join(cmd))
    with schema_file.open("w", encoding="utf-8") as fh:
        subprocess.run(cmd, stdout=fh, check=True)


def fetch_tables(conn, schema: str) -> list[str]:
    """Return a list of table names for *schema*."""

    query = sql.SQL(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
    )
    with conn.cursor() as cur:
        cur.execute(query, (schema,))
        return [row[0] for row in cur.fetchall()]


def table_comment(conn, schema: str, table: str) -> str | None:
    """Fetch comment for *table* if available."""

    query = sql.SQL(
        """
        SELECT obj_description(pc.oid)
        FROM pg_catalog.pg_class pc
        JOIN pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace
        WHERE pn.nspname = %s AND pc.relname = %s
        """
    )
    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        row = cur.fetchone()
        return row[0] if row else None


def column_info(conn, schema: str, table: str) -> Iterable[tuple[str, str, str, str, str]]:
    """Yield column metadata for *table*."""

    query = sql.SQL(
        """
        SELECT c.column_name, c.data_type, c.is_nullable, c.column_default, pgd.description
        FROM information_schema.columns c
        JOIN pg_catalog.pg_class pc ON pc.relname = c.table_name
        JOIN pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace AND pn.nspname = c.table_schema
        LEFT JOIN pg_catalog.pg_description pgd ON pgd.objoid = pc.oid AND pgd.objsubid = c.ordinal_position
        WHERE c.table_schema = %s AND c.table_name = %s
        ORDER BY c.ordinal_position
        """
    )
    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        yield from cur.fetchall()


def build_data_dictionary(conn, schema: str, output_dir: Path) -> None:
    """Create a Markdown data dictionary for *schema*."""

    data_dict_file = output_dir / "data_dictionary.md"
    tables = fetch_tables(conn, schema)
    lines = [f"# Data Dictionary for schema `{schema}`\n"]
    for table in tables:
        comment = table_comment(conn, schema, table)
        lines.append(f"\n## {table}\n")
        if comment:
            lines.append(f"{comment}\n\n")
        lines.append("| Column | Data Type | Nullable | Default | Description |\n")
        lines.append("|--------|-----------|----------|---------|-------------|\n")
        for col_name, data_type, nullable, default, description in column_info(
            conn, schema, table
        ):
            default_val = default if default is not None else ""
            desc_val = description if description is not None else ""
            lines.append(
                f"| {col_name} | {data_type} | {nullable} | {default_val} | {desc_val} |\n"
            )
    data_dict_file.write_text("".join(lines), encoding="utf-8")


def sample_table(  # noqa: PLR0913
    conn, schema: str, table: str, sample_rows: int, samples_dir: Path, skip_parquet: bool
) -> None:
    """Write random samples from *table* to CSV and optionally Parquet."""

    query = sql.SQL(
        "SELECT * FROM {}.{} ORDER BY random() LIMIT {}"
    ).format(sql.Identifier(schema), sql.Identifier(table), sql.Literal(sample_rows))
    df = pd.read_sql_query(query.as_string(conn), conn)
    csv_path = samples_dir / f"{table}.csv"
    df.to_csv(csv_path, index=False)
    if not skip_parquet:
        try:
            df.to_parquet(samples_dir / f"{table}.parquet", index=False)
        except Exception as exc:  # pragma: no cover - optional dependency
            log.warning("Parquet support not available: %s", exc)


def generate_samples(
    conn, schema: str, output_dir: Path, sample_rows: int, skip_parquet: bool
) -> None:
    samples_dir = output_dir / "samples"
    samples_dir.mkdir(parents=True, exist_ok=True)
    for table in fetch_tables(conn, schema):
        sample_table(conn, schema, table, sample_rows, samples_dir, skip_parquet)


def build_erd(conn, schema: str, output_dir: Path) -> None:
    """Generate a simple ERD using Graphviz."""

    erd_dir = output_dir / "erd"
    erd_dir.mkdir(parents=True, exist_ok=True)
    dot_path = erd_dir / "erd.dot"
    png_path = erd_dir / "erd.png"

    query = sql.SQL(
        """
        SELECT tc.table_name AS source_table,
               kcu.column_name AS source_column,
               ccu.table_name AS target_table,
               ccu.column_name AS target_column
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = %s
        """
    )
    edges = []
    with conn.cursor() as cur:
        cur.execute(query, (schema,))
        for src_table, src_col, dst_table, dst_col in cur.fetchall():
            label = f"{src_col}→{dst_col}"
            edges.append(f'  "{src_table}" -> "{dst_table}" [label="{label}"];')
    lines = ["digraph ERD {", "  rankdir=LR;", *edges, "}"]
    dot_path.write_text("\n".join(lines), encoding="utf-8")

    if shutil.which("dot"):
        try:
            subprocess.run(
                ["dot", "-Tpng", str(dot_path), "-o", str(png_path)], check=True
            )
        except Exception as exc:  # pragma: no cover - environment dependent
            log.warning("Failed to create ERD PNG: %s", exc)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-url", required=True, help="PostgreSQL connection URL")
    parser.add_argument("--schema", required=True, help="Schema name to export")
    parser.add_argument(
        "--output", default="./utils/context_bundle", help="Output directory"
    )
    parser.add_argument("--sample-rows", type=int, default=200)
    parser.add_argument("--pg-dump-path", default="pg_dump")
    parser.add_argument(
        "--skip-parquet", type=str2bool, default=False, help="Skip parquet export"
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    db_url = sanitize_db_url(args.db_url)
    conn = psycopg2.connect(db_url)
    try:
        run_pg_dump(args.db_url, args.schema, output_dir, args.pg_dump_path)
        build_data_dictionary(conn, args.schema, output_dir)
        generate_samples(conn, args.schema, output_dir, args.sample_rows, args.skip_parquet)
        build_erd(conn, args.schema, output_dir)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
