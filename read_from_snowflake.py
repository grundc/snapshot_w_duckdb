"""Read data from Snowflake and save to a local DuckDB database."""

from snowflake.snowpark import Session
import duckdb
import json
import argparse
import shutil
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Read data from Snowflake and save to DuckDB.")
    parser.add_argument("--sf_connection", type=str, required=True, help="Snowflake connection name in connections.toml")
    parser.add_argument("--duckdb", type=str, required=True, help="Name of the duckdb file to save the data")
    return parser.parse_args()


def load_tables(path: str = "tables.json") -> list[tuple[str, str]]:
    """Load table definitions from a JSON file."""
    with open(path, "r") as f:
        tables_list = json.load(f)
    return [tuple(item) for item in tables_list["tables"]]


def snapshot_tables(
    sf_connection: str,
    duckdb_path: str,
    tables: list[tuple[str, str]],
) -> None:
    """Export tables from Snowflake and import them into DuckDB."""
    session = Session.builder.config("connection_name", sf_connection).create()
    duck_conn = duckdb.connect(duckdb_path)

    try:
        for table, where_clause in tables:
            try:
                query = f"SELECT * FROM {table} WHERE {where_clause}"
                df = session.sql(query)

                remote_file_path = f"{session.get_session_stage()}/{table}"
                copy_result = df.write.parquet(remote_file_path, overwrite=True, header=True)
                logger.info("Exported %s rows from %s to Snowflake stage", copy_result[0].rows_unloaded, table)

                session.file.get(remote_file_path, f"./stage/{table}")

                duck_conn.execute(f"DROP TABLE IF EXISTS {table}")
                duck_conn.execute(f"""
                    CREATE TABLE {table} AS
                    SELECT * FROM read_parquet('stage/{table}/*.parquet')
                """)

                logger.info("Imported %s into DuckDB", table)
            except Exception:
                logger.exception("Error processing table %s — skipping", table)
    finally:
        session.close()
        duck_conn.close()
        shutil.rmtree("./stage", ignore_errors=True)

    logger.info("Data import to DuckDB completed.")


def main() -> None:
    """Entry point."""
    args = parse_args()
    tables = load_tables()
    snapshot_tables(args.sf_connection, args.duckdb, tables)


if __name__ == "__main__":
    main()
