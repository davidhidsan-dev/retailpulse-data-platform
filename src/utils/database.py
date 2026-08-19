"""PostgreSQL helpers used by the source-data workflow."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine, create_engine

from src.utils.config import get_database_url


LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCHEMA_PATH = PROJECT_ROOT / "sql" / "source_schema.sql"
SOURCE_TABLE_LOAD_ORDER = (
    "customers",
    "products",
    "inventory",
    "orders",
    "order_items",
    "payments",
)


def get_engine() -> Engine:
    """Create a SQLAlchemy engine using environment-based configuration."""
    return create_engine(get_database_url(), pool_pre_ping=True)


def execute_sql_file(
    sql_path: Path | str = DEFAULT_SCHEMA_PATH,
    engine: Engine | None = None,
) -> None:
    """Execute a SQL file in a single database transaction."""
    path = Path(sql_path)
    if not path.is_file():
        raise FileNotFoundError(f"SQL file not found: {path}")

    sql = path.read_text(encoding="utf-8")
    database_engine = engine if engine is not None else get_engine()
    with database_engine.begin() as connection:
        connection.exec_driver_sql(sql)

    LOGGER.info("Executed SQL schema: %s", path)


def load_dataframes_to_postgres(
    dataframes: Mapping[str, pd.DataFrame],
    engine: Engine | None = None,
) -> dict[str, int]:
    """Replace source-table contents with DataFrames in dependency order."""
    missing = [table for table in SOURCE_TABLE_LOAD_ORDER if table not in dataframes]
    if missing:
        raise ValueError(f"Missing DataFrames for tables: {', '.join(missing)}")

    database_engine = engine if engine is not None else get_engine()
    loaded_counts: dict[str, int] = {}

    with database_engine.begin() as connection:
        connection.exec_driver_sql(
            "TRUNCATE TABLE payments, order_items, orders, inventory, "
            "products, customers RESTART IDENTITY CASCADE"
        )

        for table in SOURCE_TABLE_LOAD_ORDER:
            dataframe = dataframes[table]
            dataframe.to_sql(
                table,
                con=connection,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1_000,
            )
            loaded_counts[table] = len(dataframe)
            LOGGER.info("Loaded %s rows into %s", len(dataframe), table)

    return loaded_counts


def initialize_source_schema() -> None:
    """Create the RetailPulse source tables when they do not exist."""
    execute_sql_file()


def main() -> None:
    """CLI entry point for database initialization."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    initialize_source_schema()
    LOGGER.info("RetailPulse source schema is ready.")


if __name__ == "__main__":
    main()
