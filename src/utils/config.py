"""Environment-based configuration for RetailPulse."""

from __future__ import annotations

import os

from dotenv import load_dotenv
from sqlalchemy.engine import URL


REQUIRED_POSTGRES_VARIABLES = (
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
)


def get_database_url() -> str:
    """Build a PostgreSQL SQLAlchemy URL from environment variables."""
    load_dotenv()

    values = {name: os.getenv(name) for name in REQUIRED_POSTGRES_VARIABLES}
    missing = [name for name, value in values.items() if not value]
    if missing:
        missing_variables = ", ".join(missing)
        raise RuntimeError(
            f"Missing required environment variables: {missing_variables}. "
            "Copy .env.example to .env and provide local values."
        )

    try:
        port = int(values["POSTGRES_PORT"])
    except ValueError as error:
        raise RuntimeError("POSTGRES_PORT must be an integer.") from error

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=values["POSTGRES_USER"],
        password=values["POSTGRES_PASSWORD"],
        host=values["POSTGRES_HOST"],
        port=port,
        database=values["POSTGRES_DB"],
    )
    return url.render_as_string(hide_password=False)
