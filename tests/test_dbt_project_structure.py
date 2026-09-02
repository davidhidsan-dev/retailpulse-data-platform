"""Static contract tests for the initial RetailPulse dbt project."""

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_ROOT = PROJECT_ROOT / "dbt"
STAGING_ROOT = DBT_ROOT / "models" / "staging"
MARTS_ROOT = DBT_ROOT / "models" / "marts"

SOURCE_TABLES = (
    "customers",
    "products",
    "inventory",
    "orders",
    "order_items",
    "payments",
)
STAGING_TEXT_COLUMNS = {
    "customers": (
        "first_name",
        "last_name",
        "email",
        "country",
        "city",
        "synthetic_behavior_segment",
        "ingestion_id",
        "source_system",
        "source_table",
        "quality_run_id",
    ),
    "products": (
        "sku",
        "product_name",
        "category",
        "ingestion_id",
        "source_system",
        "source_table",
        "quality_run_id",
    ),
    "inventory": (
        "ingestion_id",
        "source_system",
        "source_table",
        "quality_run_id",
    ),
    "orders": (
        "order_status",
        "country",
        "ingestion_id",
        "source_system",
        "source_table",
        "quality_run_id",
    ),
    "order_items": (
        "ingestion_id",
        "source_system",
        "source_table",
        "quality_run_id",
    ),
    "payments": (
        "payment_method",
        "payment_status",
        "ingestion_id",
        "source_system",
        "source_table",
        "quality_run_id",
    ),
}
MART_MODELS = (
    "dim_customer",
    "dim_product",
    "dim_date",
    "fact_sales",
    "fact_inventory",
)


def test_dbt_project_contains_expected_files() -> None:
    expected = [
        DBT_ROOT / "dbt_project.yml",
        DBT_ROOT / "profiles.yml.example",
        STAGING_ROOT / "sources.yml",
        MARTS_ROOT / "schema.yml",
    ]
    expected.extend(STAGING_ROOT / f"stg_{table}.sql" for table in SOURCE_TABLES)
    expected.extend(MARTS_ROOT / f"{model}.sql" for model in MART_MODELS)

    assert all(path.is_file() for path in expected)


def test_dbt_project_uses_retailpulse_profile_and_model_layers() -> None:
    project = (DBT_ROOT / "dbt_project.yml").read_text(encoding="utf-8")
    profile = (DBT_ROOT / "profiles.yml.example").read_text(encoding="utf-8")

    assert "name: retailpulse" in project
    assert "profile: retailpulse" in project
    assert "+materialized: view" in project
    assert "+materialized: table" in project
    assert "type: postgres" in profile
    assert "env_var('POSTGRES_PASSWORD'" in profile


def test_sources_point_to_warehouse_source_schema() -> None:
    sources = (STAGING_ROOT / "sources.yml").read_text(encoding="utf-8")

    assert "name: warehouse_source" in sources
    assert "schema: warehouse_source" in sources
    for table in SOURCE_TABLES:
        assert f"- name: {table}" in sources


@pytest.mark.parametrize("table", SOURCE_TABLES)
def test_staging_models_select_from_dbt_source(table: str) -> None:
    sql = (STAGING_ROOT / f"stg_{table}.sql").read_text(encoding="utf-8")
    normalized_sql = sql.lower()

    assert f"source('warehouse_source', '{table}')" in sql
    assert "quality_run_id" in sql
    assert "quality_checked_at" in sql
    assert "select *" not in normalized_sql
    assert " join " not in normalized_sql
    assert "group by" not in normalized_sql
    for column in STAGING_TEXT_COLUMNS[table]:
        assert f"cast({column} as text) as {column}" in normalized_sql


def test_fact_sales_declares_order_item_grain_and_required_joins() -> None:
    sql = (MARTS_ROOT / "fact_sales.sql").read_text(encoding="utf-8")
    required_columns = {
        "order_item_id",
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "payment_id",
        "order_status",
        "payment_status",
        "payment_method",
        "quantity",
        "unit_price",
        "line_total",
    }

    assert all(column in sql for column in required_columns)
    assert "ref('stg_order_items')" in sql
    assert "ref('stg_orders')" in sql
    assert "ref('stg_payments')" in sql


def test_fact_inventory_calculates_low_stock_at_product_grain() -> None:
    sql = (MARTS_ROOT / "fact_inventory.sql").read_text(encoding="utf-8")

    assert "ref('stg_inventory')" in sql
    assert "ref('stg_products')" in sql
    assert "stock_quantity <= inventory.reorder_level as is_low_stock" in sql


def test_dim_date_builds_a_calendar_from_order_dates() -> None:
    sql = (MARTS_ROOT / "dim_date.sql").read_text(encoding="utf-8")

    assert "ref('stg_orders')" in sql
    assert "generate_series" in sql
    for column in ("date_day", "year", "month", "day", "month_name", "quarter"):
        assert column in sql


def test_mart_schema_declares_key_and_domain_tests() -> None:
    schema = (MARTS_ROOT / "schema.yml").read_text(encoding="utf-8")

    for model in MART_MODELS:
        assert f"- name: {model}" in schema
    assert "data_tests:" in schema
    assert "- unique" in schema
    assert "- not_null" in schema
    assert schema.count("accepted_values:") == 3
    assert schema.count("relationships:") == 3
    assert schema.count("to: ref('dim_customer')") == 1
    assert schema.count("to: ref('dim_product')") == 2
