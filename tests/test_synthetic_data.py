"""In-memory tests for the RetailPulse synthetic source data."""

import pandas as pd
import pytest

from src.synthetic_data.generate_retail_data import (
    CATEGORY_VARIANTS,
    COUNTRY_CITIES,
    DEFAULT_CUSTOMERS,
    DEFAULT_ORDERS,
    DEFAULT_PRODUCTS,
    DEFAULT_SEED,
    FIRST_NAMES,
    LAST_NAMES,
    ORDER_STATUSES,
    PAYMENT_METHODS,
    PAYMENT_STATUSES,
    PRODUCT_CATALOG,
    REFERENCE_DATE,
    SYNTHETIC_BEHAVIOR_SEGMENTS,
    generate_all_data,
    parse_args,
)


@pytest.fixture(scope="module")
def synthetic_data() -> dict[str, pd.DataFrame]:
    return generate_all_data(
        n_customers=300,
        n_products=80,
        n_orders=1_000,
        seed=42,
    )


@pytest.mark.parametrize(
    ("table", "primary_key"),
    [
        ("customers", "customer_id"),
        ("products", "product_id"),
        ("orders", "order_id"),
        ("order_items", "order_item_id"),
        ("payments", "payment_id"),
    ],
)
def test_primary_keys_are_unique(
    synthetic_data: dict[str, pd.DataFrame],
    table: str,
    primary_key: str,
) -> None:
    assert synthetic_data[table][primary_key].is_unique


def test_customer_emails_are_unique(
    synthetic_data: dict[str, pd.DataFrame],
) -> None:
    assert synthetic_data["customers"]["email"].is_unique


def test_product_skus_are_unique_and_well_formed(
    synthetic_data: dict[str, pd.DataFrame],
) -> None:
    skus = synthetic_data["products"]["sku"]
    assert skus.is_unique
    assert skus.str.fullmatch(r"[A-Z]{4}-[A-Z]{2}-\d{4}").all()


def test_reference_catalog_has_required_variety(
    synthetic_data: dict[str, pd.DataFrame],
) -> None:
    assert len(FIRST_NAMES) >= 30
    assert len(LAST_NAMES) >= 30
    assert len(COUNTRY_CITIES) >= 5
    assert all(len(cities) >= 5 for cities in COUNTRY_CITIES.values())
    assert all(len(entries) >= 10 for _, entries in PRODUCT_CATALOG.values())

    variants = tuple(
        variant
        for category_variants in CATEGORY_VARIANTS.values()
        for variant in category_variants
    )
    assert synthetic_data["products"]["product_name"].str.endswith(variants).all()


def test_product_prices_are_positive(
    synthetic_data: dict[str, pd.DataFrame],
) -> None:
    assert synthetic_data["products"]["unit_price"].gt(0).all()


def test_inventory_quantities_are_non_negative(
    synthetic_data: dict[str, pd.DataFrame],
) -> None:
    inventory = synthetic_data["inventory"]
    assert inventory["stock_quantity"].ge(0).all()
    assert inventory["reorder_level"].ge(0).all()


def test_orders_reference_existing_customers(
    synthetic_data: dict[str, pd.DataFrame],
) -> None:
    customer_ids = set(synthetic_data["customers"]["customer_id"])
    assert set(synthetic_data["orders"]["customer_id"]).issubset(customer_ids)


def test_order_items_reference_existing_orders(
    synthetic_data: dict[str, pd.DataFrame],
) -> None:
    order_ids = set(synthetic_data["orders"]["order_id"])
    assert set(synthetic_data["order_items"]["order_id"]).issubset(order_ids)


def test_order_items_reference_existing_products(
    synthetic_data: dict[str, pd.DataFrame],
) -> None:
    product_ids = set(synthetic_data["products"]["product_id"])
    assert set(synthetic_data["order_items"]["product_id"]).issubset(product_ids)


def test_inventory_references_existing_products(
    synthetic_data: dict[str, pd.DataFrame],
) -> None:
    product_ids = set(synthetic_data["products"]["product_id"])
    assert set(synthetic_data["inventory"]["product_id"]) == product_ids


def test_relationship_tables_do_not_use_descriptive_attributes(
    synthetic_data: dict[str, pd.DataFrame],
) -> None:
    descriptive_attributes = {
        "first_name",
        "last_name",
        "email",
        "sku",
        "product_name",
    }
    for table in ("inventory", "orders", "order_items", "payments"):
        assert descriptive_attributes.isdisjoint(synthetic_data[table].columns)


def test_line_totals_are_calculated_correctly(
    synthetic_data: dict[str, pd.DataFrame],
) -> None:
    order_items = synthetic_data["order_items"]
    expected = (order_items["quantity"] * order_items["unit_price"]).round(2)
    pd.testing.assert_series_equal(
        order_items["line_total"],
        expected,
        check_names=False,
    )


def test_payments_match_order_totals(
    synthetic_data: dict[str, pd.DataFrame],
) -> None:
    expected = (
        synthetic_data["order_items"]
        .groupby("order_id")["line_total"]
        .sum()
        .round(2)
        .sort_index()
    )
    actual = (
        synthetic_data["payments"]
        .set_index("order_id")["payment_amount"]
        .sort_index()
    )
    pd.testing.assert_series_equal(actual, expected, check_names=False)


def test_statuses_methods_and_segments_are_controlled(
    synthetic_data: dict[str, pd.DataFrame],
) -> None:
    customers = synthetic_data["customers"]
    orders = synthetic_data["orders"]
    payments = synthetic_data["payments"]

    assert "customer_segment" not in customers.columns
    assert set(customers["synthetic_behavior_segment"]).issubset(
        SYNTHETIC_BEHAVIOR_SEGMENTS
    )
    assert set(orders["order_status"]).issubset(ORDER_STATUSES)
    assert set(payments["payment_status"]).issubset(PAYMENT_STATUSES)
    assert set(payments["payment_method"]).issubset(PAYMENT_METHODS)


def test_synthetic_behavior_segments_create_purchase_patterns(
    synthetic_data: dict[str, pd.DataFrame],
) -> None:
    customers = synthetic_data["customers"]
    orders = synthetic_data["orders"].merge(
        customers[["customer_id", "synthetic_behavior_segment"]],
        on="customer_id",
        validate="many_to_one",
    )
    items = synthetic_data["order_items"]

    orders_per_customer = orders.groupby("synthetic_behavior_segment").size() / (
        customers.groupby("synthetic_behavior_segment").size()
    )
    tickets = items.groupby("order_id")["line_total"].sum().rename("ticket")
    tickets_by_segment = orders.join(tickets, on="order_id").groupby(
        "synthetic_behavior_segment"
    )["ticket"].mean()

    assert orders_per_customer["frequent"] > orders_per_customer["occasional"]
    assert orders_per_customer["frequent"] > orders_per_customer["high_value"]
    assert orders_per_customer["high_value"] < orders_per_customer["occasional"]
    assert tickets_by_segment["high_value"] > tickets_by_segment["frequent"]

    inactive_dates = orders.loc[
        orders["synthetic_behavior_segment"] == "inactive", "order_date"
    ]
    new_dates = orders.loc[
        orders["synthetic_behavior_segment"] == "new", "order_date"
    ]
    assert inactive_dates.max() <= REFERENCE_DATE - pd.Timedelta(days=365)
    assert new_dates.min() >= REFERENCE_DATE - pd.Timedelta(days=90)


def test_same_seed_reproduces_every_table() -> None:
    first = generate_all_data(40, 20, 80, seed=777)
    second = generate_all_data(40, 20, 80, seed=777)

    for table in first:
        pd.testing.assert_frame_equal(first[table], second[table])


def test_generation_respects_requested_volumes() -> None:
    data = generate_all_data(25, 10, 50, seed=123)

    assert len(data["customers"]) == 25
    assert len(data["products"]) == 10
    assert len(data["inventory"]) == 10
    assert len(data["orders"]) == 50
    assert len(data["payments"]) == 50
    assert len(data["order_items"]) >= 50


def test_cli_arguments_control_volumes_and_seed() -> None:
    args = parse_args(
        [
            "--customers",
            "25",
            "--products",
            "10",
            "--orders",
            "50",
            "--seed",
            "123",
        ]
    )
    assert (args.customers, args.products, args.orders, args.seed) == (
        25,
        10,
        50,
        123,
    )

    defaults = parse_args([])
    assert (
        defaults.customers,
        defaults.products,
        defaults.orders,
        defaults.seed,
    ) == (
        DEFAULT_CUSTOMERS,
        DEFAULT_PRODUCTS,
        DEFAULT_ORDERS,
        DEFAULT_SEED,
    )
