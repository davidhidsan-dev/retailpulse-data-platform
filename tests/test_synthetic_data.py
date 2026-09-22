"""In-memory tests for the RetailPulse synthetic source data."""

import numpy as np
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
    INITIAL_CATALOG_RATIO,
    LAST_NAMES,
    MAX_ORDER_LOOKBACK_DAYS,
    ORDER_STATUSES,
    PAYMENT_METHODS,
    PAYMENT_STATUSES,
    PRODUCT_CATALOG,
    REFERENCE_DATE,
    SYNTHETIC_BEHAVIOR_SEGMENTS,
    VARIANT_PRICE_MULTIPLIERS,
    _apply_variant_price,
    _order_window,
    _timestamp_between,
    _timestamp_days_ago,
    generate_all_data,
    generate_order_items,
    generate_products,
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


def test_timestamp_between_rejects_an_inverted_interval() -> None:
    generator = np.random.default_rng(42)
    start = REFERENCE_DATE
    end = REFERENCE_DATE - pd.Timedelta(days=1)

    with pytest.raises(ValueError, match="end must be greater"):
        _timestamp_between(generator, start, end)


def test_timestamp_between_accepts_a_single_possible_timestamp() -> None:
    generator = np.random.default_rng(42)

    assert _timestamp_between(generator, REFERENCE_DATE, REFERENCE_DATE) == (
        REFERENCE_DATE
    )


@pytest.mark.parametrize(
    ("minimum_days", "maximum_days"),
    [(-1, 10), (10, 9)],
)
def test_timestamp_days_ago_rejects_invalid_ranges(
    minimum_days: int,
    maximum_days: int,
) -> None:
    generator = np.random.default_rng(42)

    with pytest.raises(ValueError, match="maximum_days"):
        _timestamp_days_ago(generator, minimum_days, maximum_days)


def test_order_window_rejects_unknown_segments() -> None:
    with pytest.raises(ValueError, match="Unsupported synthetic behavior segment"):
        _order_window(REFERENCE_DATE - pd.Timedelta(days=100), "unknown")


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


def test_every_product_variant_has_a_price_multiplier() -> None:
    configured_variants = {
        variant
        for category_variants in CATEGORY_VARIANTS.values()
        for variant in category_variants
    }

    assert set(VARIANT_PRICE_MULTIPLIERS) == configured_variants
    assert (
        VARIANT_PRICE_MULTIPLIERS["Premium"]
        > VARIANT_PRICE_MULTIPLIERS["Plus"]
        > VARIANT_PRICE_MULTIPLIERS["Basic"]
    )


def test_product_variant_adjusts_the_same_base_price() -> None:
    assert _apply_variant_price(100.0, "Basic") == 85.0
    assert _apply_variant_price(100.0, "Plus") == 110.0
    assert _apply_variant_price(100.0, "Premium") == 130.0


def test_initial_catalog_covers_twenty_percent_of_products() -> None:
    products = generate_products(10, rng=np.random.default_rng(42))
    simulation_start = REFERENCE_DATE - pd.Timedelta(
        days=MAX_ORDER_LOOKBACK_DAYS
    )
    initial_catalog = products.loc[products["created_at"].le(simulation_start)]

    assert INITIAL_CATALOG_RATIO == 0.20
    assert len(initial_catalog) == 2


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


def test_products_exist_before_their_order_items(
    synthetic_data: dict[str, pd.DataFrame],
) -> None:
    sales = (
        synthetic_data["order_items"]
        .merge(
            synthetic_data["orders"][["order_id", "order_date"]],
            on="order_id",
            validate="many_to_one",
        )
        .merge(
            synthetic_data["products"][["product_id", "created_at"]],
            on="product_id",
            validate="many_to_one",
        )
    )

    assert sales["created_at"].le(sales["order_date"]).all()


def test_order_items_only_choose_products_available_on_order_date() -> None:
    order_date = REFERENCE_DATE - pd.Timedelta(days=100)
    orders = pd.DataFrame(
        {
            "order_id": [1],
            "customer_id": [1],
            "order_date": [order_date],
            "order_status": ["completed"],
            "country": ["Spain"],
        }
    )
    products = pd.DataFrame(
        {
            "product_id": [10, 20],
            "unit_price": [25.0, 50.0],
            "created_at": [
                order_date - pd.Timedelta(days=1),
                order_date + pd.Timedelta(days=1),
            ],
        }
    )

    items = generate_order_items(
        orders, products, rng=np.random.default_rng(42)
    )

    assert items["product_id"].tolist() == [10]


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
