"""Unit tests for reusable row-level quality rules."""

import pandas as pd

from src.quality.rules import (
    duplicate_values,
    invalid_emails,
    invalid_foreign_keys,
    invalid_line_totals,
    negative_values,
    non_positive_values,
    null_values,
    values_not_in,
)


def test_null_values_detects_missing_primary_keys() -> None:
    dataframe = pd.DataFrame({"customer_id": [1, None, 3]})

    assert null_values(dataframe, "customer_id").tolist() == [False, True, False]


def test_duplicate_values_marks_every_duplicate_occurrence() -> None:
    dataframe = pd.DataFrame({"product_id": [1, 2, 2, 3]})

    assert duplicate_values(dataframe, "product_id").tolist() == [
        False,
        True,
        True,
        False,
    ]


def test_invalid_emails_detects_basic_format_errors() -> None:
    dataframe = pd.DataFrame(
        {"email": ["valid@example.com", "missing-at.example.com", None]}
    )

    assert invalid_emails(dataframe).tolist() == [False, True, False]


def test_numeric_rules_detect_invalid_prices_and_quantities() -> None:
    dataframe = pd.DataFrame({"price": [10, 0, -1, None]})

    assert non_positive_values(dataframe, "price").tolist() == [
        False,
        True,
        True,
        True,
    ]
    assert negative_values(dataframe, "price").tolist() == [
        False,
        False,
        True,
        True,
    ]


def test_values_not_in_detects_unapproved_statuses() -> None:
    dataframe = pd.DataFrame({"status": ["paid", "unknown", None]})

    assert values_not_in(dataframe, "status", {"paid", "pending"}).tolist() == [
        False,
        True,
        True,
    ]


def test_invalid_foreign_keys_detects_missing_references() -> None:
    orders = pd.DataFrame({"customer_id": [1, 99, None]})
    customers = pd.DataFrame({"customer_id": [1, 2]})

    assert invalid_foreign_keys(
        orders, "customer_id", customers, "customer_id"
    ).tolist() == [False, True, False]


def test_invalid_line_totals_allows_two_decimal_rounding() -> None:
    dataframe = pd.DataFrame(
        {
            "quantity": [3, 2],
            "unit_price": [3.335, 5.00],
            "line_total": [10.01, 9.99],
        }
    )

    assert invalid_line_totals(dataframe).tolist() == [False, True]
