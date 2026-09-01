"""Small, reusable row-level data-quality rules."""

from __future__ import annotations

import re
from collections.abc import Iterable

import pandas as pd


EMAIL_PATTERN = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def null_values(dataframe: pd.DataFrame, column: str) -> pd.Series:
    """Return a mask for null values in a required column."""
    return dataframe[column].isna()


def duplicate_values(dataframe: pd.DataFrame, column: str) -> pd.Series:
    """Return a mask for every non-null occurrence of a duplicate value."""
    values = dataframe[column]
    return values.notna() & values.duplicated(keep=False)


def invalid_emails(dataframe: pd.DataFrame, column: str = "email") -> pd.Series:
    """Return a mask for non-null strings that fail a basic email pattern."""
    values = dataframe[column].astype("string")
    return values.notna() & ~values.str.fullmatch(EMAIL_PATTERN, na=False)


def values_not_in(
    dataframe: pd.DataFrame,
    column: str,
    allowed_values: Iterable[str],
) -> pd.Series:
    """Return a mask for null values or values outside an allowed domain."""
    values = dataframe[column]
    return values.isna() | ~values.isin(set(allowed_values))


def non_positive_values(dataframe: pd.DataFrame, column: str) -> pd.Series:
    """Return a mask for null, non-numeric or numeric values not above zero."""
    values = pd.to_numeric(dataframe[column], errors="coerce")
    return values.isna() | values.le(0)


def negative_values(dataframe: pd.DataFrame, column: str) -> pd.Series:
    """Return a mask for null, non-numeric or negative numeric values."""
    values = pd.to_numeric(dataframe[column], errors="coerce")
    return values.isna() | values.lt(0)


def invalid_foreign_keys(
    dataframe: pd.DataFrame,
    column: str,
    referenced_dataframe: pd.DataFrame,
    referenced_column: str,
) -> pd.Series:
    """Return a mask for non-null keys absent from a referenced DataFrame."""
    values = dataframe[column]
    referenced_values = referenced_dataframe[referenced_column].dropna()
    return values.notna() & ~values.isin(referenced_values)


def invalid_line_totals(dataframe: pd.DataFrame) -> pd.Series:
    """Return rows whose line total differs from quantity times unit price."""
    quantity = pd.to_numeric(dataframe["quantity"], errors="coerce")
    unit_price = pd.to_numeric(dataframe["unit_price"], errors="coerce")
    line_total = pd.to_numeric(dataframe["line_total"], errors="coerce")
    expected = quantity * unit_price
    half_cent_tolerance = 0.005 + 1e-9
    return (
        line_total.isna()
        | expected.isna()
        | line_total.sub(expected).abs().gt(half_cent_tolerance)
    )
