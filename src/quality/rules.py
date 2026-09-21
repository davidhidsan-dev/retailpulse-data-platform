"""Small, reusable row-level data-quality rules."""

from __future__ import annotations

import re
from collections.abc import Iterable

import numpy as np
import pandas as pd


EMAIL_PATTERN = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def null_values(dataframe: pd.DataFrame, column: str) -> pd.Series:
    """Return a mask for null values in a required column."""
    return dataframe[column].isna()


def blank_values(dataframe: pd.DataFrame, column: str) -> pd.Series:
    """Return a mask for non-null strings containing no visible characters."""
    values = dataframe[column].astype("string")
    return values.notna() & values.str.strip().eq("")


def duplicate_values(dataframe: pd.DataFrame, column: str) -> pd.Series:
    """Return a mask for every non-null occurrence of a duplicate value."""
    values = dataframe[column]
    return values.notna() & values.duplicated(keep=False)


def invalid_emails(dataframe: pd.DataFrame, column: str = "email") -> pd.Series:
    """Return a mask for non-null strings that fail a basic email pattern."""
    values = dataframe[column].astype("string")
    return values.notna() & ~values.str.fullmatch(EMAIL_PATTERN, na=False)


def invalid_datetimes(dataframe: pd.DataFrame, column: str) -> pd.Series:
    """Return a mask for non-null values that cannot represent a timestamp."""
    values = dataframe[column]
    parsed = pd.to_datetime(values, errors="coerce", utc=True)
    return values.notna() & parsed.isna()


def fractional_values(dataframe: pd.DataFrame, column: str) -> pd.Series:
    """Return a mask for finite numeric values that are not whole numbers."""
    values = pd.to_numeric(dataframe[column], errors="coerce")
    return values.notna() & np.isfinite(values) & values.mod(1).ne(0)


def values_not_in(
    dataframe: pd.DataFrame,
    column: str,
    allowed_values: Iterable[str],
) -> pd.Series:
    """Return a mask for null values or values outside an allowed domain."""
    values = dataframe[column]
    return values.isna() | ~values.isin(set(allowed_values))


def non_positive_values(dataframe: pd.DataFrame, column: str) -> pd.Series:
    """Return a mask for null, non-finite or numeric values not above zero."""
    values = pd.to_numeric(dataframe[column], errors="coerce")
    return values.isna() | ~np.isfinite(values) | values.le(0)


def negative_values(dataframe: pd.DataFrame, column: str) -> pd.Series:
    """Return a mask for null, non-finite or negative numeric values."""
    values = pd.to_numeric(dataframe[column], errors="coerce")
    return values.isna() | ~np.isfinite(values) | values.lt(0)


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
        quantity.isna()
        | unit_price.isna()
        | line_total.isna()
        | ~np.isfinite(quantity)
        | ~np.isfinite(unit_price)
        | ~np.isfinite(line_total)
        | line_total.sub(expected).abs().gt(half_cent_tolerance)
    )
