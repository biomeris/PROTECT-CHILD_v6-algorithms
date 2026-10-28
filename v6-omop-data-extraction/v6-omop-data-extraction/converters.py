import math

import pandas as pd


def to_category(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Convert columns to pandas categorical dtype."""
    for column in columns:
        df[column] = df[column].astype("category")
    return df


def _parse_float(value) -> float:
    """Parse any value to float; return NaN for missing/unparseable."""
    try:
        return float(str(value))
    except (ValueError, TypeError):
        return float("nan")


def to_int64(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Convert columns to nullable Int64 dtype."""
    for column in columns:
        df[column] = pd.array(
            [pd.NA if math.isnan(f := _parse_float(x)) else int(f) for x in df[column]],
            dtype="Int64",
        )
    return df


def to_float64(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Convert columns to nullable Float64 dtype."""
    for column in columns:
        df[column] = pd.array(
            [pd.NA if math.isnan(f := _parse_float(x)) else f for x in df[column]],
            dtype="Float64",
        )
    return df


def to_boolean(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Convert columns to nullable boolean dtype."""
    for column in columns:
        df[column] = df[column].astype("boolean")
    return df
