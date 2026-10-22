"""Local pooled t-test summaries for Vantage6 v5."""
import os
from typing import Any
import numpy as np
import pandas as pd
from vantage6.algorithm.decorator.action import federated
from vantage6.algorithm.decorator.data import dataframe


@federated
@dataframe(1)
def federated_function(
    df1: pd.DataFrame,
    columns: list[str] | None = None,
    group_col: str | None = None,
) -> dict[str, Any]:
    if df1 is None:
        return {"error": "No data available."}
    minimum = int(os.environ.get("T_TEST_MINIMUM_NUMBER_OF_RECORDS", "3"))
    if len(df1) <= minimum:
        return {"error": f"Station has {len(df1)} records; requires more than {minimum}."}
    if group_col and group_col not in df1.columns:
        return {"error": f"Grouping column {group_col!r} not found."}
    selected = list(columns) if columns is not None else list(df1.select_dtypes(include=np.number).columns)
    selected = [column for column in selected if column != group_col]
    if not selected or len(set(selected)) != len(selected) or any(column not in df1 for column in selected):
        return {"error": "No valid distinct numeric columns selected."}
    for column in selected:
        if not pd.api.types.is_numeric_dtype(df1[column]):
            return {"error": f"Column {column!r} must be numeric."}

    def stats(frame: pd.DataFrame) -> dict:
        output = {}
        for col in selected:
            vals = frame[col].replace([np.inf, -np.inf], np.nan).dropna().to_numpy(dtype=float)
            if len(vals):
                output[col] = {
                    "n": int(len(vals)),
                    "sum": float(vals.sum()),
                    "sum_sq": float(vals @ vals),
                }
        return output

    if group_col:
        grouped = {str(label): stats(group) for label, group in df1.groupby(group_col, sort=False)}
        return {"group_col": group_col, "stats": grouped}
    return {"group_col": None, "stats": stats(df1)}
