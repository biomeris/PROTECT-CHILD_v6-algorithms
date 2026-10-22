"""Local sufficient statistics for an exact federated one-way ANOVA."""
from typing import Any
import numpy as np
import pandas as pd
from vantage6.algorithm.decorator.action import federated
from vantage6.algorithm.decorator.data import dataframe


@federated
@dataframe(1)
def federated_function(
    df1: pd.DataFrame,
    group_col: str,
    features: list[str] | None = None,
) -> dict[str, Any]:
    if df1 is None or df1.empty:
        return {"error": "Empty dataframe"}
    if group_col not in df1:
        return {"error": f"Unknown grouping column: {group_col}"}
    columns = list(features) if features is not None else list(df1.select_dtypes(include=np.number).columns)
    columns = [name for name in columns if name != group_col]
    if len(set(columns)) != len(columns) or not columns:
        return {"error": "Select one or more distinct numeric features."}
    if any(name not in df1 for name in columns):
        return {"error": "One or more requested feature columns are missing."}
    result = {}
    for column in columns:
        subset = df1[[group_col, column]].copy()
        subset[column] = pd.to_numeric(subset[column], errors="coerce")
        subset = subset.replace([np.inf, -np.inf], np.nan).dropna()
        entries = []
        for label, part in subset.groupby(group_col, sort=False):
            values = part[column].to_numpy(dtype=float)
            if not len(values):
                continue
            entries.append({
                "group": label.item() if isinstance(label, np.generic) else label,
                "n": int(len(values)),
                "sum": float(values.sum()),
                "sum_sq": float(values @ values),
            })
        result[column] = entries
    return {"group_col": group_col, "columns": columns, "stats": result}
