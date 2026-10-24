"""Local sufficient statistics for exact federated PCA."""
from typing import Any
import numpy as np
import pandas as pd
from vantage6.algorithm.decorator.action import federated
from vantage6.algorithm.decorator.data import dataframe


@federated
@dataframe(1)
def federated_function(
    df1: pd.DataFrame,
    features: list[str] | None = None,
) -> dict[str, Any]:
    if df1 is None or df1.empty:
        return {"error": "Empty dataframe."}
    columns = list(features) if features is not None else df1.select_dtypes(include=np.number).columns.tolist()
    if not columns or len(set(columns)) != len(columns):
        return {"error": "No valid distinct numeric feature columns selected."}
    if any(column not in df1.columns for column in columns):
        return {"error": "Missing requested feature column."}
    x = df1[columns].apply(pd.to_numeric, errors="coerce")
    x = x.replace([np.inf, -np.inf], np.nan).dropna()
    values = x.to_numpy(dtype=float)
    return {
        "columns": columns,
        "n": int(values.shape[0]),
        "sum": values.sum(axis=0).tolist(),
        "sum_sq": (values.T @ values).tolist(),
    }
