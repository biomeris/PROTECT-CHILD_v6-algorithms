from itertools import combinations
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.action import federated
from vantage6.algorithm.decorator.data import dataframe


def _select_columns(df: pd.DataFrame, columns: Optional[List[str]]) -> List[str]:
    if columns is not None:
        missing = [col for col in columns if col not in df.columns]
        if missing:
            raise ValueError(f"Requested columns not found in data: {missing}")
        return list(columns)
    return list(df.select_dtypes(include=[np.number]).columns)


def _assign_bins(values: np.ndarray, edges: np.ndarray) -> np.ndarray:
    idx = np.searchsorted(edges, values, side="right") - 1
    return np.clip(idx, 0, len(edges) - 2).astype(int)


def _pair_key(col1: str, col2: str) -> str:
    return f"{col1}|||{col2}"


@federated
@dataframe(1)
def federated_function(
    df1: pd.DataFrame,
    mode: str,
    columns: Optional[List[str]] = None,
    bin_edges: Optional[Dict[str, List[float]]] = None,
) -> Any:
    info(f"Starting federated Kendall computation in mode='{mode}'")

    if df1 is None or df1.empty:
        warn("Received empty dataframe.")
        return {"error": "Empty dataframe."}

    try:
        cols = _select_columns(df1, columns)
    except ValueError as exc:
        error(str(exc))
        return {"error": str(exc)}

    if not cols:
        return {"error": "No usable numeric columns found."}

    if mode == "summary":
        summary = {}
        for col in cols:
            series = pd.to_numeric(df1[col], errors="coerce").dropna()
            if series.empty:
                summary[col] = {"count": 0, "min": None, "max": None}
            else:
                summary[col] = {
                    "count": int(series.shape[0]),
                    "min": float(series.min()),
                    "max": float(series.max()),
                }
        return {"mode": "summary", "columns": cols, "summary": summary}

    if mode == "contingency":
        if bin_edges is None:
            return {"error": "Missing 'bin_edges' for contingency mode."}

        contingency_tables = {}
        for col1, col2 in combinations(cols, 2):
            if col1 not in bin_edges or col2 not in bin_edges:
                return {"error": f"Missing bin edges for pair '{col1}', '{col2}'."}

            pair_df = df1[[col1, col2]].apply(
                pd.to_numeric, errors="coerce"
            ).dropna()
            edges_x = np.asarray(bin_edges[col1], dtype=float)
            edges_y = np.asarray(bin_edges[col2], dtype=float)
            table = np.zeros(
                (len(edges_x) - 1, len(edges_y) - 1), dtype=int
            )

            if not pair_df.empty:
                bx = _assign_bins(
                    pair_df[col1].to_numpy(dtype=float), edges_x
                )
                by = _assign_bins(
                    pair_df[col2].to_numpy(dtype=float), edges_y
                )
                np.add.at(table, (bx, by), 1)

            contingency_tables[_pair_key(col1, col2)] = table.tolist()

        return {
            "mode": "contingency",
            "columns": cols,
            "contingency_tables": contingency_tables,
        }

    return {"error": f"Unsupported mode '{mode}'."}
