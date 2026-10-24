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
    rank_maps: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Any:
    info(f"Starting federated Spearman computation in mode='{mode}'")

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

    if mode == "histogram":
        if bin_edges is None:
            return {"error": "Missing 'bin_edges' for histogram mode."}

        histograms = {}
        for col in cols:
            if col not in bin_edges:
                return {"error": f"Missing bin edges for column '{col}'."}

            edges = np.asarray(bin_edges[col], dtype=float)
            series = pd.to_numeric(df1[col], errors="coerce").dropna()

            if series.empty:
                counts = np.zeros(len(edges) - 1, dtype=int)
            else:
                counts = np.histogram(
                    series.to_numpy(dtype=float), bins=edges
                )[0]

            histograms[col] = counts.astype(int).tolist()

        return {
            "mode": "histogram",
            "columns": cols,
            "histograms": histograms,
        }

    if mode == "pair_stats":
        if rank_maps is None:
            return {"error": "Missing 'rank_maps' for pair_stats mode."}

        pair_stats = {}
        for col1, col2 in combinations(cols, 2):
            if col1 not in rank_maps or col2 not in rank_maps:
                return {"error": f"Missing rank map for pair '{col1}', '{col2}'."}

            pair_df = df1[[col1, col2]].apply(
                pd.to_numeric, errors="coerce"
            ).dropna()
            n = int(pair_df.shape[0])

            if n == 0:
                pair_stats[_pair_key(col1, col2)] = {
                    "n": 0,
                    "sum_x": 0.0,
                    "sum_y": 0.0,
                    "sum_x2": 0.0,
                    "sum_y2": 0.0,
                    "sum_xy": 0.0,
                }
                continue

            x = pair_df[col1].to_numpy(dtype=float)
            y = pair_df[col2].to_numpy(dtype=float)

            edges_x = np.asarray(rank_maps[col1]["edges"], dtype=float)
            edges_y = np.asarray(rank_maps[col2]["edges"], dtype=float)
            midranks_x = np.asarray(
                rank_maps[col1]["midranks"], dtype=float
            )
            midranks_y = np.asarray(
                rank_maps[col2]["midranks"], dtype=float
            )

            rx = midranks_x[_assign_bins(x, edges_x)]
            ry = midranks_y[_assign_bins(y, edges_y)]

            pair_stats[_pair_key(col1, col2)] = {
                "n": n,
                "sum_x": float(np.sum(rx)),
                "sum_y": float(np.sum(ry)),
                "sum_x2": float(np.sum(rx ** 2)),
                "sum_y2": float(np.sum(ry ** 2)),
                "sum_xy": float(np.sum(rx * ry)),
            }

        return {
            "mode": "pair_stats",
            "columns": cols,
            "pair_stats": pair_stats,
        }

    return {"error": f"Unsupported mode '{mode}'."}
