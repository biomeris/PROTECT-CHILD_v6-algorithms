"""
This file contains all partial algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the partial task
or directly to the user (if they requested partial results).
"""
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.tools.decorators import data


def _to_python_scalar(value: Any) -> Any:
    """Convert numpy scalars to plain Python scalars for JSON serialization."""
    if isinstance(value, np.generic):
        return value.item()
    return value


@data(1)
def partial(
    df1: pd.DataFrame,
    group_col: str,
    features: Optional[List[str]] = None,
) -> Any:
    """
    Partial part of a federated Tukey HSD algorithm.

    For each feature and each group, compute local sufficient statistics:
        - n
        - sum
        - sum_sq

    Parameters
    ----------
    df1 : pd.DataFrame
        Local dataframe.
    group_col : str
        Column containing the group labels.
    features : list[str], optional
        Numeric columns to analyse. If None, all numeric columns are used,
        excluding the grouping column if it is numeric.

    Returns
    -------
    dict
        JSON-serializable dictionary with local sufficient statistics.
    """
    info("Starting partial Tukey HSD statistics computation")

    if df1 is None or df1.empty:
        warn("Received empty dataframe.")
        return {"error": "Empty dataframe."}

    if group_col not in df1.columns:
        error(f"Grouping column '{group_col}' not found in dataframe.")
        return {"error": f"Grouping column '{group_col}' not found in dataframe."}

    # Select numeric features
    if features is not None:
        missing = [col for col in features if col not in df1.columns]
        if missing:
            error(f"Requested features not found in data: {missing}")
            return {"error": f"Requested features not found: {missing}"}
        X = df1[features].copy()
        columns = list(features)
    else:
        X = df1.select_dtypes(include=[np.number]).copy()
        if group_col in X.columns:
            X = X.drop(columns=[group_col])
        columns = list(X.columns)

    if not columns:
        error("No usable numeric features found for Tukey HSD.")
        return {"error": "No usable numeric features found for Tukey HSD."}

    # Keep grouping column + selected features, then drop rows with any missing values
    data = pd.concat([df1[[group_col]], X], axis=1)
    data = data.dropna(axis=0, how="any")

    if data.empty:
        error("All rows contain missing values in grouping/features.")
        return {"error": "All rows contain missing values in grouping/features."}

    # Preserve first-seen group order locally
    local_groups = [_to_python_scalar(g) for g in data[group_col].drop_duplicates().tolist()]

    stats_by_feature: Dict[str, List[Dict[str, Any]]] = {}

    for col in columns:
        entries: List[Dict[str, Any]] = []

        for group_value, group_df in data.groupby(group_col, sort=False, dropna=False):
            values = group_df[col].to_numpy(dtype=float)
            n = int(values.size)

            if n == 0:
                continue

            entries.append(
                {
                    "group": _to_python_scalar(group_value),
                    "n": n,
                    "sum": float(np.sum(values)),
                    "sum_sq": float(np.sum(values ** 2)),
                }
            )

        stats_by_feature[col] = entries

    result = {
        "group_col": group_col,
        "columns": columns,
        "groups": local_groups,
        "stats": stats_by_feature,
    }

    info("Partial Tukey HSD statistics computation finished")
    return result