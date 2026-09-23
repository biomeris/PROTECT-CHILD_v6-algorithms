"""
This file contains all partial algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the partial task
or directly to the user (if they requested partial results).
"""
from typing import Any, Dict, List, Optional

import pandas as pd
import numpy as np

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.tools.decorators import data

from .globals import LDA_MINIMUM_NUMBER_OF_RECORDS


@data(1)
def partial(
    df1: pd.DataFrame,
    class_col: str,
    features: Optional[List[str]] = None,
) -> Any:
    """
    Decentral part of a federated LDA algorithm.

    Computes per-class sufficient statistics that allow the central node to
    reconstruct the global within-class scatter matrix (S_W) and between-class
    scatter matrix (S_B) without accessing row-level data.

    For each class c the following are computed:
        - n_c  : number of observations in class c at this node
        - sum_c : sum of feature vectors (shape d)
        - sum_sq_c : sum of outer products X_c^T X_c (shape d x d)

    From these, the central node can derive:
        local mean     : mu_c = sum_c / n_c
        local scatter  : sum_sq_c - (1/n_c) * outer(sum_c, sum_c)
    and then apply the parallel-axis correction to combine scatter matrices
    across nodes into a single global S_W.

    Parameters
    ----------
    df1 : pd.DataFrame
        The data for the data station.
    class_col : str
        Column name containing the class labels.
    features : list[str] | None, optional
        Numeric feature columns. If None, all numeric columns are used
        (excluding class_col).

    Returns
    -------
    dict with:
        - columns : list of feature column names
        - classes : dict mapping each class label to its local statistics
                    { "n": int, "sum": list, "sum_sq": list[list] }
    """
    info("Starting partial LDA statistics computation")

    if df1 is None or df1.empty:
        warn("Received empty dataframe.")
        return {"error": "Empty dataframe."}

    if class_col not in df1.columns:
        error(f"Class column '{class_col}' not found in dataframe.")
        return {"error": f"Class column '{class_col}' not found in dataframe."}

    if len(df1) <= LDA_MINIMUM_NUMBER_OF_RECORDS:
        error(
            f"Number of records ({len(df1)}) must be greater than "
            f"{LDA_MINIMUM_NUMBER_OF_RECORDS}."
        )
        return {"error": "Insufficient number of records."}

    # Select features
    if features is not None:
        missing = [c for c in features if c not in df1.columns]
        if missing:
            error(f"Requested features not found in data: {missing}")
            return {"error": f"Requested features not found: {missing}"}
        X = df1[features]
        columns = list(features)
    else:
        # Default: numeric columns excluding class_col
        X = df1.select_dtypes(include=[np.number]).drop(
            columns=[class_col], errors="ignore"
        )
        columns = list(X.columns)

    if X.empty or len(columns) == 0:
        error("No usable numeric features found for LDA.")
        return {"error": "No usable numeric features found for LDA."}

    # Drop rows with NaN in selected columns or class_col
    valid_mask = df1[class_col].notna() & X.notna().all(axis=1)
    X = X[valid_mask]
    labels = df1.loc[valid_mask, class_col]

    if X.empty:
        error("All rows contain NaNs for the selected features or class column.")
        return {"error": "All rows contain NaNs for the selected features or class column."}

    X_np = X.to_numpy(dtype=float)

    # Compute per-class sufficient statistics
    classes_stats: Dict[str, Any] = {}
    for label in labels.unique():
        mask = (labels == label).values
        X_c = X_np[mask]
        n_c = int(X_c.shape[0])

        sum_c = np.sum(X_c, axis=0)         # shape (d,)
        sum_sq_c = X_c.T @ X_c              # shape (d, d)

        classes_stats[str(label)] = {
            "n": n_c,
            "sum": sum_c.tolist(),
            "sum_sq": sum_sq_c.tolist(),
        }

    result: Dict[str, Any] = {
        "columns": columns,
        "classes": classes_stats,
    }

    info("Partial LDA statistics computation finished")
    return result
