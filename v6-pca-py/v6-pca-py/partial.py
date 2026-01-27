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
from vantage6.algorithm.tools.decorators import algorithm_client
from vantage6.algorithm.tools.decorators import data
from vantage6.algorithm.client import AlgorithmClient


@data(1)
@algorithm_client
def partial(
    client: AlgorithmClient,
    df1: pd.DataFrame,
    features: Optional[List[str]] = None,
) -> Any:
    """
    Decentral part of a federated PCA algorithm.

    Computes local sufficient statistics:
        - n
        - sum of features
        - sum of outer products (X^T X)

    Parameters
    ----------
    features:
        List of column names to include in PCA. If None, numeric columns are used.

    Returns
    -------
    dict with:
        - n
        - columns
        - sum
        - sum_sq
    """

    info("Starting partial PCA statistics computation")

    if df1 is None or df1.empty:
        warn("Received empty dataframe.")
        return {"error": "Empty dataframe."}

    # Select features
    if features is not None:
        missing = [c for c in features if c not in df1.columns]
        if missing:
            error(f"Requested features not found in data: {missing}")
            return {"error": f"Requested features not found: {missing}"}
        X = df1[features]
        columns = list(features)
    else:
        # Default: use numeric columns only
        X = df1.select_dtypes(include=[np.number])
        columns = list(X.columns)

    if X.empty:
        error("No usable numeric features found for PCA.")
        return {"error": "No usable numeric features found for PCA."}

    # Drop rows with missing values in selected columns
    X = X.dropna(axis=0, how="any")
    if X.empty:
        error("All rows contain NaNs for the selected features.")
        return {"error": "All rows contain NaNs for the selected features."}

    # Convert to numpy
    X_np = X.to_numpy(dtype=float)

    n = int(X_np.shape[0])
    d = int(X_np.shape[1])

    # Local sums
    sum_vec = np.sum(X_np, axis=0)  # shape (d,)

    # Local sum of squares / cross-products
    # X^T X
    sum_sq = X_np.T @ X_np  # shape (d, d)

    # Privacy note: this returns aggregated statistics only.
    # Make sure this level of sharing is acceptable for your governance model.
    result: Dict[str, Any] = {
        "n": n,
        "columns": columns,
        "sum": sum_vec.tolist(),
        "sum_sq": sum_sq.tolist(),
    }

    info("Partial PCA statistics computation finished")
    return result

# Feel free to add more partial functions here.
