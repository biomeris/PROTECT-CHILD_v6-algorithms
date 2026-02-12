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
def partial(
    df1: pd.DataFrame,
    groups: List[str],
    features: Optional[List[str]] = None,
) -> Any:
    """
    Decentral part of a federated ANOVA algorithm.

    Computes local statistics for ANOVA:
        - means
        - variances
        - sum of squares between and within groups

    Parameters
    ----------
    df1 : pd.DataFrame
        The data for the data station
    groups:
        List of column names that define the groups for ANOVA.
    features:
        List of column names to include in ANOVA. If None, numeric columns are used.

    Returns
    -------
    dict with:
        - n
        - groups
        - means
        - variances
        - ss_between
        - ss_within
    """

    info("Starting partial ANOVA statistics computation")

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
        error("No usable numeric features found for ANOVA.")
        return {"error": "No usable numeric features found for ANOVA."}

    # Drop rows with missing values in selected columns
    X = X.dropna(axis=0, how="any")
    if X.empty:
        error("All rows contain NaNs for the selected features.")
        return {"error": "All rows contain NaNs for the selected features."}

    # Convert to numpy
    X_np = X.to_numpy(dtype=float)
    n = X_np.shape[0]
    d = X_np.shape[1]

    # Group the data based on the `groups` column(s)
    group_means = []
    group_variances = []
    ss_between = 0
    ss_within = 0

    group_labels = df1[groups[0]].unique()

    for label in group_labels:
        group_data = X_np[df1[groups[0]] == label]
        n_group = group_data.shape[0]
        mean_group = np.mean(group_data, axis=0)
        var_group = np.var(group_data, axis=0, ddof=1)

        group_means.append(mean_group)
        group_variances.append(var_group)

        # Sum of squares between groups
        ss_between += n_group * np.sum((mean_group - np.mean(X_np, axis=0))**2)

        # Sum of squares within groups
        ss_within += np.sum((group_data - mean_group)**2)

    group_means = np.array(group_means)
    group_variances = np.array(group_variances)

    result: Dict[str, Any] = {
        "n": n,
        "groups": group_labels.tolist(),
        "means": group_means.tolist(),
        "variances": group_variances.tolist(),
        "ss_between": ss_between,
        "ss_within": ss_within,
    }

    info("Partial ANOVA statistics computation finished")
    return result

# Feel free to add more partial functions here.
