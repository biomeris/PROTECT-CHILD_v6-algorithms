"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""
from typing import Any, Dict, List, Optional

import pandas as pd
import numpy as np

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.tools.decorators import algorithm_client
from vantage6.algorithm.tools.decorators import data
from vantage6.algorithm.client import AlgorithmClient


@algorithm_client
def central(
    client: AlgorithmClient,
    features: Optional[List[str]] = None,
    n_components: Optional[int] = None,
    center: bool = True,
) -> Any:
    """
    Central part of a federated PCA algorithm.

    Parameters
    ----------
    features:
        List of column names to include in PCA. If None, numeric columns are used.
    n_components:
        Number of principal components to return. If None, all components are returned.
    center:
        If True, compute covariance using global centering.

    Returns
    -------
    dict with:
        - columns
        - n_total
        - mean
        - components
        - explained_variance
        - explained_variance_ratio
    """

    info("Starting central federated PCA")

    # get all organizations (ids) within the collaboration so you can send a
    # task to them.
    organizations = client.organization.list()
    org_ids = [organization.get("id") for organization in organizations]

    if not org_ids:
        error("No organizations found in the collaboration.")
        return {"error": "No organizations found in the collaboration."}

    # Define input parameters for a subtask
    info("Defining input parameters for partial PCA tasks")
    input_ = {
        "method": "partial",
        "kwargs": {
            "features": features,
        },
    }

    # create a subtask for all organizations in the collaboration.
    info("Creating subtask for all organizations in the collaboration")
    task = client.task.create(
        input_=input_,
        organizations=org_ids,
        name="Federated PCA partial stats",
        description="Compute local sufficient statistics for federated PCA"
    )

    # wait for node to return results of the subtask.
    info("Waiting for results")
    results = client.wait_for_results(task_id=task.get("id"))
    info("Results obtained!")

    if not results:
        error("No results received from partial tasks.")
        return {"error": "No results received from partial tasks."}

    # Validate and aggregate statistics
    info("Aggregating local statistics")

    # Each result is expected to be a dict:
    # {
    #   "n": int,
    #   "columns": [...],
    #   "sum": [...],
    #   "sum_sq": [[...], ...]
    # }
    columns = None
    n_total = 0
    sum_total = None
    sum_sq_total = None

    for idx, res in enumerate(results):
        if res is None:
            warn(f"Received empty result from a node at index {idx}. Skipping.")
            continue

        if "error" in res:
            warn(f"Node at index {idx} returned error: {res['error']}")
            continue

        cols = res.get("columns")
        n = res.get("n")
        s = res.get("sum")
        ss = res.get("sum_sq")

        if cols is None or n is None or s is None or ss is None:
            warn(f"Incomplete result from node at index {idx}. Skipping.")
            continue

        if columns is None:
            columns = cols
            sum_total = np.zeros(len(columns), dtype=float)
            sum_sq_total = np.zeros((len(columns), len(columns)), dtype=float)
        else:
            if cols != columns:
                error(
                    "Column mismatch between nodes. "
                    "Ensure all nodes use the same feature set and ordering."
                )
                return {"error": "Column mismatch between nodes."}

        n = int(n)
        s = np.asarray(s, dtype=float)
        ss = np.asarray(ss, dtype=float)

        if s.shape[0] != len(columns) or ss.shape != (len(columns), len(columns)):
            warn(f"Shape mismatch in node result at index {idx}. Skipping.")
            continue

        n_total += n
        sum_total += s
        sum_sq_total += ss

    if columns is None or n_total == 0:
        error("No valid node results to aggregate.")
        return {"error": "No valid node results to aggregate."}

    # Compute global mean
    mean = (sum_total / n_total).astype(float)

    # Compute global covariance
    if len(columns) == 1:
        # Degenerate case: single feature
        cov = np.array([[0.0]])
    else:
        if center:
            # Scatter matrix: X^T X - (1/n) * outer(sum, sum)
            scatter = sum_sq_total - (1.0 / n_total) * np.outer(sum_total, sum_total)
        else:
            # Without centering, treat sum_sq_total as scatter
            scatter = sum_sq_total

        denom = max(n_total - 1, 1)
        cov = scatter / denom

    # Eigen decomposition (symmetric)
    eigvals, eigvecs = np.linalg.eigh(cov)

    # Sort descending
    order = np.argsort(eigvals)[::-1]
    eigvals = eigvals[order]
    eigvecs = eigvecs[:, order]

    # Decide number of components
    d = len(columns)
    if n_components is None:
        k = d
    else:
        k = int(n_components)
        k = max(1, min(k, d))

    components = eigvecs[:, :k]  # columns are principal directions
    explained_variance = eigvals[:k]

    total_var = float(np.sum(eigvals)) if np.sum(eigvals) > 0 else 0.0
    if total_var > 0:
        explained_variance_ratio = explained_variance / total_var
    else:
        explained_variance_ratio = np.zeros_like(explained_variance)

    # Return a JSON-serializable object
    result = {
        "columns": columns,
        "n_total": int(n_total),
        "mean": mean.tolist(),
        "components": components.tolist(),  # shape (d, k)
        "explained_variance": explained_variance.tolist(),
        "explained_variance_ratio": explained_variance_ratio.tolist(),
        "centered": bool(center),
        "covariance": cov.tolist(),  # optional but often useful
    }

    info("Central federated PCA finished")
    return result

# Feel free to add more central functions here.
