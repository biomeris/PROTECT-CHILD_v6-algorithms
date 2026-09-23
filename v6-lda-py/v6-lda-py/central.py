"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""
from typing import Any, Dict, List, Optional

import numpy as np

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.tools.decorators import algorithm_client
from vantage6.algorithm.client import AlgorithmClient


@algorithm_client
def central(
    client: AlgorithmClient,
    class_col: str,
    features: Optional[List[str]] = None,
    n_components: Optional[int] = None,
) -> Any:
    """
    Central part of the federated LDA algorithm.

    Aggregates per-class sufficient statistics (n, sum, sum_sq) from all
    participating nodes to reconstruct the global within-class scatter matrix
    (S_W) and between-class scatter matrix (S_B) without accessing row-level
    data. Solves the generalised eigenvalue problem S_W^{-1} S_B to obtain
    the discriminant axes.

    Aggregation uses the parallel-axis theorem: the global within-class
    scatter for class c is

        S_W_c = sum_k [ local_scatter_c_k
                        + n_c_k * outer(mu_c_k - mu_c_global,
                                        mu_c_k - mu_c_global) ]

    where local_scatter_c_k = sum_sq_c_k - (1/n_c_k) * outer(sum_c_k, sum_c_k).

    Parameters
    ----------
    client : AlgorithmClient
        Vantage6 algorithm client.
    class_col : str
        Column name containing the class labels.
    features : list[str] | None, optional
        Numeric feature columns. If None, all numeric columns are used.
    n_components : int | None, optional
        Number of discriminant components to return.
        Capped at min(n_classes - 1, n_features). If None, the maximum
        number of components is returned.

    Returns
    -------
    dict with:
        - columns              : list of feature names
        - classes              : sorted list of class labels
        - n_per_class          : dict mapping class label to total sample size
        - n_total              : total number of observations across all nodes
        - class_means          : dict mapping class label to global mean vector
        - scalings             : LDA discriminant axes, shape (n_features, n_components);
                                 each column is one discriminant direction
        - explained_variance_ratio : fraction of between-class variance explained
                                     by each component
        - n_components         : number of components returned
    """
    info("Starting central federated LDA")

    organizations = client.organization.list()
    org_ids = [org.get("id") for org in organizations]

    if not org_ids:
        error("No organizations found in the collaboration.")
        return {"error": "No organizations found in the collaboration."}

    info("Dispatching partial LDA tasks to all organizations")
    input_ = {
        "method": "partial",
        "kwargs": {"class_col": class_col, "features": features},
    }
    task = client.task.create(
        input_=input_,
        organizations=org_ids,
        name="Federated LDA partial stats",
        description="Compute per-class sufficient statistics for federated LDA",
    )

    info("Waiting for results")
    results = client.wait_for_results(task_id=task.get("id"))
    info("Results obtained!")

    if not results:
        error("No results received from partial tasks.")
        return {"error": "No results received from partial tasks."}

    # ------------------------------------------------------------------ #
    # Pass 1: resolve global column list and collect per-class accumulators
    # ------------------------------------------------------------------ #
    columns: Optional[List[str]] = None

    # Per-class accumulators: { class_label: {"n": int, "sum": np.array,
    #                                          "sum_sq": np.array,
    #                                          "local_stats": list of (n_k, sum_k)} }
    class_accum: Dict[str, Dict] = {}

    for idx, res in enumerate(results):
        if res is None:
            warn(f"Empty result from node {idx}. Skipping.")
            continue
        if "error" in res:
            warn(f"Node {idx} returned error: {res['error']}. Skipping.")
            continue

        cols = res.get("columns")
        classes_stats = res.get("classes")

        if cols is None or classes_stats is None:
            warn(f"Incomplete result from node {idx}. Skipping.")
            continue

        # Resolve / validate feature columns
        if columns is None:
            columns = cols
            d = len(columns)
        else:
            if cols != columns:
                error("Column mismatch between nodes.")
                return {"error": "Column mismatch between nodes."}

        d = len(columns)

        for label, stats in classes_stats.items():
            n_k = int(stats["n"])
            sum_k = np.asarray(stats["sum"], dtype=float)
            sum_sq_k = np.asarray(stats["sum_sq"], dtype=float)

            if sum_k.shape != (d,) or sum_sq_k.shape != (d, d):
                warn(f"Shape mismatch for class '{label}' at node {idx}. Skipping.")
                continue

            if label not in class_accum:
                class_accum[label] = {
                    "n_total": 0,
                    "sum_total": np.zeros(d, dtype=float),
                    # list of (n_k, sum_k, sum_sq_k) tuples for parallel-axis correction
                    "local_stats": [],
                }

            class_accum[label]["n_total"] += n_k
            class_accum[label]["sum_total"] += sum_k
            class_accum[label]["local_stats"].append((n_k, sum_k, sum_sq_k))

    if columns is None or not class_accum:
        error("No valid node results to aggregate.")
        return {"error": "No valid node results to aggregate."}

    d = len(columns)
    class_labels = sorted(class_accum.keys())
    n_classes = len(class_labels)

    if n_classes < 2:
        error(f"LDA requires at least 2 classes; found: {class_labels}")
        return {"error": f"LDA requires at least 2 classes; found {class_labels}"}

    # ------------------------------------------------------------------ #
    # Pass 2: compute global class means and global S_W
    # ------------------------------------------------------------------ #
    info("Computing global within-class scatter matrix S_W")

    n_per_class: Dict[str, int] = {}
    class_means: Dict[str, np.ndarray] = {}
    S_W = np.zeros((d, d), dtype=float)

    for label in class_labels:
        acc = class_accum[label]
        n_c = acc["n_total"]
        mu_c = acc["sum_total"] / n_c          # global mean for class c

        n_per_class[label] = n_c
        class_means[label] = mu_c

        # Aggregate within-class scatter using parallel-axis theorem
        for n_k, sum_k, sum_sq_k in acc["local_stats"]:
            mu_k = sum_k / n_k
            local_scatter = sum_sq_k - n_k * np.outer(mu_k, mu_k)
            correction = n_k * np.outer(mu_k - mu_c, mu_k - mu_c)
            S_W += local_scatter + correction

    # ------------------------------------------------------------------ #
    # Pass 3: compute global overall mean and between-class scatter S_B
    # ------------------------------------------------------------------ #
    info("Computing global between-class scatter matrix S_B")

    n_total = sum(n_per_class.values())
    mu_global = sum(
        n_per_class[c] * class_means[c] for c in class_labels
    ) / n_total

    S_B = np.zeros((d, d), dtype=float)
    for label in class_labels:
        diff = class_means[label] - mu_global
        S_B += n_per_class[label] * np.outer(diff, diff)

    # ------------------------------------------------------------------ #
    # Pass 4: solve generalised eigenvalue problem S_W^{-1} S_B
    # ------------------------------------------------------------------ #
    info("Solving LDA eigenvalue problem")

    try:
        S_W_inv = np.linalg.inv(S_W)
    except np.linalg.LinAlgError:
        warn("S_W is singular; using pseudoinverse.")
        S_W_inv = np.linalg.pinv(S_W)

    M = S_W_inv @ S_B
    eigvals, eigvecs = np.linalg.eig(M)

    # Keep only real parts (imaginary parts arise from numerical noise)
    eigvals = eigvals.real
    eigvecs = eigvecs.real

    # Sort descending by eigenvalue
    order = np.argsort(eigvals)[::-1]
    eigvals = eigvals[order]
    eigvecs = eigvecs[:, order]

    # Maximum meaningful components: min(n_classes - 1, n_features)
    max_components = min(n_classes - 1, d)
    eigvals = eigvals[:max_components]
    eigvecs = eigvecs[:, :max_components]

    if n_components is not None:
        k = max(1, min(int(n_components), max_components))
    else:
        k = max_components

    scalings = eigvecs[:, :k]               # shape (d, k)
    eigenvalues = eigvals[:k]

    total_var = float(np.sum(np.abs(eigvals))) if np.sum(np.abs(eigvals)) > 0 else 1.0
    explained_variance_ratio = (np.abs(eigenvalues) / total_var).tolist()

    result = {
        "columns": columns,
        "classes": class_labels,
        "n_per_class": {c: int(v) for c, v in n_per_class.items()},
        "n_total": int(n_total),
        "class_means": {c: class_means[c].tolist() for c in class_labels},
        "scalings": scalings.tolist(),
        "explained_variance_ratio": explained_variance_ratio,
        "n_components": k,
    }

    info("Central federated LDA finished")
    return result
