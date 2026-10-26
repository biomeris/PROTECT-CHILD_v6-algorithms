"""Exact covariance PCA from additively aggregated cross-products."""
from typing import Any
import numpy as np
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.client import AlgorithmClient


@central
@algorithm_client
def central_function(
    client: AlgorithmClient,
    features: list[str] | None = None,
    n_components: int | None = None,
    center: bool = True,
    organizations_to_include: list[int] | None = None,
) -> dict[str, Any]:
    available = [org["id"] for org in client.organization.list()]
    selected = available if organizations_to_include is None else list(organizations_to_include)
    if not selected or len(set(selected)) != len(selected) or not set(selected).issubset(available):
        return {"error": "Organizations must be a nonempty subset of the collaboration."}

    task = client.task.create(
        method="federated_function",
        arguments={"features": features},
        organizations=selected,
        name="Federated PCA cross-products",
        description="Compute count, feature sums and X transpose X at each station",
    )
    replies = client.wait_for_results(task_id=task["id"])
    if not isinstance(replies, list) or len(replies) != len(selected):
        return {"error": "Missing or unexpected number of node results."}

    columns, n_total, total_sum, xtx = None, 0, None, None
    for index, reply in enumerate(replies):
        if not isinstance(reply, dict) or reply.get("error"):
            return {"error": f"Organization result {index}: {reply}"}
        local_columns = reply.get("columns")
        if not isinstance(local_columns, list) or not local_columns:
            return {"error": "No numeric columns at a participating node."}
        if columns is None:
            columns = local_columns
            dim = len(columns)
            total_sum = np.zeros(dim, dtype=float)
            xtx = np.zeros((dim, dim), dtype=float)
        elif columns != local_columns:
            return {"error": "Feature names/order differ between organizations."}
        count = int(reply.get("n", -1))
        local_sum = np.asarray(reply.get("sum"), dtype=float)
        local_xtx = np.asarray(reply.get("sum_sq"), dtype=float)
        if count < 0 or local_sum.shape != total_sum.shape or local_xtx.shape != xtx.shape:
            return {"error": "Invalid sufficient statistic dimensions."}
        n_total += count
        total_sum += local_sum
        xtx += local_xtx

    if n_total < 2:
        return {"error": "PCA requires at least two complete observations."}
    avg = total_sum / n_total
    scatter = xtx - np.outer(total_sum, total_sum) / n_total if center else xtx
    cov = (scatter + scatter.T) / (2.0 * (n_total - 1))
    eigvals, eigvecs = np.linalg.eigh(cov)
    order = np.argsort(eigvals)[::-1]
    eigvals = np.maximum(eigvals[order], 0.0)
    eigvecs = eigvecs[:, order]
    dim = len(columns)
    if n_components is not None and not (1 <= int(n_components) <= dim):
        return {"error": f"n_components must be between 1 and {dim}."}
    count = dim if n_components is None else int(n_components)
    vals = eigvals[:count]
    explained_ratio = vals / eigvals.sum() if eigvals.sum() > 0 else np.zeros(count)
    return {
        "test": "federated_pca",
        "columns": columns,
        "n_total": n_total,
        "mean": avg.tolist(),
        "components": eigvecs[:, :count].tolist(),
        "explained_variance": vals.tolist(),
        "explained_variance_ratio": explained_ratio.tolist(),
        "centered": bool(center),
        "covariance": cov.tolist(),
    }
