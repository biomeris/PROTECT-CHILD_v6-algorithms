"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""
from itertools import combinations
from typing import Any, Dict, List, Optional

import numpy as np
from scipy.stats import studentized_range

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.tools.decorators import algorithm_client
from vantage6.algorithm.client import AlgorithmClient


def _group_key(group_value: Any) -> str:
    """Stable internal key for group aggregation."""
    return str(group_value)


@algorithm_client
def central(
    client: AlgorithmClient,
    group_col: str,
    features: Optional[List[str]] = None,
    alpha: float = 0.05,
    organizations_to_include: Optional[List[int]] = None,
) -> Any:
    """
    Central part of a federated Tukey HSD algorithm.

    Parameters
    ----------
    client : AlgorithmClient
        Vantage6 algorithm client.
    group_col : str
        Column containing the group labels.
    features : list[str], optional
        Numeric columns to analyse. If None, all numeric columns are used.
    alpha : float
        Significance level.
    organizations_to_include : list[int], optional
        Optional subset of organizations to include. If None, all organizations
        in the collaboration are used.

    Returns
    -------
    dict
        Global Tukey HSD results for each feature.
    """
    info("Starting central federated Tukey HSD")

    organizations = client.organization.list()
    available_org_ids = [organization.get("id") for organization in organizations]

    if not available_org_ids:
        error("No organizations found in the collaboration.")
        return {"error": "No organizations found in the collaboration."}

    if organizations_to_include is None:
        org_ids = available_org_ids
    else:
        org_ids = [org_id for org_id in organizations_to_include if org_id in available_org_ids]

    if not org_ids:
        error("No valid organizations selected for the analysis.")
        return {"error": "No valid organizations selected for the analysis."}

    input_ = {
        "method": "partial",
        "kwargs": {
            "group_col": group_col,
            "features": features,
        },
    }

    info("Creating subtask for all selected organizations")
    task = client.task.create(
        input_=input_,
        organizations=org_ids,
        name="Federated Tukey HSD partial stats",
        description="Compute local sufficient statistics for federated Tukey HSD",
    )

    info("Waiting for results")
    results = client.wait_for_results(task_id=task.get("id"))
    info("Results obtained")

    if not results:
        error("No results received from partial tasks.")
        return {"error": "No results received from partial tasks."}

    expected_columns: Optional[List[str]] = None
    aggregated: Dict[str, Dict[str, Dict[str, Any]]] = {}
    group_labels: Dict[str, Any] = {}

    for idx, res in enumerate(results):
        if res is None:
            warn(f"Received empty result from node at index {idx}. Skipping.")
            continue

        if "error" in res:
            warn(f"Node at index {idx} returned error: {res['error']}")
            continue

        local_group_col = res.get("group_col")
        local_columns = res.get("columns")
        local_stats = res.get("stats")

        if local_group_col != group_col:
            error("Grouping column mismatch between nodes.")
            return {"error": "Grouping column mismatch between nodes."}

        if local_columns is None or local_stats is None:
            warn(f"Incomplete result from node at index {idx}. Skipping.")
            continue

        if expected_columns is None:
            expected_columns = list(local_columns)
            aggregated = {col: {} for col in expected_columns}
        elif list(local_columns) != expected_columns:
            error("Column mismatch between nodes. Ensure same features/order everywhere.")
            return {"error": "Column mismatch between nodes."}

        for col in expected_columns:
            entries = local_stats.get(col, [])

            for entry in entries:
                group_value = entry["group"]
                key = _group_key(group_value)
                group_labels[key] = group_value

                slot = aggregated[col].setdefault(
                    key,
                    {
                        "group": group_value,
                        "n": 0,
                        "sum": 0.0,
                        "sum_sq": 0.0,
                    },
                )

                slot["n"] += int(entry["n"])
                slot["sum"] += float(entry["sum"])
                slot["sum_sq"] += float(entry["sum_sq"])

    if expected_columns is None:
        error("No valid node results to aggregate.")
        return {"error": "No valid node results to aggregate."}

    results_by_feature: Dict[str, Any] = {}

    for col in expected_columns:
        group_stats = []
        for key, stats_ in aggregated[col].items():
            n = int(stats_["n"])
            if n <= 0:
                continue

            mean = stats_["sum"] / n
            # Within-group SSE reconstructed from sufficient statistics
            sse = stats_["sum_sq"] - (stats_["sum"] ** 2) / n
            sse = max(float(sse), 0.0)

            group_stats.append(
                {
                    "group": stats_["group"],
                    "n": n,
                    "sum": float(stats_["sum"]),
                    "sum_sq": float(stats_["sum_sq"]),
                    "mean": float(mean),
                    "sse": sse,
                }
            )

        group_stats = sorted(group_stats, key=lambda x: str(x["group"]))

        k = len(group_stats)
        n_total = int(sum(item["n"] for item in group_stats))

        if k < 2:
            results_by_feature[col] = {
                "error": "At least two groups with observations are required."
            }
            continue

        df_error = n_total - k
        if df_error <= 0:
            results_by_feature[col] = {
                "error": "Not enough degrees of freedom to compute Tukey HSD."
            }
            continue

        sse_total = float(sum(item["sse"] for item in group_stats))
        mse = sse_total / df_error

        q_critical = float(studentized_range.ppf(1 - alpha, k, df_error))

        comparisons = []
        for g1, g2 in combinations(group_stats, 2):
            n1, n2 = g1["n"], g2["n"]
            mean1, mean2 = g1["mean"], g2["mean"]

            std_error = float(np.sqrt((mse / 2.0) * ((1.0 / n1) + (1.0 / n2))))
            mean_diff = float(mean2 - mean1)

            if std_error == 0.0:
                q_stat = float("inf") if mean_diff != 0 else 0.0
                p_value = 0.0 if mean_diff != 0 else 1.0
                half_width = 0.0
            else:
                q_stat = float(abs(mean_diff) / std_error)
                p_value = float(studentized_range.sf(q_stat, k, df_error))
                half_width = float(q_critical * std_error)

            ci_low = float(mean_diff - half_width)
            ci_high = float(mean_diff + half_width)

            comparisons.append(
                {
                    "group1": g1["group"],
                    "group2": g2["group"],
                    "mean1": mean1,
                    "mean2": mean2,
                    "mean_diff": mean_diff,   # group2 - group1
                    "std_error": std_error,
                    "q_stat": q_stat,
                    "p_value": p_value,
                    "ci_low": ci_low,
                    "ci_high": ci_high,
                    "reject": bool(p_value < alpha),
                }
            )

        results_by_feature[col] = {
            "n_total": n_total,
            "n_groups": k,
            "df_error": int(df_error),
            "mse": float(mse),
            "q_critical": q_critical,
            "group_statistics": group_stats,
            "comparisons": comparisons,
        }

    result = {
        "test": "federated_tukey_hsd",
        "group_col": group_col,
        "alpha": float(alpha),
        "columns": expected_columns,
        "results": results_by_feature,
    }

    info("Central federated Tukey HSD finished")
    return result