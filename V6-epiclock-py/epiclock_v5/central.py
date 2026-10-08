"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""
from typing import Any, List
import pandas as pd
import numpy as np

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.client import AlgorithmClient

from .federated import NODE_RESULT_COLUMNS

GLOBAL_RESULT_COLUMNS = [
    "cohort",
    "clock",
    "mean_age_global",
    "sd_age_global",
    "n_samples_total",
    "n_nodes",
]


def aggregate_cohort_results(results: list) -> pd.DataFrame:
    """
    Combine node-level statistics into global statistics per (cohort, clock).

    For the nodes i that returned a (cohort, clock):
        n_samples_total = sum(n_i)
        mean_age_global = sum(n_i * mean_i) / n_samples_total
        sd_age_global   = sqrt(sum((n_i - 1) * sd_i^2 + n_i * (mean_i - mean_age_global)^2)
                               / (n_samples_total - 1))
    which equals the mean and sample standard deviation of all the cohort's samples.

    Parameters:
    -----------
    results : list
        One entry per node: a list of records (or DataFrame) with columns cohort,
        clock, n_samples, mean_age and sd_age

    Returns:
    --------
    pd.DataFrame : Columns cohort, clock, mean_age_global, sd_age_global,
        n_samples_total, n_nodes
    """
    node_dfs = []
    for r in results:
        if isinstance(r, pd.DataFrame):
            node_dfs.append(r)
        elif isinstance(r, list):
            node_dfs.append(pd.DataFrame(r, columns=NODE_RESULT_COLUMNS))

    combined = (
        pd.concat(node_dfs, ignore_index=True)
        if node_dfs
        else pd.DataFrame(columns=NODE_RESULT_COLUMNS)
    )
    if combined.empty:
        return pd.DataFrame(columns=GLOBAL_RESULT_COLUMNS)

    rows = []
    for (cohort, clk), group in combined.groupby(["cohort", "clock"]):
        n = group["n_samples"].to_numpy(dtype=float)
        means = group["mean_age"].to_numpy(dtype=float)
        sds = group["sd_age"].to_numpy(dtype=float)

        total_n = n.sum()
        mean_global = float((n * means).sum() / total_n)
        sum_squares = ((n - 1) * sds ** 2 + n * (means - mean_global) ** 2).sum()
        sd_global = float(np.sqrt(sum_squares / (total_n - 1)))

        rows.append({
            "cohort": cohort,
            "clock": clk,
            "mean_age_global": mean_global,
            "sd_age_global": sd_global,
            "n_samples_total": int(total_n),
            "n_nodes": int(len(group)),
        })

    return pd.DataFrame(rows, columns=GLOBAL_RESULT_COLUMNS)


@central
@algorithm_client
def central_function(
    client: AlgorithmClient,
    lista_relojes: List[str],
    cohort_column: str = "cohort",
    min_samples: int = 3,
) -> Any:
    """
    Central aggregation of epigenetic age predictions per cohort.

    Implements PRIVACY-PRESERVING AGGREGATION: nodes return only the number of
    samples, mean and standard deviation per (cohort, clock), never individual
    patient data. A cohort may span several organizations; the central node pools
    each cohort across organizations, weighting by the number of samples.

    Parameters:
    -----------
    lista_relojes : List[str]
        List of clock names to calculate (e.g., ['horvath2013', 'hannum', 'pcphenoage'])
    cohort_column : str, optional
        Name of the column in each node's data that holds the cohort of each sample
        (default: "cohort")
    min_samples : int, optional
        Minimum number of samples a cohort needs on a node to be included in that
        node's result (default: 3)

    Returns:
    --------
    list[dict] : One record per (cohort, clock) with mean_age_global,
        sd_age_global, n_samples_total and n_nodes
    """
    info("Retrieving organizations in collaboration")
    organizations = client.organization.list()
    org_ids = [org.get("id") for org in organizations]
    info(f"Found {len(org_ids)} organizations")

    info(f"Creating subtask to calculate clocks: {lista_relojes}")
    task = client.task.create(
        organizations=org_ids,
        method="federated_function",
        name="Epigenetic clock calculation",
        description="Calculate epigenetic age per cohort using multiple clocks (aggregated statistics only)",
        arguments={
            "lista_relojes": lista_relojes,
            "cohort_column": cohort_column,
            "min_samples": min_samples,
        },
        databases=[{"label": "default"}],
    )

    info(f"Waiting for aggregated results from {len(org_ids)} organizations")
    results = client.wait_for_results(task_id=task.get("id"))
    info(f"Received aggregated results from {len(results)} organizations")

    global_summary = aggregate_cohort_results(results)
    if global_summary.empty:
        warn("No cohort met the min_samples threshold on any node; no results to return")
    else:
        info(
            f"Global summary: {global_summary['cohort'].nunique()} cohort(s), "
            f"{global_summary['clock'].nunique()} clock(s)"
        )
    return global_summary.to_dict(orient="records")
