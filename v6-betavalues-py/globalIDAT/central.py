"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""
"""
Central functions that orchestrate the federated Beta/M computation.
"""
from typing import Any

import pandas as pd
from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.client import AlgorithmClient

from .federated import NODE_RESULT_COLUMNS

GLOBAL_RESULT_COLUMNS = [
    "cohort",
    "probe_id",
    "beta_mean_global",
    "m_mean_global",
    "n_samples_total",
]


def aggregate_cohort_results(results: list) -> pd.DataFrame:
    """Combine node-level summaries into a sample-weighted global mean per cohort.

    Each node result holds rows with cohort, probe_id, beta_mean, m_mean and
    n_samples. Within each (cohort, probe_id) the global mean is
    sum(mean_i * n_i) / sum(n_i) over the nodes that returned that cohort.
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

    combined["beta_weighted"] = combined["beta_mean"] * combined["n_samples"]
    combined["m_weighted"] = combined["m_mean"] * combined["n_samples"]

    global_summary = (
        combined.groupby(["cohort", "probe_id"])
        .agg(
            beta_sum=("beta_weighted", "sum"),
            m_sum=("m_weighted", "sum"),
            n_samples_total=("n_samples", "sum"),
        )
        .reset_index()
    )

    global_summary["beta_mean_global"] = global_summary["beta_sum"] / global_summary["n_samples_total"]
    global_summary["m_mean_global"] = global_summary["m_sum"] / global_summary["n_samples_total"]
    return global_summary[GLOBAL_RESULT_COLUMNS]


@central
@algorithm_client
def central_function(
    client: AlgorithmClient,
    idat_dir: str | None = None,
    cohort_column: str = "cohort",
    min_samples: int = 2,
) -> Any:
    """Dispatch federated tasks and aggregate node-level means per cohort.

    Parameters
    ----------
    client : AlgorithmClient
        Client provided by vantage6.
    idat_dir : str | None
        Passed on to the federated function as ``arg1`` (currently unused).
    cohort_column : str
        Name of the column in each node's data that holds the cohort of each sample.
    min_samples : int
        Minimum number of samples a cohort needs on a node to be included in that
        node's result.

    Returns
    -------
    list[dict]
        One record per (cohort, probe_id) with beta_mean_global, m_mean_global and
        n_samples_total.
    """
    organizations = client.organization.list()
    org_ids = [organization.get("id") for organization in organizations]

    info(f"Creating federated subtask for {len(org_ids)} organisations")
    task = client.task.create(
        method="federated_function",
        arguments={
            "arg1": idat_dir,
            "cohort_column": cohort_column,
            "min_samples": min_samples,
        },
        organizations=org_ids,
        name="IDAT Beta/M computation",
        description="Compute per-cohort Beta and M means using pylluminator preprocessing",
    )

    info(f"Waiting for results from federated task {task.get('id')}")
    results = client.wait_for_results(task_id=task.get("id"))
    info(f"Received results from {len(results)} organisations")

    global_summary = aggregate_cohort_results(results)
    if global_summary.empty:
        warn("No cohort met the min_samples threshold on any node; no results to return")
    else:
        info(
            f"Global summary: {global_summary['cohort'].nunique()} cohort(s), "
            f"{global_summary['probe_id'].nunique()} probes"
        )
    return global_summary.to_dict(orient="records")
