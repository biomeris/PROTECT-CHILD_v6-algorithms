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


@central
@algorithm_client
def central_function(client: AlgorithmClient, idat_dir: str | None = None) -> Any:
    """Dispatch federated tasks and aggregate node-level means into a weighted global mean."""
    organizations = client.organization.list()
    org_ids = [organization.get("id") for organization in organizations]

    info(f"Creating federated subtask for {len(org_ids)} organisations")
    task = client.task.create(
        method="federated_function",
        arguments={"arg1": idat_dir},
        organizations=org_ids,
        name="IDAT Beta/M computation",
        description="Compute Beta and M values using pylluminator preprocessing",
    )

    info(f"Waiting for results from federated task {task.get('id')}")
    results = client.wait_for_results(task_id=task.get("id"))
    info(f"Received results from {len(results)} organisations")

    # Each result is a DataFrame with probe_id, beta_mean, m_mean, n_samples
    node_dfs = []
    for r in results:
        if isinstance(r, pd.DataFrame):
            node_dfs.append(r)
        elif isinstance(r, list):
            node_dfs.append(pd.DataFrame(r))

    if not node_dfs:
        error("No valid results received from nodes")
        return pd.DataFrame(columns=["probe_id", "beta_mean_global", "m_mean_global", "n_samples_total"])

    combined = pd.concat(node_dfs, ignore_index=True)

    # Weighted mean: sum(mean * n) / sum(n) per probe
    combined["beta_weighted"] = combined["beta_mean"] * combined["n_samples"]
    combined["m_weighted"] = combined["m_mean"] * combined["n_samples"]

    global_summary = (
        combined.groupby("probe_id")
        .agg(
            beta_sum=("beta_weighted", "sum"),
            m_sum=("m_weighted", "sum"),
            n_samples_total=("n_samples", "sum"),
        )
        .reset_index()
    )

    global_summary["beta_mean_global"] = global_summary["beta_sum"] / global_summary["n_samples_total"]
    global_summary["m_mean_global"] = global_summary["m_sum"] / global_summary["n_samples_total"]
    global_summary = global_summary[["probe_id", "beta_mean_global", "m_mean_global", "n_samples_total"]]

    info(f"Global summary: {len(global_summary)} probes across {global_summary['n_samples_total'].max()} total samples")
    return global_summary