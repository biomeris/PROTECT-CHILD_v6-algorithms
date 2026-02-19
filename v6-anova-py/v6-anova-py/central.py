"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""
from typing import Any, Dict, List, Optional

import pandas as pd
import numpy as np
from scipy import stats

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.tools.decorators import algorithm_client
from vantage6.algorithm.tools.decorators import data
from vantage6.algorithm.client import AlgorithmClient


@algorithm_client
def central(
    client: AlgorithmClient,
    groups: List[str],
    features: Optional[List[str]] = None,
) -> Any:
    """
    Central part of the federated ANOVA algorithm.

    Parameters
    ----------
    groups:
        List of column names that define the groups for ANOVA.
    features:
        List of column names to include in ANOVA. If None, numeric columns are used.

    Returns
    -------
    dict with:
        - f_statistic
        - p_value
        - group_means
        - group_variances
    """

    info("Starting central federated ANOVA")

    # get all organizations (ids) within the collaboration so you can send a
    # task to them.
    organizations = client.organization.list()
    org_ids = [organization.get("id") for organization in organizations]

    if not org_ids:
        error("No organizations found in the collaboration.")
        return {"error": "No organizations found in the collaboration."}

    # Define input parameters for a subtask
    info("Defining input parameters for partial ANOVA tasks")
    input_ = {
        "method": "partial",
        "kwargs": {
            "groups": groups,
            "features": features,
        },
    }

    # create a subtask for all organizations in the collaboration.
    info("Creating subtask for all organizations in the collaboration")
    task = client.task.create(
        input_=input_,
        organizations=org_ids,
        name="Federated ANOVA partial stats",
        description="Compute local statistics for federated ANOVA"
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

    group_means = None
    group_variances = None
    n_total = 0
    total_ss_between = 0
    total_ss_within = 0

    for idx, res in enumerate(results):
        if res is None:
            warn(f"Received empty result from a node at index {idx}. Skipping.")
            continue

        if "error" in res:
            warn(f"Node at index {idx} returned error: {res['error']}")
            continue

        groups = res.get("groups")
        n = res.get("n")
        means = np.array(res.get("means"))
        variances = np.array(res.get("variances"))
        ss_between = res.get("ss_between")
        ss_within = res.get("ss_within")

        if groups is None or n is None or means is None or variances is None:
            warn(f"Incomplete result from node at index {idx}. Skipping.")
            continue

        if group_means is None:
            group_means = np.zeros_like(means)
            group_variances = np.zeros_like(variances)

        n_total += n
        group_means += means * n
        group_variances += variances * (n - 1)
        total_ss_between += ss_between
        total_ss_within += ss_within

    if group_means is None or n_total == 0:
        error("No valid node results to aggregate.")
        return {"error": "No valid node results to aggregate."}

    group_means /= n_total
    ss_total = total_ss_within + total_ss_between
    ms_between = total_ss_between / len(groups)
    ms_within = total_ss_within / (n_total - len(groups))

    # F-statistic and p-value
    f_statistic = ms_between / ms_within
    p_value = stats.f.sf(f_statistic, len(groups) - 1, n_total - len(groups))

    result = {
        "f_statistic": f_statistic,
        "p_value": p_value,
        "group_means": group_means.tolist(),
        "group_variances": group_variances.tolist(),
    }

    info("Central federated ANOVA finished")
    return result

# Feel free to add more central functions here.
