from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np
from scipy.stats import t

from vantage6.algorithm.client import AlgorithmClient
from vantage6.algorithm.tools.decorators import algorithm_client
from vantage6.algorithm.tools.exceptions import UserInputError
from vantage6.algorithm.tools.util import info


def _combine_stats(stats_list: List[Dict[str, float]]) -> Optional[Dict[str, float]]:
    """
    Combine per-site summary stats (mean, sample variance, count) into a single
    global summary for a group.

    Each element of `stats_list` is expected to look like:
        {
            "average": <float>,
            "count": <float>,
            "variance": <float>   # sample variance (denominator n-1)
        }

    Returns
    -------
    dict | None
        {
            "average": global_mean,
            "count": global_count,
            "variance": global_sample_variance
        }
        or None if total count <= 1.
    """
    total_n = sum(site_stats["count"] for site_stats in stats_list)
    if total_n <= 1:
        return None

    global_mean = (
        sum(site_stats["average"] * site_stats["count"] for site_stats in stats_list)
        / total_n
    )

    # Total corrected sum of squares:
    #   sum_i [ (n_i - 1)*s_i^2 + n_i * (mu_i - mu_global)^2 ]
    total_ss = sum(
        (site_stats["count"] - 1) * site_stats["variance"]
        + site_stats["count"] * (site_stats["average"] - global_mean) ** 2
        for site_stats in stats_list
    )

    global_var = total_ss / (total_n - 1)

    return {
        "average": float(global_mean),
        "count": float(total_n),
        "variance": float(global_var),
    }


@algorithm_client
def central(
    client: AlgorithmClient,
    organizations_to_include: List[int],
    group_col: str,
    columns: Optional[List[str]] = None,
) -> Dict[str, Dict[str, float]]:
    """
    Perform a two-sample independent t-test between two groups defined by `group_col`,
    aggregating *all* samples from *all* participating organizations.

    Each organization runs `partial()` locally with `group_col`, producing per-group
    summary stats (mean, sample variance, count) for each numeric column.
    This function:
      1. collects those summaries,
      2. aggregates them across organizations for each group,
      3. runs a pooled-variance two-sample t-test.

    Assumptions:
      - Exactly two groups exist globally in `group_col`.
      - The t-test uses equal-variance assumption (pooled variance).
      - Raw rows never leave the node; only aggregates are shared.

    Parameters
    ----------
    client : AlgorithmClient
        Vantage6 algorithm client.
    organizations_to_include : list[int]
        IDs of the organizations whose data are included.
        Can be >= 1. They are all pooled.
    columns : list[str] | None
        Numeric columns to test. If None, nodes will use all numeric columns.
    group_col : str
        Name of the categorical column identifying the two groups.

    Returns
    -------
    dict
        {
            "<column_name>": {
                "t_score": <float>,
                "p_value": <float>,
            },
            ...
        }
        Only columns for which both groups have at least 2 total samples are returned.

    Raises
    ------
    UserInputError
        If `organizations_to_include` is empty,
        or if globally there are not exactly two groups.
    """

    # Basic input validation
    if not organizations_to_include:
        raise UserInputError("Provide at least one organization id in 'organizations_to_include'.")

    if not group_col:
        raise UserInputError(
            "Parameter 'group_col' is required for the two-sample t-test."
        )

    # Prepare subtask input for all organizations
    info("Defining subtask input for participating organizations.")
    subtask_input = {
        "method": "partial",
        "kwargs": {
            "columns": columns,
            "group_col": group_col,
        },
    }

    # Create subtask on all nodes
    info("Creating subtask for all selected organizations.")
    task = client.task.create(
        input_=subtask_input,
        organizations=organizations_to_include,
        name="Subtask: per-group mean/variance/count",
        description=(
            "Compute local mean, sample variance, and count per group "
            f"for column(s) in {columns if columns else 'all numeric columns'}."
        ),
    )

    # Fetch results (list of dicts, one per org)
    info("Collecting results from organizations.")
    results = client.wait_for_results(task_id=task.get("id"))
    info("Results received from all organizations.")

    if not results:
        raise UserInputError("No results received from the organizations.")

    # Determine which groups exist globally.
    # Each organization's result is a dict: { "<group_value>": { "<col>": {stats} } }
    global_groups = set().union(*(org_result.keys() for org_result in results))

    if len(global_groups) != 2:
        raise UserInputError(
            f"Exactly 2 distinct groups in '{group_col}' are required globally, "
            f"found: {sorted(global_groups)}"
        )

    # Fix a deterministic order for the groups (alphabetical / string order)
    group_a, group_b = sorted(global_groups)

    # Determine union of all column names that appear in *either* group across all orgs
    all_columns = set().union(
        *(set(org_result.get(group_a, {}).keys()) for org_result in results),
        *(set(org_result.get(group_b, {}).keys()) for org_result in results),
    )

    info(
        f"Global groups detected: {group_a!r} vs {group_b!r}. "
        f"Columns to test: {sorted(all_columns)}"
    )

    t_test_results: Dict[str, Dict[str, float]] = {}

    for col_name in all_columns:
        # Collect per-site stats for each group, for this column
        group_a_stats = [
            org_result[group_a][col_name]
            for org_result in results
            if group_a in org_result and col_name in org_result[group_a]
        ]
        group_b_stats = [
            org_result[group_b][col_name]
            for org_result in results
            if group_b in org_result and col_name in org_result[group_b]
        ]

        # If either group has no data for this column across all nodes, skip it
        if not group_a_stats or not group_b_stats:
            info(
                f"Skipping column '{col_name}' because one of the groups "
                "has no data across all organizations."
            )
            continue

        # Combine stats across organizations to get global stats per group
        combined_a = _combine_stats(group_a_stats)
        combined_b = _combine_stats(group_b_stats)

        # If after combining, one group still doesn't have >=2 samples, skip
        if not combined_a or not combined_b:
            info(
                f"Skipping column '{col_name}' due to insufficient total "
                "records in one of the groups."
            )
            continue

        n_a = combined_a["count"]
        n_b = combined_b["count"]
        mean_a = combined_a["average"]
        mean_b = combined_b["average"]
        var_a = combined_a["variance"]
        var_b = combined_b["variance"]

        # Pooled variance (equal-variance two-sample t-test)
        pooled_var = (
            ((n_a - 1) * var_a + (n_b - 1) * var_b) / (n_a + n_b - 2)
        )

        # t statistic
        denom = (pooled_var / n_a + pooled_var / n_b) ** 0.5
        if denom == 0:
            info(
                f"Skipping column '{col_name}' because pooled variance is zero."
            )
            continue

        t_score = (mean_a - mean_b) / denom

        # Degrees of freedom
        dof = int(n_a + n_b - 2)

        # Two-sided p-value
        p_value = float(2 * (1 - t.cdf(np.abs(t_score), dof)))

        t_test_results[col_name] = {
            "t_score": float(t_score),
            "p_value": p_value,
        }

    return t_test_results
