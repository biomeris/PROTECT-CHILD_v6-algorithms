"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""
from __future__ import annotations

from typing import Dict, List, Optional

from scipy.stats import mannwhitneyu

from vantage6.algorithm.client import AlgorithmClient
from vantage6.algorithm.tools.decorators import algorithm_client
from vantage6.algorithm.tools.exceptions import UserInputError
from vantage6.algorithm.tools.util import info


@algorithm_client
def central(
    client: AlgorithmClient,
    organizations_to_include: List[int],
    group_col: str,
    columns: Optional[List[str]] = None,
) -> Dict[str, Dict]:
    """
    Central part of the federated Mann-Whitney U test algorithm.

    Orchestrates the federated computation by dispatching a partial task to
    each participating organization, collecting local value lists per group
    per column, merging them across all nodes, and computing the global
    Mann-Whitney U statistic and two-sided p-value for each column.

    The Mann-Whitney U test is a non-parametric alternative to the two-sample
    t-test. It does not assume normality and tests whether one group tends to
    have larger values than the other.

    Parameters
    ----------
    client : AlgorithmClient
        Vantage6 algorithm client used to create subtasks and collect results.
    organizations_to_include : list[int]
        IDs of the organizations that participate in the computation.
    group_col : str
        Column name that defines the two comparison groups. Exactly two
        distinct values must be present globally across all nodes.
    columns : list[str] | None, optional
        Numeric columns to test. If None, all numeric columns are used.

    Returns
    -------
    dict
        Maps each column name to a dict with:
        - ``u_statistic``: Mann-Whitney U statistic
        - ``p_value``: two-sided p-value
        - ``group_a``: label of the first group (alphabetically)
        - ``group_b``: label of the second group (alphabetically)
        - ``n_group_a``: total observations in group A across all nodes
        - ``n_group_b``: total observations in group B across all nodes

    Raises
    ------
    UserInputError
        If no organizations are provided, if the number of distinct groups
        found globally is not exactly 2, or if no results are returned.
    """
    if not organizations_to_include:
        raise UserInputError(
            "Provide at least one organization id in 'organizations_to_include'."
        )

    info(f"Running federated Mann-Whitney U test using group_col='{group_col}'.")

    subtask_input = {
        "method": "partial",
        "kwargs": {"group_col": group_col, "columns": columns},
    }
    task = client.task.create(
        input_=subtask_input,
        organizations=organizations_to_include,
        name="Subtask: local group values for Mann-Whitney U test",
        description=(
            "Collect local sorted values per group for Mann-Whitney U test on "
            f"{columns if columns else 'all numeric columns'}."
        ),
    )

    info("Waiting for partial results from all nodes.")
    results = client.wait_for_results(task_id=task.get("id"))
    if not results:
        raise UserInputError("No results received from the organizations.")

    info("Aggregating local value lists across nodes.")

    # Identify the two groups globally
    global_groups = set().union(*(r.keys() for r in results))
    if len(global_groups) != 2:
        raise UserInputError(
            f"Exactly 2 distinct groups in '{group_col}' are required globally, "
            f"found: {sorted(global_groups)}"
        )
    group_a, group_b = sorted(global_groups)
    info(f"Groups identified: '{group_a}' vs '{group_b}'")

    # Union of all columns present across nodes
    all_columns = set().union(
        *(set(r.get(group_a, {}).keys()) for r in results),
        *(set(r.get(group_b, {}).keys()) for r in results),
    )

    out: Dict[str, Dict] = {}
    for col in sorted(all_columns):
        # Merge values from all nodes for each group
        a_values: List[float] = []
        for r in results:
            a_values.extend(r.get(group_a, {}).get(col, []))

        b_values: List[float] = []
        for r in results:
            b_values.extend(r.get(group_b, {}).get(col, []))

        if len(a_values) < 1 or len(b_values) < 1:
            info(f"Skipping column '{col}': insufficient data in one or both groups.")
            continue

        info(
            f"Computing Mann-Whitney U for '{col}': "
            f"n_{group_a}={len(a_values)}, n_{group_b}={len(b_values)}"
        )
        u_stat, p_value = mannwhitneyu(a_values, b_values, alternative="two-sided")

        out[col] = {
            "u_statistic": float(u_stat),
            "p_value": float(p_value),
            "group_a": group_a,
            "group_b": group_b,
            "n_group_a": len(a_values),
            "n_group_b": len(b_values),
        }

    info("Central federated Mann-Whitney U test finished.")
    return out
