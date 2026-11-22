"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""

from typing import Any

from vantage6.algorithm.tools.util import info, warn
from vantage6.algorithm.tools.exceptions import InputError
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.client import AlgorithmClient

from .manova_math import (
    N_THRESHOLD_FLAG,
    combine,
    manova_tests,
    sscp_matrices,
)


def _validate_arguments(group_column: Any, outcome_columns: Any) -> list[str]:
    if not isinstance(group_column, str) or not group_column:
        raise InputError("'group_column' must be a non-empty string.")
    if not isinstance(outcome_columns, (list, tuple)) or not all(
        isinstance(c, str) for c in outcome_columns
    ):
        raise InputError("'outcome_columns' must be a list of column names.")
    outcome_columns = list(outcome_columns)
    if len(outcome_columns) < 2:
        raise InputError(
            "MANOVA needs at least 2 outcome columns; for a single outcome use a "
            "one-way ANOVA instead."
        )
    if len(set(outcome_columns)) != len(outcome_columns):
        raise InputError(f"'outcome_columns' contains duplicates: {outcome_columns}")
    if group_column in outcome_columns:
        raise InputError("'group_column' must not be one of the outcome columns.")
    return outcome_columns


@central
@algorithm_client
def manova(
    client: AlgorithmClient,
    group_column: str,
    outcome_columns: list[str],
    organizations_to_include: list[int] | None = None,
) -> Any:
    """
    Federated one-way MANOVA.

    Tests whether the mean vector of ``outcome_columns`` differs between the
    groups defined by ``group_column``. Each node shares only per-group counts,
    sums and cross-product matrices; the central function builds the within- and
    between-group SSCP matrices from these and computes Wilks' lambda, Pillai's
    trace, the Hotelling-Lawley trace and Roy's greatest root. The result is
    identical to a MANOVA on the pooled data.

    Parameters
    ----------
    client : AlgorithmClient
        The client instance used to interact with the vantage6 server.
    group_column : str
        The column defining the groups (the factor of the MANOVA).
    outcome_columns : list[str]
        The numeric dependent variables (at least 2).
    organizations_to_include : list[int], optional
        The organizations to include in the analysis. Defaults to every
        organization in the collaboration.

    Returns
    -------
    dict
        The four test statistics with their F approximations, the group sizes
        and means, the SSCP matrices, and which organizations were included or
        excluded.
    """
    outcome_columns = _validate_arguments(group_column, outcome_columns)

    if organizations_to_include is None:
        organizations = client.organization.list()
        organizations_to_include = [o.get("id") for o in organizations]
    ids = list(organizations_to_include)
    if not ids:
        raise InputError("'organizations_to_include' must not be empty.")

    # One subtask per organization so every result can be attributed to the
    # organization that produced it (the order of results across a multi-
    # organization task is not guaranteed).
    info(f"Creating subtasks for organizations {ids}")
    tasks = {}
    for org_id in ids:
        tasks[org_id] = client.task.create(
            method="compute_local_summary",
            arguments={
                "group_column": group_column,
                "outcome_columns": outcome_columns,
            },
            organizations=[org_id],
            name="MANOVA local summary",
            description="Per-group counts, sums and cross-product matrices",
        )

    info("Waiting for results")
    summaries, included, excluded = [], [], []
    suppressed: dict[str, list[str]] = {}
    for org_id, task in tasks.items():
        results = client.wait_for_results(task_id=task.get("id"))
        if len(results) != 1 or results[0] is None:
            raise InputError(
                f"Organization {org_id} returned no result; its task run may have "
                "failed. Aborting."
            )
        result = results[0]
        if N_THRESHOLD_FLAG in result:
            warn(
                f"Insufficient records for organization {org_id}. Excluding "
                "organization from analysis."
            )
            excluded.append(org_id)
            continue
        included.append(org_id)
        summaries.append(result)
        if result["suppressed_groups"]:
            suppressed[str(org_id)] = result["suppressed_groups"]
    info("Results obtained!")

    output = {
        "included_organizations": included,
        "excluded_organizations": excluded,
        "suppressed_groups": suppressed,
        "outcome_columns": outcome_columns,
        "warnings": [],
    }

    if not summaries:
        warn("No organization meets the minimum number of records.")
        output["warnings"].append(
            "No organization meets the minimum number of records; nothing computed."
        )
        output["tests"] = None
        return output

    if len(included) == 1 and len(ids) > 1:
        output["warnings"].append(
            "Only one organization contributed; the result describes that "
            "organization's data alone."
        )

    info("Aggregating per-group statistics")
    pooled = combine(summaries)
    W, B, N, k = sscp_matrices(pooled)

    info("Computing multivariate test statistics")
    table = manova_tests(W, B, N, k)

    output.update(
        {
            "n_total": int(N),
            "n_groups": int(k),
            "df_hypothesis": int(k - 1),
            "df_error": int(N - k),
            "group_sizes": {lbl: int(g["n"]) for lbl, g in pooled["groups"].items()},
            "group_means": {
                lbl: (g["sum"] / g["n"]).tolist() for lbl, g in pooled["groups"].items()
            },
            "sscp_between": B.tolist(),
            "sscp_within": W.tolist(),
            "tests": {
                name: {
                    "value": float(row["Value"]),
                    "num_df": float(row["Num DF"]),
                    "den_df": float(row["Den DF"]),
                    "f_value": float(row["F Value"]),
                    "p_value": float(row["Pr > F"]),
                }
                for name, row in table.iterrows()
            },
        }
    )

    info("Returning results!")
    return output
