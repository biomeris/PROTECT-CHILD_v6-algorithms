"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""

import pandas as pd
import json
from scipy.stats import fisher_exact
from typing import Any, Literal

from vantage6.algorithm.tools.util import info
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.client import AlgorithmClient


@central
@algorithm_client
def compute_fisher_exact_test(
    client: AlgorithmClient,
    group_column: str,
    outcome_column: str,
    organizations_to_include: list[int] | None = None,
    alternative: Literal["two-sided", "less", "greater"] = "two-sided",
) -> Any:
    """Central part of the algorithm"""

    # If organization_to_include is None, get all organizations
    if organizations_to_include is None:
        # get all organizations (ids) within the collaboration so you can send a
        # task to them.
        organizations = client.organization.list()
        organizations_to_include = [
            organization.get("id") for organization in organizations
        ]

    # create a subtask for all organizations in the collaboration to get unique values
    # for group and outcome
    info("Creating subtask to retrieve unique values from all participating nodes")
    task = client.task.create(
        method="get_unique_values",
        arguments={
            "group_column": group_column,
            "outcome_column": outcome_column,
        },
        organizations=organizations_to_include,
        name="Discover global categories for Fisher test",
        description="Retrieve the unique non-missing values for the specified group and"
        " outcome columns at the local node",
    )

    # wait for node to return results of the subtask.
    info("Waiting for results")
    categories_results = client.wait_for_results(task_id=task.get("id"))
    info("Results obtained!")

    # Compute global categories
    global_group_values = sorted(
        set().union(*[set(r["group_values"]) for r in categories_results])
    )

    global_outcome_values = sorted(
        set().union(*[set(r["outcome_values"]) for r in categories_results])
    )

    if len(global_group_values) != 2 or len(global_outcome_values) != 2:
        raise ValueError(
            "Fisher's exact test requires exactly 2 group values and 2 outcome values."
        )

    # create a subtask for all organizations in the collaboration.
    info("Creating subtask to compute local contingency tables")
    task = client.task.create(
        method="compute_local_contingency_table",
        arguments={
            "group_column": group_column,
            "outcome_column": outcome_column,
            "group_values": global_group_values,
            "outcome_values": global_outcome_values,
        },
        organizations=organizations_to_include,
        name="Local 2x2 contingency table",
        description="Computes the local counts for each combination of group and binary"
        " outcome",
    )

    # wait for node to return results of the subtask.
    info("Waiting for results")
    results = client.wait_for_results(task_id=task.get("id"))
    info("Results obtained!")

    info("Aggregating 2x2 contingency tables")
    tables = []
    for r in results:
        d = json.loads(r)

        table = pd.DataFrame(d["data"], index=d["index"], columns=d["columns"])

        tables.append(table)

    aggregated_table = sum(tables)

    groups = list(aggregated_table.index)
    outcomes = list(aggregated_table.columns)

    info("Building contingency table for Fisher's exact test")
    contingency_table = [
        [
            int(aggregated_table.loc[groups[0], outcomes[0]]),
            int(aggregated_table.loc[groups[0], outcomes[1]]),
        ],
        [
            int(aggregated_table.loc[groups[1], outcomes[0]]),
            int(aggregated_table.loc[groups[1], outcomes[1]]),
        ],
    ]

    info("Running Fisher's exact test")
    oddsratio, p_value = fisher_exact(contingency_table, alternative=alternative)

    fisher_results = {
        "oddsratio": float(oddsratio),
        "p_value": float(p_value),
        "contingency_table": contingency_table,
        "group_values": groups,
        "outcome_values": outcomes,
    }

    # return the final results of the algorithm
    info("Returning results!")
    return fisher_results
