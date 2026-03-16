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

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.tools.decorators import algorithm_client
from vantage6.algorithm.client import AlgorithmClient


@algorithm_client
def central(
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

    # Define input parameters for a subtask
    info("Defining input parameters")
    input_ = {
        "method": "partial",
        "kwargs": {
            "group_column": group_column,
            "outcome_column": outcome_column,
        },
    }

    # create a subtask for all organizations in the collaboration.
    info("Creating subtask for all organizations in the collaboration")
    task = client.task.create(
        input_=input_,
        organizations=organizations_to_include,
        name="Local 2x2 contingency table",
        description="Computes the local counts for each combination of group and binary"
        " outcome",
    )

    # wait for node to return results of the subtask.
    info("Waiting for results")
    results = client.wait_for_results(task_id=task.get("id"))
    info("Results obtained!")

    tables = []

    for r in results:
        d = json.loads(r)

        table = pd.DataFrame(d["data"], index=d["index"], columns=d["columns"])

        tables.append(table)

    aggregated_table = sum(tables)

    groups = sorted(aggregated_table.index)
    outcomes = sorted(aggregated_table.columns)

    contingency_table = [
        [
            int(aggregated_table.loc[groups[0], outcomes[1]]),
            int(aggregated_table.loc[groups[0], outcomes[0]]),
        ],
        [
            int(aggregated_table.loc[groups[1], outcomes[1]]),
            int(aggregated_table.loc[groups[1], outcomes[0]]),
        ],
    ]

    oddsratio, p_value = fisher_exact(contingency_table, alternative=alternative)

    fisher_results = {
        "oddsratio": float(oddsratio),
        "p_value": float(p_value),
    }

    # return the final results of the algorithm
    return fisher_results
