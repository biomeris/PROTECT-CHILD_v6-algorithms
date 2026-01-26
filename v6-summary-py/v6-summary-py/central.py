"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""

from typing import Any
import pandas as pd
import json as js

from vantage6.algorithm.tools.util import info
from vantage6.algorithm.tools.decorators import algorithm_client
from vantage6.algorithm.tools.exceptions import AlgorithmExecutionError, InputError
from vantage6.algorithm.client import AlgorithmClient


@algorithm_client
def summary(
    client: AlgorithmClient,
    columns: list[str] | None = None,
    numeric_columns: list[str] | None = None,
    organizations_to_include: list[int] | None = None,
) -> Any:
    """
    Send task to each node participating in the task to compute a local summary,
    aggregate them for all nodes, and return the result.

    Parameters
    ----------
    client : AlgorithmClient
        The client object used to communicate with the server.
    columns : list[str] | None
        The columns to include in the summary. If not given, all columns are included.
    numeric_columns : list[str] | None
        Which of the columns are numeric. If not given, it will be inferred from the
        data.
    organizations_to_include : list[int] | None
        The organizations to include in the task. If not given, all organizations
        in the collaboration are included.
    """
    if columns and numeric_columns and not set(numeric_columns).issubset(set(columns)):
        numeric_not_in_columns = set(numeric_columns) - set(columns)
        raise InputError(
            "The 'numeric_columns' should be a subset of 'columns'. The following "
            f"columns are not in 'columns': {numeric_not_in_columns}"
        )

    # get all organizations (ids) within the collaboration so you can send a
    # task to them.
    if not organizations_to_include:
        organizations = client.organization.list()
        organizations_to_include = [
            organization.get("id") for organization in organizations
        ]

    # Define input parameters for a subtask
    info("Defining input parameters")
    input_ = {
        "method": "summary_per_data_station",
        "kwargs": {
            "columns": columns,
            "numeric_columns": numeric_columns,
        },
    }

    # create a subtask for all organizations in the collaboration.
    info("Creating subtask for all organizations in the collaboration")
    task = client.task.create(
        input_=input_,
        organizations=organizations_to_include,
        name="Subtask summary",
        description="Compute summary per data station",
    )

    # wait for node to return results of the subtask.
    info("Waiting for results")
    results = client.wait_for_results(task_id=task.get("id"))
    info("Results obtained!")

    # aggregate the partial summaries of all nodes
    results = _aggregate_partial_summaries(results)

    # compute the variance now that we have the mean
    numerical_columns = list(results["numeric"].keys())
    means = [float(results["numeric"][column]["mean"]) for column in numerical_columns]
    if numerical_columns:
        task = client.task.create(
            input_={
                "method": "variance_per_data_station",
                "kwargs": {
                    "columns": numerical_columns,
                    "means": means,
                },
            },
            organizations=organizations_to_include,
            name="Subtask variance",
            description="Compute variance per data station",
        )
        variance_results = client.wait_for_results(task_id=task.get("id"))

        # add the standard deviation to the results
        results = _add_sd_to_results(results, variance_results, numerical_columns)

    # return the final results of the algorithm
    return {
        "numeric": js.dumps(results["numeric"], indent=2),
        "categorical": results["categorical"].to_json(),
        "counts_unique_values": results["counts_unique_values"].to_json(),
        "num_complete_rows_per_node": results["num_complete_rows_per_node"].to_json(),
    }


def _aggregate_partial_summaries(results: list[dict]) -> dict:
    """Aggregate the partial summaries of all nodes.

    Parameters
    ----------
    results : list[dict]
        The partial summaries of all nodes.
    """
    info("Aggregating partial summaries")
    aggregated_summary = {}
    is_first = True
    for result in results:
        if is_first:
            # copy results. Only convert num complete rows per node to a list so that
            # we can add the other nodes to it later
            aggregated_summary["numeric"] = result["numeric"]
            aggregated_summary["categorical"] = pd.DataFrame(result["categorical"])
            aggregated_summary["counts_unique_values"] = pd.DataFrame(
                result["counts_unique_values"]
            )
            aggregated_summary["num_complete_rows_per_node"] = [
                result["num_complete_rows_per_node"]
            ]
            for column in result["numeric"]:
                aggregated_summary["numeric"][column]["25%"] = [
                    result["numeric"][column]["25%"]
                ]
                aggregated_summary["numeric"][column]["50%"] = [
                    result["numeric"][column]["50%"]
                ]
                aggregated_summary["numeric"][column]["75%"] = [
                    result["numeric"][column]["75%"]
                ]
                aggregated_summary["numeric"][column]["IQR"] = [
                    result["numeric"][column]["IQR"]
                ]
            is_first = False
            continue

        # aggregate data for numeric colums
        numeric_result = pd.DataFrame(result["numeric"])
        if not numeric_result.empty:
            for column in numeric_result.columns:
                aggregated_summary["numeric"][column]["count"] += float(
                    numeric_result.at["count", column]
                )
                aggregated_summary["numeric"][column]["min"] = float(
                    min(
                        aggregated_summary["numeric"][column]["min"],
                        numeric_result.at["min", column],
                    )
                )
                aggregated_summary["numeric"][column]["max"] = float(
                    max(
                        aggregated_summary["numeric"][column]["max"],
                        numeric_result.at["max", column],
                    )
                )
                aggregated_summary["numeric"][column]["missing"] += float(
                    numeric_result.at["missing", column]
                )
                aggregated_summary["numeric"][column]["sum"] += float(
                    numeric_result.at["sum", column]
                )
                aggregated_summary["numeric"][column]["25%"].append(
                    result["numeric"][column]["25%"]
                )
                aggregated_summary["numeric"][column]["50%"].append(
                    result["numeric"][column]["50%"]
                )
                aggregated_summary["numeric"][column]["75%"].append(
                    result["numeric"][column]["75%"]
                )
                aggregated_summary["numeric"][column]["IQR"].append(
                    result["numeric"][column]["IQR"]
                )

        # aggregate data for categorical columns
        categorical_result = pd.DataFrame(result["categorical"])
        if not categorical_result.empty:
            aggregated_summary["categorical"].loc["count"] += categorical_result.loc[
                "count"
            ]
            aggregated_summary["categorical"].loc["missing"] += categorical_result.loc[
                "missing"
            ]

        # add the number of complete rows for this node
        aggregated_summary["num_complete_rows_per_node"].append(
            result["num_complete_rows_per_node"]
        )

        # add the unique values
        unique_values_result = pd.DataFrame(result["counts_unique_values"])
        if not unique_values_result.empty:
            aggregated_summary["counts_unique_values"] = aggregated_summary[
                "counts_unique_values"
            ].add(unique_values_result, fill_value=0)

    # now that all data is aggregated, we can compute the mean
    if bool(aggregated_summary["numeric"]):
        for column in aggregated_summary["numeric"]:
            if aggregated_summary["numeric"][column]["count"]:
                aggregated_summary["numeric"][column]["mean"] = (
                    aggregated_summary["numeric"][column]["sum"]
                    / aggregated_summary["numeric"][column]["count"]
                )

    # convert the list of complete rows per node to a pandas series
    aggregated_summary["num_complete_rows_per_node"] = pd.Series(
        aggregated_summary["num_complete_rows_per_node"]
    )

    return aggregated_summary


def _add_sd_to_results(
    results: dict, variance_results: list[dict], numerical_columns: list[str]
) -> dict:
    """Add the variance to the results.

    Parameters
    ----------
    results : dict
        The results of the summary task.
    variance_results : list[dict]
        The variance results of all nodes.
    numerical_columns : list[str]
        The numerical columns.

    Returns
    -------
    dict
        The results with the variance added.
    """
    if not numerical_columns:
        return results
    variance_df = pd.DataFrame(variance_results)
    variance_sum_all_nodes = variance_df.sum()
    for column in numerical_columns:
        if results["numeric"][column]["count"] > 1:
            results["numeric"][column]["std"] = (
                float(
                    variance_sum_all_nodes[column]
                    / (results["numeric"][column]["count"] - 1)
                )
                ** 0.5
            )
    return results
