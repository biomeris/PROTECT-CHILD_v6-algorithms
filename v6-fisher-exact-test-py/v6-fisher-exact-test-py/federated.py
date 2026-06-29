"""
This file contains all federated algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the federated task
or directly to the user (if they requested federated results).
"""

import pandas as pd
from typing import Any

from vantage6.algorithm.tools.util import info, get_env_var
from vantage6.algorithm.decorator.action import federated
from vantage6.algorithm.decorator.data import dataframe
from vantage6.algorithm.tools.exceptions import InputError
from .globals import FISHER_MINIMUM_NUMBER_OF_RECORDS


@federated
@dataframe(1)
def get_unique_values(df: pd.DataFrame, group_column, outcome_column) -> Any:
    """
    Retrieve the unique non-missing values for the specified group and outcome
    columns at the local node.

    This function is intended to be used as a first step in a federated workflow,
    where each node reports the categories observed locally. The central server
    can then aggregate these values to determine the global category set before
    computing a consistent contingency table across nodes.

    Parameters
    ----------
    df : pd.DataFrame
        The data for the data station
    group_column :
        The column defining the two groups whose outcome distributions are compared.
    outcome_column :
        The column containing the binary outcome variable used to compute event counts.

    Returns
    -------
    dict
        A dictionary containing:
        - "group_values": list of unique non-null values in the group column
        - "outcome_values": list of unique non-null values in the outcome column
    """

    # Check that columns exist
    info("Checking that columns exist...")
    if group_column not in df.columns:
        raise InputError(f"Column '{group_column}' not found in DataFrame.")

    if outcome_column not in df.columns:
        raise InputError(f"Column '{outcome_column}' not found in DataFrame.")

    # Check that group_column and outcome_column are different
    if group_column == outcome_column:
        raise InputError("group_column and outcome_column must be different.")

    info("Extracting unique non-null values from columns...")
    group_values = df[group_column].dropna().unique().tolist()
    outcome_values = df[outcome_column].dropna().unique().tolist()

    info("Returning results!")
    return {"group_values": group_values, "outcome_values": outcome_values}


@federated
@dataframe(1)
def compute_local_contingency_table(
    df: pd.DataFrame, group_column, outcome_column, group_values, outcome_values
) -> Any:
    """
    The function computes the local counts for each combination of group and binary
    outcome, forming a 2×2 contingency table. The aggregated counts are then sent to
    the central server to perform the global Fisher’s exact test.

    Parameters
    ----------
    df : pd.DataFrame
        The data for the data station
    group_column :
        The column defining the two groups whose outcome distributions are compared.
    outcome_column :
        The column containing the binary outcome variable used to compute event counts.
    group_values : list
        Global list of group categories (provided by central step).
    outcome_values : list
        Global list of outcome categories (provided by central step).


    Returns
    -------
    pd.DataFrame
        The contingency table containing the counts of observations for each combination
        of the group and binary outcome variables.
    """

    info("Checking number of records in the DataFrame.")
    MINIMUM_NUMBER_OF_RECORDS = get_env_var(
        "FISHER_MINIMUM_NUMBER_OF_RECORDS",
        FISHER_MINIMUM_NUMBER_OF_RECORDS,
        as_type="int",
    )

    if len(df) <= MINIMUM_NUMBER_OF_RECORDS:
        raise InputError(
            "Number of records in 'df' must be greater than "
            f"{MINIMUM_NUMBER_OF_RECORDS}."
        )

    info("Calculating local contingency table.")
    df = df[[group_column, outcome_column]].dropna()

    contingency_table_df = pd.crosstab(df[group_column], df[outcome_column])
    contingency_table_df = contingency_table_df.reindex(
        index=group_values, columns=outcome_values, fill_value=0
    )

    info("Returning results!")
    return contingency_table_df.to_json(orient="split")
