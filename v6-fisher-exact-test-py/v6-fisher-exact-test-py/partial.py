"""
This file contains all partial algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the partial task
or directly to the user (if they requested partial results).
"""

import pandas as pd
from typing import Any

from vantage6.algorithm.tools.util import info, warn, error, get_env_var
from vantage6.algorithm.tools.decorators import data
from vantage6.algorithm.tools.exceptions import InputError
from .globals import FISHER_MINIMUM_NUMBER_OF_RECORDS


@data(1)
def partial(df: pd.DataFrame, group_column, outcome_column) -> str:
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

    Returns
    -------
    pd.DataFrame
        The contingency table containing the counts of observations for each combination
        of the group and binary outcome variables.
    """

    info("Checking number of records in the DataFrame.")
    MINIMUM_NUMBER_OF_RECORDS = get_env_var(
        "T_TEST_MINIMUM_NUMBER_OF_RECORDS",
        FISHER_MINIMUM_NUMBER_OF_RECORDS,
        as_type="int",
    )

    if len(df) <= MINIMUM_NUMBER_OF_RECORDS:
        raise InputError(
            "Number of records in 'df' must be greater than "
            f"{MINIMUM_NUMBER_OF_RECORDS}."
        )

    # Check that columns are binary

    contingency_table_df = pd.crosstab(df[group_column], df[outcome_column])

    info("Returning results!")
    return contingency_table_df.to_json(orient="split")
