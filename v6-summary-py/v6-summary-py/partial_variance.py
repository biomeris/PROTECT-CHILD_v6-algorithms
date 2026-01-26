"""
This file contains all partial algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the partial task
or directly to the user (if they requested partial results).
"""

import pandas as pd

from vantage6.algorithm.tools.util import info, error, get_env_var
from vantage6.algorithm.tools.decorators import data
from vantage6.algorithm.tools.exceptions import InputError
from .utils import check_privacy, cast_df_to_numeric
from .globals import EnvVarsAllowed


@data(1)
def variance_per_data_station(
    df: pd.DataFrame, columns: list[str], means: list[float]
) -> dict:
    """
    Compuate the variance for a single data station to share with the
    aggregator part of the algorithm

    Parameters
    ----------
    df : pd.DataFrame
        The data for the data station
    columns : list[str]
        The columns to compute the summary statistics for
    means: list[float]
        The means of the columns

    Returns
    -------
    dict
        Contains the variance of the numeric columns
    """
    return _variance_per_data_station(df, columns, means)


def _variance_per_data_station(
    df: pd.DataFrame, columns: list[str], means: list[float]
) -> dict:
    if not get_env_var(
        EnvVarsAllowed.ALLOW_VARIANCE.value, default="true", as_type="bool"
    ):
        error("Node policies do not allow sharing the variance.")
        return None
    # Check that column names exist in the dataframe - note that this check should
    # not be necessary if a user runs the central task as is has already been checked
    # in that case
    if not all([col in df.columns for col in columns]):
        non_existing_columns = [col for col in columns if col not in df.columns]
        raise InputError(
            f"Columns {non_existing_columns} do not exist in the dataframe"
        )
    if len(columns) != len(means):
        raise InputError(
            "Length of columns list does not match the length of means list"
        )

    # Filter dataframe to only include the columns of interest
    df = df[columns]

    # Check privacy settings
    info("Checking if data complies to privacy settings")
    check_privacy(df, columns)

    # Cast the columns to numeric
    try:
        cast_df_to_numeric(df, columns)
    except ValueError as exc:
        error(str(exc))
        error("Exiting algorithm...")
        return None

    # Calculate the variance
    info("Calculating variance")
    variances = {}
    for idx, column in enumerate(columns):
        mean = means[idx]
        variances[column] = ((df[column].astype(float) - mean) ** 2).sum()

    return variances
