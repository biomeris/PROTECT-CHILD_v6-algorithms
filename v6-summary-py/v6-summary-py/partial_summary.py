"""
This file contains all partial algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the partial task
or directly to the user (if they requested partial results).
"""

import pandas as pd
import numpy as np

from vantage6.algorithm.tools.util import info, warn, get_env_var
from vantage6.algorithm.tools.decorators import data
from vantage6.algorithm.tools.exceptions import InputError
from .globals import (
    DEFAULT_PRIVACY_THRESHOLD,
    ENVVAR_PRIVACY_THRESHOLD,
    EnvVarsAllowed,
)
from .utils import check_privacy, check_match_inferred_numeric


@data(1)
def summary_per_data_station(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    numeric_columns: list[str] | None = None,
) -> dict:
    """
    Compute the summary statistics for a single data station to share with the
    aggregator part of the algorithm

    Parameters
    ----------
    df : pd.DataFrame
        The data for the data station
    columns : list[str] | None
        The columns to compute the summary statistics for. If not provided, all columns
        are included.
    numeric_columns : list[str] | None
        List of columns that are numeric. If not provided, it will be inferred from the
        data.

    Returns
    -------
    dict | None
        The summary statistics for the data station. If the summary statistics cannot
        be computed, None is returned
    """
    return _summary_per_data_station(df, columns, numeric_columns)


def _summary_per_data_station(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    numeric_columns: list[str] | None = None,
) -> dict:
    if not columns:
        columns = df.columns

    # Check that column names exist in the dataframe
    if not all([col in df.columns for col in columns]):
        non_existing_columns = [col for col in columns if col not in df.columns]
        raise InputError(
            f"Columns {non_existing_columns} do not exist in the dataframe"
        )

    # filter dataframe to only include the columns of interest
    df = df[columns]

    # Check privacy settings
    info("Checking if data complies to privacy settings")
    check_privacy(df, columns)

    # Split the data in numeric and non-numeric columns
    # inferred_numeric_columns = [df[col].name in [int, float] for col in df.columns]
    inferred_numeric_columns = df.select_dtypes(include=[int, float]).columns.tolist()
    if numeric_columns is None:
        numeric_columns = inferred_numeric_columns
    else:
        df = check_match_inferred_numeric(numeric_columns, inferred_numeric_columns, df)

    # set numeric and non-numeric columns
    non_numeric_columns = list(set(columns) - set(numeric_columns))
    df_numeric = df[numeric_columns]
    df_non_numeric = df[non_numeric_columns]

    # compute data summary for numeric columns
    summary_numeric = pd.DataFrame()
    if not df_numeric.empty:
        summary_numeric = _get_numeric_summary(df_numeric)

    # compute data summary for non-numeric columns. Also compute the counts of the
    # unique values in the non-numeric columns (if they meet the privacy threshold)
    summary_categorical = pd.DataFrame()
    counts_unique_values = {}
    if not df_non_numeric.empty:
        summary_categorical = _get_categorical_summary(df_non_numeric)
        counts_unique_values = _get_counts_unique_values(df_non_numeric)

    # count complete rows without missing values
    num_complete_rows_per_node = len(df.dropna())

    # filter out the variables that are not allowed to be shared
    summary_numeric, summary_categorical = _filter_results(
        summary_numeric, summary_categorical
    )
    if not get_env_var(
        EnvVarsAllowed.ALLOW_NUM_COMPLETE_ROWS.value, default="true", as_type="bool"
    ):
        warn(
            "Removing number of complete rows from summary as policies do not "
            "allow sharing it."
        )
        num_complete_rows_per_node = None
    if not get_env_var(
        EnvVarsAllowed.ALLOW_COUNTS_UNIQUE_VALUES.value, default="true", as_type="bool"
    ):
        warn(
            "Removing counts of unique values from summary as policies do not "
            "allow sharing it."
        )
        counts_unique_values = None

    return {
        "numeric": summary_numeric.to_dict(),
        "categorical": summary_categorical.to_dict(),
        "num_complete_rows_per_node": num_complete_rows_per_node,
        "counts_unique_values": counts_unique_values,
    }


def _get_numeric_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the summary statistics for the numeric columns

    Parameters
    ----------
    df : pd.DataFrame
        The data to compute the summary statistics for
    """
    summary_numeric = df.describe(include=[int, float], percentiles=[0.25, 0.5, 0.75])
    summary_numeric.loc["IQR"] = summary_numeric.loc["75%"] - summary_numeric.loc["25%"]
    summary_numeric.loc["missing"] = df.isna().sum()
    summary_numeric.loc["sum"] = df.sum()
    summary_numeric.drop(["mean", "std"], inplace=True)
    return summary_numeric


def _get_categorical_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the summary statistics for the non-numeric columns

    Parameters
    ----------
    df : pd.DataFrame
        The data to compute the summary statistics for
    """
    # summary for non-numeric columns. Include the NA count and remove the values
    # that we don't want to share
    summary_categorical = df.describe(exclude=[int, float])
    summary_categorical.loc["missing"] = df.isna().sum()
    summary_categorical.drop(["top", "freq", "unique"], inplace=True)
    return summary_categorical


def _get_counts_unique_values(df: pd.DataFrame) -> dict:
    """
    Get the counts of the unique values in categorical columns

    Parameters
    ----------
    df : pd.DataFrame
        The data to get the counts of the unique values for

    Returns
    -------
    dict
        The counts of the unique values
    """
    counts = {}
    privacy_threshold = get_env_var(
        ENVVAR_PRIVACY_THRESHOLD, default=DEFAULT_PRIVACY_THRESHOLD, as_type="int"
    )
    for col in df.columns:
        counts[col] = _mask_privacy(df[col].value_counts(), privacy_threshold, col)
    return counts


def _mask_privacy(counts: pd.Series, privacy_threshold: int, column: str) -> dict:
    """
    Mask the values of a pandas series if the frequency is too low

    Parameters
    ----------
    counts : pd.Series
        The counts of the unique values
    privacy_threshold : int
        The minimum frequency of a value to be shared
    column : str
        The name of the column whose values are counted

    Returns
    -------
    pd.Series
        The masked counts
    """
    num_low_counts = counts[counts < privacy_threshold].sum()
    if num_low_counts > 0:
        # It may be possible to share ranges of values instead of the actual values,
        # but we need to be vary careful. E.g. if the dataframe length is 20 and we
        # have frequencies 2 and 18, masking 2 as 0-5 while sharing 18 and 20 is not
        # effective. Similarly, if we have frequencies 17 and three times 1, masking 1
        # as 0-5 thrice and sharing 17 is also not helpful.
        # Because it is rather difficult to ensure that nothing can be inferred, we
        # choose not to share anything if one of the frequencies is too low.
        # TODO how do we make clear to the user that this happened in the central task?
        warn(
            f"Value counts for column {column} contain values with low frequency. "
            "All counts for this column will be masked."
        )
        return {}
    return counts.to_dict()


def _filter_results(
    summary_numeric: pd.DataFrame, summary_categorical: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Filter out the variables that are not allowed to be shared

    Parameters
    ----------
    summary_numeric : pd.DataFrame
        The summary statistics for the numeric columns
    summary_categorical : pd.DataFrame
        The summary statistics for the non-numeric columns

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        The filtered summary statistics for the numeric and non-numeric columns
    """
    if not get_env_var(EnvVarsAllowed.ALLOW_MIN.value, default="true", as_type="bool"):
        warn("Removing minimum from summary as policies do not allow sharing it.")
        summary_numeric.drop("min", inplace=True)
    if not get_env_var(EnvVarsAllowed.ALLOW_MAX.value, default="true", as_type="bool"):
        warn("Removing maximum from summary as policies do not allow sharing it.")
        summary_numeric.drop("max", inplace=True)
    if not get_env_var(
        EnvVarsAllowed.ALLOW_COUNT.value, default="true", as_type="bool"
    ):
        warn("Removing count from summary as policies do not allow sharing it.")
        summary_numeric.drop("count", inplace=True)
    if not get_env_var(EnvVarsAllowed.ALLOW_SUM.value, default="true", as_type="bool"):
        warn("Removing sum from summary as policies do not allow sharing it.")
        summary_numeric.drop("sum", inplace=True)
    if not get_env_var(
        EnvVarsAllowed.ALLOW_MISSING.value, default="true", as_type="bool"
    ):
        warn("Removing missing from summary as policies do not allow sharing it.")
        summary_numeric.drop("missing", inplace=True)
    return summary_numeric, summary_categorical
