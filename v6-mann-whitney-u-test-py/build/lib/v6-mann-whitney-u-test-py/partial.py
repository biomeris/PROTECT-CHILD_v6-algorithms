"""
This file contains all partial algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the partial task
or directly to the user (if they requested partial results).
"""
from typing import Dict, List, Optional

import pandas as pd
import pandas.api.types as ptypes

from vantage6.algorithm.tools.util import info, get_env_var
from vantage6.algorithm.tools.decorators import data
from vantage6.algorithm.tools.exceptions import InputError

from .globals import MANN_WHITNEY_MINIMUM_NUMBER_OF_RECORDS


@data(1)
def partial(
    df: pd.DataFrame,
    group_col: str,
    columns: Optional[List[str]] = None,
) -> Dict:
    """
    Decentral part of a federated Mann-Whitney U test algorithm.

    For each feature column and each group, returns the locally observed
    values so that the central node can merge them across all data stations
    and compute the global rank-based U statistic without sharing raw data
    beyond the per-column value lists.

    Parameters
    ----------
    df : pd.DataFrame
        The data for the data station.
    group_col : str
        Column name that defines the two comparison groups. Must contain
        exactly two distinct non-null values across all participating nodes.
    columns : list[str] | None, optional
        Numeric columns to test. If None, all numeric columns are used.

    Returns
    -------
    dict
        Nested dict of the form ``{ group_label: { col: [sorted values] } }``.
    """
    info("Checking number of records in the DataFrame.")
    minimum_records = get_env_var(
        "MANN_WHITNEY_MINIMUM_NUMBER_OF_RECORDS",
        MANN_WHITNEY_MINIMUM_NUMBER_OF_RECORDS,
        as_type="int",
    )
    if len(df) <= minimum_records:
        raise InputError(
            f"Number of records in 'df' must be greater than {minimum_records}."
        )

    if group_col not in df.columns:
        raise InputError(f"Group column '{group_col}' not found in dataframe.")

    if not columns:
        columns = df.select_dtypes(include=["number"]).columns.tolist()
    else:
        non_existing = [c for c in columns if c not in df.columns]
        if non_existing:
            raise InputError(
                f"Columns {non_existing} do not exist in the dataframe."
            )
        non_numeric = [c for c in columns if not ptypes.is_numeric_dtype(df[c])]
        if non_numeric:
            raise InputError(f"Columns {non_numeric} are not numeric.")

    if not columns:
        raise InputError("No numeric columns found for Mann-Whitney U test.")

    result: Dict = {}
    for group_label, group_df in df.groupby(group_col):
        group_result: Dict[str, list] = {}
        for col in columns:
            values = group_df[col].dropna().tolist()
            if values:
                group_result[col] = sorted(values)
        result[str(group_label)] = group_result

    info("Partial Mann-Whitney U statistics computation finished.")
    return result
