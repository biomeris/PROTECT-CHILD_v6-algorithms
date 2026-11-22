"""
This file contains all federated algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the federated task
or directly to the user (if they requested federated results).
"""

import pandas as pd

from vantage6.algorithm.tools.util import info, warn, get_env_var
from vantage6.algorithm.decorator.action import federated
from vantage6.algorithm.decorator.data import dataframe

from .globals import MANOVA_MINIMUM_GROUP_SIZE, MANOVA_MINIMUM_NUMBER_OF_RECORDS
from .manova_math import N_THRESHOLD_FLAG, local_summary


@federated
@dataframe(1)
def compute_local_summary(
    df: pd.DataFrame, group_column: str, outcome_columns: list[str]
) -> dict:
    """
    Compute the per-group sufficient statistics for a one-way MANOVA.

    For every group present locally, the node returns the number of complete
    cases ``n``, the column sums of the outcome columns and the cross-product
    matrix ``X^T X``. The central function adds these up over all nodes to build
    the within- and between-group SSCP matrices.

    Parameters
    ----------
    df : pd.DataFrame
        The data for the data station.
    group_column : str
        The column defining the groups (the factor of the MANOVA).
    outcome_columns : list[str]
        The numeric dependent variables.

    Returns
    -------
    dict
        Either ``{"outcome_columns": [...], "groups": {label: {"n", "sum",
        "xtx"}}, "suppressed_groups": [...]}`` or
        ``{"N-Threshold not met": True}`` if there are too few local records.
    """
    minimum_records = get_env_var(
        "MANOVA_MINIMUM_NUMBER_OF_RECORDS",
        MANOVA_MINIMUM_NUMBER_OF_RECORDS,
        as_type="int",
    )
    minimum_group_size = get_env_var(
        "MANOVA_MINIMUM_GROUP_SIZE", MANOVA_MINIMUM_GROUP_SIZE, as_type="int"
    )

    info("Computing local per-group sums and cross-product matrices")
    summary = local_summary(
        df, group_column, outcome_columns, minimum_records, minimum_group_size
    )

    if N_THRESHOLD_FLAG in summary:
        warn(
            "Sub-task was not executed because the number of local records is too "
            f"small (n <= {minimum_records})"
        )
        return summary

    if summary["suppressed_groups"]:
        warn(
            f"Groups {summary['suppressed_groups']} have fewer than "
            f"{minimum_group_size} local records and are left out of this node's "
            "contribution"
        )

    info("Returning results!")
    return summary
