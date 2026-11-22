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

from .globals import EFA_MINIMUM_NUMBER_OF_RECORDS
from .efa_math import N_THRESHOLD_FLAG, local_moments


@federated
@dataframe(1)
def compute_local_moments(df: pd.DataFrame, columns: list[str]) -> dict:
    """
    Compute the moments needed for the pooled correlation matrix.

    The node returns the number of complete cases ``n``, the column sums and the
    cross-product matrix ``X^T X`` of ``columns``. The central function adds
    these up over all nodes to build the pooled correlation matrix.

    Parameters
    ----------
    df : pd.DataFrame
        The data for the data station.
    columns : list[str]
        The numeric variables to factor-analyse.

    Returns
    -------
    dict
        Either ``{"columns": [...], "n": ..., "sum": [...], "xtx": [[...]]}`` or
        ``{"N-Threshold not met": True}`` if there are too few local records.
    """
    minimum_records = get_env_var(
        "EFA_MINIMUM_NUMBER_OF_RECORDS", EFA_MINIMUM_NUMBER_OF_RECORDS, as_type="int"
    )

    info("Computing local column sums and cross-product matrix")
    moments = local_moments(df, columns, minimum_records)

    if N_THRESHOLD_FLAG in moments:
        warn(
            "Sub-task was not executed because the number of local records is too "
            f"small (n <= {minimum_records})"
        )
        return moments

    info("Returning results!")
    return moments
