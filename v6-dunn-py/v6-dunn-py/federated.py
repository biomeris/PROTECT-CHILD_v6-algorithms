"""
This file contains all federated algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the federated task
or directly to the user (if they requested federated results).

None of the three functions returns an individual value: only counts, sums and
sums of squares.
"""

import pandas as pd

from vantage6.algorithm.tools.util import info, warn, get_env_var
from vantage6.algorithm.decorator.action import federated
from vantage6.algorithm.decorator.data import dataframe

from .globals import (
    DUNN_MINIMUM_CELL_COUNT,
    DUNN_MINIMUM_GROUP_SIZE,
    DUNN_MINIMUM_NUMBER_OF_RECORDS,
)
from .dunn_math import (
    N_THRESHOLD_FLAG,
    local_histogram,
    local_moments,
    local_rank_scores,
)


def _minimum_group_size() -> int:
    return get_env_var(
        "DUNN_MINIMUM_GROUP_SIZE", DUNN_MINIMUM_GROUP_SIZE, as_type="int"
    )


@federated
@dataframe(1)
def compute_local_moments(df: pd.DataFrame, group_column: str, column: str) -> dict:
    """
    Round 1: count, sum and sum of squares of the eligible values.

    No group breakdown, no extremes, no quantiles. The central function uses
    these to lay out a shared grid for round 2.

    Parameters
    ----------
    df : pd.DataFrame
        The data for the data station.
    group_column : str
        The column defining the groups.
    column : str
        The numeric column whose ranks are compared.

    Returns
    -------
    dict
        ``{"n", "sum", "sum_sq", "suppressed_groups"}`` or
        ``{"N-Threshold not met": True}`` if there are too few local records.
    """
    minimum_records = get_env_var(
        "DUNN_MINIMUM_NUMBER_OF_RECORDS", DUNN_MINIMUM_NUMBER_OF_RECORDS, as_type="int"
    )
    info("Dunn round 1: computing count, sum and sum of squares")
    moments = local_moments(
        df, group_column, column, minimum_records, _minimum_group_size()
    )
    if N_THRESHOLD_FLAG in moments:
        warn(
            "Sub-task was not executed because the number of local records is too "
            f"small (n <= {minimum_records})"
        )
    elif moments["suppressed_groups"]:
        warn(
            f"Groups {moments['suppressed_groups']} are too small and are left out "
            "of this node's contribution"
        )
    return moments


@federated
@dataframe(1)
def compute_local_histogram(
    df: pd.DataFrame, group_column: str, column: str, edges: list[float]
) -> dict:
    """
    Round 2: group-blind record counts per bin of the shared grid.

    Cells holding fewer than ``DUNN_MINIMUM_CELL_COUNT`` records are merged
    into a neighbouring cell before the counts leave the node.

    Parameters
    ----------
    df : pd.DataFrame
        The data for the data station.
    group_column : str
        The column defining the groups.
    column : str
        The numeric column whose ranks are compared.
    edges : list[float]
        The bin edges laid out by the central function.

    Returns
    -------
    dict
        ``{"counts": [count per bin]}``
    """
    minimum_cell_count = get_env_var(
        "DUNN_MINIMUM_CELL_COUNT", DUNN_MINIMUM_CELL_COUNT, as_type="int"
    )
    info("Dunn round 2: computing the group-blind histogram")
    counts = local_histogram(
        df, group_column, column, edges, _minimum_group_size(), minimum_cell_count
    )
    return {"counts": counts}


@federated
@dataframe(1)
def compute_local_rank_scores(
    df: pd.DataFrame,
    group_column: str,
    column: str,
    edges: list[float],
    mid_ranks: list[float],
) -> dict:
    """
    Round 3: per group, the count, sum and sum of squares of the rank scores.

    Every record is scored with the global mid-rank of the rank bin it falls in.

    Parameters
    ----------
    df : pd.DataFrame
        The data for the data station.
    group_column : str
        The column defining the groups.
    column : str
        The numeric column whose ranks are compared.
    edges : list[float]
        The rank-bin edges derived by the central function.
    mid_ranks : list[float]
        The global mid-rank of each rank bin.

    Returns
    -------
    dict
        ``{"groups": {label: {"n", "score_sum", "score_sq_sum"}}}``
    """
    info("Dunn round 3: computing rank-score sums per group")
    groups = local_rank_scores(
        df, group_column, column, edges, mid_ranks, _minimum_group_size()
    )
    return {"groups": groups}
