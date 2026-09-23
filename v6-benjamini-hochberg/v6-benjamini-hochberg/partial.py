"""
Node-side functions for the federated Benjamini-Hochberg algorithm.

None of the three returns an individual record, and none returns a value that
can be traced back to one:

``partial_moments``
    returns only ``n``, ``sum`` and ``sum of squares`` per column. No minimum,
    no maximum, no quantile, no group label.

``partial_histogram``
    returns only record counts per bin, pooled across groups, against a grid the
    central server derived from those moments. Cells holding fewer than
    ``BENJAMINI_HOCHBERG_MINIMUM_CELL_COUNT`` records are merged away before the
    result leaves the node.

``partial_rank_sums``
    returns three numbers per (column, group) — the sum of the assigned rank
    scores, their sum of squares, and the record count — against global
    mid-ranks supplied by the central server. No per-group distribution is
    transmitted at all.

All three analyse exactly the same records: rows with a group label, in a group
holding at least ``BENJAMINI_HOCHBERG_MINIMUM_GROUP_SIZE`` records for the
column in question, with a non-null value.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import pandas as pd
import pandas.api.types as ptypes

from vantage6.algorithm.tools.decorators import data
from vantage6.algorithm.tools.exceptions import InputError
from vantage6.algorithm.tools.util import get_env_var, info

from .binned import assign_bins, merge_small_cells
from .globals import (
    BENJAMINI_HOCHBERG_MINIMUM_CELL_COUNT,
    BENJAMINI_HOCHBERG_MINIMUM_GROUP_SIZE,
    BENJAMINI_HOCHBERG_MINIMUM_NUMBER_OF_RECORDS,
)


def _minimum_records() -> int:
    return get_env_var(
        "BENJAMINI_HOCHBERG_MINIMUM_NUMBER_OF_RECORDS",
        BENJAMINI_HOCHBERG_MINIMUM_NUMBER_OF_RECORDS,
        as_type="int",
    )


def _minimum_group_size() -> int:
    return get_env_var(
        "BENJAMINI_HOCHBERG_MINIMUM_GROUP_SIZE",
        BENJAMINI_HOCHBERG_MINIMUM_GROUP_SIZE,
        as_type="int",
    )


def _minimum_cell_count() -> int:
    return get_env_var(
        "BENJAMINI_HOCHBERG_MINIMUM_CELL_COUNT",
        BENJAMINI_HOCHBERG_MINIMUM_CELL_COUNT,
        as_type="int",
    )


def _check_records(df: pd.DataFrame, group_col: str) -> None:
    minimum_records = _minimum_records()
    if len(df) <= minimum_records:
        raise InputError(
            f"Number of records must be greater than {minimum_records}."
        )
    if group_col not in df.columns:
        raise InputError(f"Group column '{group_col}' not found in dataframe.")


def _resolve_columns(
    df: pd.DataFrame,
    group_col: str,
    columns: Optional[List[str]],
) -> List[str]:
    """Resolve and validate the list of columns to test."""
    if not columns:
        cols = df.select_dtypes(include=["number"]).columns.tolist()
        cols = [c for c in cols if c != group_col]
    else:
        non_existing = [c for c in columns if c not in df.columns]
        if non_existing:
            raise InputError(f"Columns {non_existing} do not exist in the dataframe.")
        non_numeric = [c for c in columns if not ptypes.is_numeric_dtype(df[c])]
        if non_numeric:
            raise InputError(f"Columns {non_numeric} are not numeric.")
        cols = list(columns)

    if not cols:
        raise InputError("No numeric columns found to test.")
    return cols


def _eligible_groups(
    df: pd.DataFrame,
    group_col: str,
    column: str,
    minimum_group_size: int,
) -> Tuple[Dict[str, pd.Series], List[Dict[str, object]]]:
    """
    Split a column into the per-group value series that may be analysed.

    A group holding fewer than ``minimum_group_size`` non-null values for this
    column is dropped, so no aggregate ever describes a handful of records.
    """
    eligible: Dict[str, pd.Series] = {}
    suppressed: List[Dict[str, object]] = []
    subset = df[df[group_col].notna()]
    for group_label, group_df in subset.groupby(group_col):
        values = group_df[column].dropna()
        if len(values) < minimum_group_size:
            suppressed.append(
                {
                    "column": column,
                    "group": str(group_label),
                    "reason": f"fewer than {minimum_group_size} records",
                }
            )
            continue
        eligible[str(group_label)] = values
    return eligible, suppressed


@data(1)
def partial_moments(
    df: pd.DataFrame,
    group_col: str,
    columns: Optional[List[str]] = None,
) -> Dict[str, Dict[str, float]]:
    """
    Round 1: per-column count, sum and sum of squares — nothing else.

    The central server needs a location and a scale to lay out a shared
    reporting grid. These three aggregates supply both without revealing any
    observed value, not even the extremes.

    Returns
    -------
    dict
        ``{ column: {"n": int, "sum": float, "sum_sq": float} }``
    """
    _check_records(df, group_col)
    columns = _resolve_columns(df, group_col, columns)
    minimum_group_size = _minimum_group_size()
    info("BH round 1: computing per-column moments (no values leave the node).")

    out: Dict[str, Dict[str, float]] = {}
    for col in columns:
        eligible, _ = _eligible_groups(df, group_col, col, minimum_group_size)
        if not eligible:
            info(f"BH round 1: column '{col}' suppressed (no group large enough).")
            continue
        values = pd.concat(eligible.values())
        out[col] = {
            "n": int(len(values)),
            "sum": float(values.sum()),
            "sum_sq": float((values ** 2).sum()),
        }
    return out


@data(1)
def partial_histogram(
    df: pd.DataFrame,
    group_col: str,
    columns: List[str],
    edges_per_column: Dict[str, List[float]],
) -> Dict[str, object]:
    """
    Round 2: record counts per bin, pooled across groups.

    Parameters
    ----------
    edges_per_column
        ``{ column: [bin edges] }`` as laid out by the central server. Values
        outside the grid are clipped into the outermost bins, so every record is
        counted exactly once.

    Returns
    -------
    dict
        ``{"counts": { column: [counts per bin] }, "suppressed": [...]}``
        Counts only, with no group breakdown and no cell below the minimum.
    """
    _check_records(df, group_col)
    minimum_group_size = _minimum_group_size()
    minimum_cell_count = _minimum_cell_count()
    info("BH round 2: computing the group-blind histogram (counts only).")

    counts: Dict[str, List[int]] = {}
    suppressed: List[Dict[str, object]] = []

    for col in columns:
        edges = edges_per_column.get(col)
        if col not in df.columns or not edges:
            continue
        eligible, col_suppressed = _eligible_groups(
            df, group_col, col, minimum_group_size
        )
        suppressed.extend(col_suppressed)
        if not eligible:
            continue

        values = pd.concat(eligible.values()).tolist()
        raw = [0] * (len(edges) - 1)
        for idx in assign_bins(values, edges):
            raw[idx] += 1
        counts[col] = merge_small_cells(raw, minimum_cell_count)

    return {"counts": counts, "suppressed": suppressed}


@data(1)
def partial_rank_sums(
    df: pd.DataFrame,
    group_col: str,
    columns: List[str],
    edges_per_column: Dict[str, List[float]],
    mid_ranks_per_column: Dict[str, List[float]],
) -> Dict[str, Dict[str, Dict[str, float]]]:
    """
    Round 3: the rank-score sum, sum of squares and record count per group.

    Parameters
    ----------
    edges_per_column
        ``{ column: [rank bin edges] }`` derived by the central server from the
        pooled histogram.
    mid_ranks_per_column
        ``{ column: [global mid-rank of each rank bin] }``.

    Returns
    -------
    dict
        ``{ column: { group: {"rank_sum": float, "rank_sq_sum": float,
        "n": int} } }`` — three numbers per group. No distribution, no value,
        no ordering.
    """
    _check_records(df, group_col)
    minimum_group_size = _minimum_group_size()
    info("BH round 3: computing rank-score sums per group (three numbers per group).")

    result: Dict[str, Dict[str, Dict[str, float]]] = {}
    for col in columns:
        edges = edges_per_column.get(col)
        ranks = mid_ranks_per_column.get(col)
        if col not in df.columns or not edges or not ranks:
            continue

        eligible, _ = _eligible_groups(df, group_col, col, minimum_group_size)
        col_result: Dict[str, Dict[str, float]] = {}
        for group_label, values in eligible.items():
            scores = [ranks[i] for i in assign_bins(values.tolist(), edges)]
            col_result[group_label] = {
                "rank_sum": float(sum(scores)),
                "rank_sq_sum": float(sum(s * s for s in scores)),
                "n": int(len(scores)),
            }
        if col_result:
            result[col] = col_result
    return result
