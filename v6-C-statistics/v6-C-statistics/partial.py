"""
Node-side functions for the federated C-statistic algorithm.

None of the three returns an individual record, and none returns a value that
can be traced back to one:

``partial_moments``
    returns only ``n``, ``sum`` and ``sum of squares`` per column, pooled over
    both outcome groups. No minimum, no maximum, no quantile, no outcome label.

``partial_histogram``
    returns only record counts per bin, pooled across outcome groups, against a
    grid the central server derived from those moments. Cells holding fewer than
    ``C_STATISTIC_MINIMUM_CELL_COUNT`` records are merged away before the result
    leaves the node.

``partial_rank_scores``
    returns the sum and sum of squares of the assigned rank scores, and the
    record count, for the event and non-event groups separately — six numbers
    per column. No per-group distribution is transmitted at all.

All three analyse exactly the same records: rows with a non-null outcome, in an
outcome group holding at least ``C_STATISTIC_MINIMUM_GROUP_SIZE`` records for
the column in question, with a non-null score.
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
    C_STATISTIC_MINIMUM_CELL_COUNT,
    C_STATISTIC_MINIMUM_GROUP_SIZE,
    C_STATISTIC_MINIMUM_NUMBER_OF_RECORDS,
)


def _minimum_records() -> int:
    return get_env_var(
        "C_STATISTIC_MINIMUM_NUMBER_OF_RECORDS",
        C_STATISTIC_MINIMUM_NUMBER_OF_RECORDS,
        as_type="int",
    )


def _minimum_group_size() -> int:
    return get_env_var(
        "C_STATISTIC_MINIMUM_GROUP_SIZE",
        C_STATISTIC_MINIMUM_GROUP_SIZE,
        as_type="int",
    )


def _minimum_cell_count() -> int:
    return get_env_var(
        "C_STATISTIC_MINIMUM_CELL_COUNT",
        C_STATISTIC_MINIMUM_CELL_COUNT,
        as_type="int",
    )


def _check_records(df: pd.DataFrame, outcome_col: str) -> None:
    minimum_records = _minimum_records()
    if len(df) <= minimum_records:
        raise InputError(
            f"Number of records must be greater than {minimum_records}."
        )
    if outcome_col not in df.columns:
        raise InputError(f"Outcome column '{outcome_col}' not found in dataframe.")


def _resolve_columns(
    df: pd.DataFrame,
    outcome_col: str,
    columns: Optional[List[str]],
) -> List[str]:
    """Resolve and validate the list of score columns to test."""
    if not columns:
        cols = df.select_dtypes(include=["number"]).columns.tolist()
        cols = [c for c in cols if c != outcome_col]
    else:
        non_existing = [c for c in columns if c not in df.columns]
        if non_existing:
            raise InputError(f"Columns {non_existing} do not exist in the dataframe.")
        non_numeric = [c for c in columns if not ptypes.is_numeric_dtype(df[c])]
        if non_numeric:
            raise InputError(f"Columns {non_numeric} are not numeric.")
        cols = list(columns)

    if not cols:
        raise InputError("No numeric score columns found to test.")
    return cols


def _resolve_positive_label(series: pd.Series, positive_label: Optional[object]):
    """
    Decide which raw outcome value denotes the "event"/positive group.

    Resolved node-locally with no coordination round, so it must be
    deterministic given only this node's data. If the caller supplies
    ``positive_label`` explicitly that value is used as-is — this is the
    recommended path for anything other than a plain 0/1 or boolean column, and
    it works even when a node observes only one of the two outcome values. If
    left unset, only the unambiguous conventions (booleans -> ``True``, a 0/1
    column -> ``1``) are inferred automatically; anything else raises rather
    than guess, since a wrong guess would silently flip the sign of C.
    """
    if positive_label is not None:
        return positive_label
    if ptypes.is_bool_dtype(series):
        return True
    if ptypes.is_numeric_dtype(series):
        distinct = set(series.dropna().unique().tolist())
        if distinct and distinct <= {0, 1}:
            return 1
    raise InputError(
        "Cannot infer the event/positive outcome automatically from column "
        f"'{series.name}'. Pass positive_label explicitly (e.g. positive_label=1)."
    )


def _eligible_groups(
    df: pd.DataFrame,
    outcome_col: str,
    column: str,
    positive_label: Optional[object],
    minimum_group_size: int,
) -> Tuple[Dict[str, pd.Series], List[Dict[str, object]]]:
    """
    Split a score column into its event / non-event value series.

    A group holding fewer than ``minimum_group_size`` non-null values for this
    column is dropped, so no aggregate ever describes a handful of records.
    """
    subset = df[df[outcome_col].notna()]
    resolved_positive = _resolve_positive_label(subset[outcome_col], positive_label)
    is_event = subset[outcome_col] == resolved_positive

    eligible: Dict[str, pd.Series] = {}
    suppressed: List[Dict[str, object]] = []
    for label, mask in (("event", is_event), ("non_event", ~is_event)):
        values = subset.loc[mask, column].dropna()
        if len(values) < minimum_group_size:
            suppressed.append(
                {
                    "column": column,
                    "group": label,
                    "reason": f"fewer than {minimum_group_size} records",
                }
            )
            continue
        eligible[label] = values
    return eligible, suppressed


@data(1)
def partial_moments(
    df: pd.DataFrame,
    outcome_col: str,
    columns: Optional[List[str]] = None,
    positive_label: Optional[object] = None,
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
    _check_records(df, outcome_col)
    columns = _resolve_columns(df, outcome_col, columns)
    minimum_group_size = _minimum_group_size()
    info("C-stat round 1: computing per-column moments (no values leave the node).")

    out: Dict[str, Dict[str, float]] = {}
    for col in columns:
        eligible, _ = _eligible_groups(
            df, outcome_col, col, positive_label, minimum_group_size
        )
        if not eligible:
            info(f"C-stat round 1: column '{col}' suppressed (no group large enough).")
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
    outcome_col: str,
    columns: List[str],
    edges_per_column: Dict[str, List[float]],
    positive_label: Optional[object] = None,
) -> Dict[str, object]:
    """
    Round 2: record counts per bin, pooled across the event/non-event groups.

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
        Counts only, with no outcome breakdown and no cell below the minimum.
    """
    _check_records(df, outcome_col)
    minimum_group_size = _minimum_group_size()
    minimum_cell_count = _minimum_cell_count()
    info("C-stat round 2: computing the outcome-blind histogram (counts only).")

    counts: Dict[str, List[int]] = {}
    suppressed: List[Dict[str, object]] = []

    for col in columns:
        edges = edges_per_column.get(col)
        if col not in df.columns or not edges:
            continue
        eligible, col_suppressed = _eligible_groups(
            df, outcome_col, col, positive_label, minimum_group_size
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
def partial_rank_scores(
    df: pd.DataFrame,
    outcome_col: str,
    columns: List[str],
    edges_per_column: Dict[str, List[float]],
    mid_ranks_per_column: Dict[str, List[float]],
    positive_label: Optional[object] = None,
) -> Dict[str, Dict[str, Dict[str, float]]]:
    """
    Round 3: the rank-score sum, sum of squares and count for each outcome group.

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
        ``{ column: { "event": {"rank_sum", "rank_sq_sum", "n"},
        "non_event": {...} } }`` — six numbers per column. No distribution, no
        value, no ordering of individual records.
    """
    _check_records(df, outcome_col)
    minimum_group_size = _minimum_group_size()
    info("C-stat round 3: computing rank-score sums per outcome group.")

    result: Dict[str, Dict[str, Dict[str, float]]] = {}
    for col in columns:
        edges = edges_per_column.get(col)
        ranks = mid_ranks_per_column.get(col)
        if col not in df.columns or not edges or not ranks:
            continue

        eligible, _ = _eligible_groups(
            df, outcome_col, col, positive_label, minimum_group_size
        )
        col_result: Dict[str, Dict[str, float]] = {}
        for group_label, values in eligible.items():
            scores = [ranks[i] for i in assign_bins(values.tolist(), edges)]
            col_result[group_label] = {
                "rank_sum": float(sum(scores)),
                "rank_sq_sum": float(sum(s * s for s in scores)),
                "n": int(len(scores)),
            }
        if "event" in col_result and "non_event" in col_result:
            result[col] = col_result
    return result
