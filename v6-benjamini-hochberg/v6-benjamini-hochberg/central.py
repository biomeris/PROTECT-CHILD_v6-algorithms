"""
Central orchestration for the federated Benjamini-Hochberg algorithm.

Two entry points:

``central``
    Applies the Benjamini-Hochberg FDR correction to a family of p-values that
    were produced elsewhere — typically by a previous federated run of the
    Kruskal-Wallis, Mann-Whitney, t-test or ANOVA algorithms in this repository.
    It contacts no node and reads no database.

``central_federated``
    Runs a rank-based test per column across the participating organizations and
    then corrects the resulting family of p-values. The nodes return only
    aggregate statistics: per-column moments, then a group-blind histogram, then
    one rank sum per group. No raw value is ever transmitted.
"""
from __future__ import annotations

from math import sqrt
from typing import Dict, List, Optional

from vantage6.algorithm.client import AlgorithmClient
from vantage6.algorithm.tools.decorators import algorithm_client
from vantage6.algorithm.tools.exceptions import UserInputError
from vantage6.algorithm.tools.util import get_env_var, info

from .bh import PValues, benjamini_hochberg
from .binned import (
    KRUSKAL_WALLIS,
    SUPPORTED_TESTS,
    bin_edges,
    coarsen_histogram,
    mid_ranks,
    run_test,
)
from .globals import (
    BENJAMINI_HOCHBERG_HISTOGRAM_BINS,
    BENJAMINI_HOCHBERG_MAXIMUM_RANK_BINS,
    BENJAMINI_HOCHBERG_MINIMUM_CELL_COUNT,
    BENJAMINI_HOCHBERG_RANGE_SD,
)


def _env_float(var_name: str, default: float) -> float:
    """``get_env_var`` only converts to str/bool/int, so floats are parsed here."""
    value = get_env_var(var_name, None)
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise UserInputError(
            f"Environment variable '{var_name}' has value '{value}', "
            "which cannot be converted to a float."
        ) from exc


def central(p_values: PValues, alpha: float = 0.05) -> Dict:
    """
    Benjamini-Hochberg correction of an existing family of p-values.

    Parameters
    ----------
    p_values
        ``{hypothesis_name: p_value}``, or a plain list of p-values. Entries that
        are ``None`` or ``NaN`` are reported back untested and are excluded from
        the family size ``m``.
    alpha
        Target false discovery rate. Default ``0.05``.

    Returns
    -------
    dict
        ``{"alpha", "n_hypotheses", "n_tested", "n_rejected",
        "largest_rejected_rank", "results"}``.
    """
    info("Benjamini-Hochberg correction on supplied p-values (no node contact).")
    try:
        return benjamini_hochberg(p_values, alpha=alpha)
    except ValueError as exc:
        raise UserInputError(str(exc)) from exc


def _pooled_moments(results: List[Dict], column: str) -> Optional[Dict[str, float]]:
    """Pool per-node ``n``/``sum``/``sum_sq`` into a global mean and sd."""
    n_total = 0
    total = 0.0
    total_sq = 0.0
    n_contributors = 0
    for node in results:
        stats = (node or {}).get(column)
        if not stats:
            continue
        n = int(stats["n"])
        if n <= 0:
            continue
        n_total += n
        total += float(stats["sum"])
        total_sq += float(stats["sum_sq"])
        n_contributors += 1

    if n_total < 3 or not n_contributors:
        return None

    mean = total / n_total
    variance = max(total_sq / n_total - mean ** 2, 0.0)
    return {
        "n": n_total,
        "mean": mean,
        "sd": sqrt(variance),
        "n_contributors": n_contributors,
    }


@algorithm_client
def central_federated(
    client: AlgorithmClient,
    organizations_to_include: List[int],
    group_col: str,
    columns: Optional[List[str]] = None,
    test: str = KRUSKAL_WALLIS,
    alpha: float = 0.05,
) -> Dict:
    """
    Federated per-column rank test followed by Benjamini-Hochberg correction.

    Round 1 collects per-column moments so the central server can lay out a
    shared reporting grid. Round 2 collects a group-blind histogram on that grid,
    which the central server pools and coarsens into roughly equal-count rank
    bins. Round 3 collects the sum and sum of squares of the assigned rank scores
    per group, and the test statistic follows from those alone.

    Records sharing a rank bin are treated as tied; both statistics are computed
    in their general-scores form, which handles that intrinsically.

    Parameters
    ----------
    organizations_to_include
        IDs of the organizations that participate in the computation.
    group_col
        Column that defines the groups.
    columns
        Numeric columns to test — one hypothesis each. If ``None``, all numeric
        columns are used.
    test
        ``"kruskal-wallis"`` (two or more groups, default) or ``"mann-whitney"``
        (exactly two groups).
    alpha
        Target false discovery rate for the correction. Default ``0.05``.

    Returns
    -------
    dict
        ``{"test", "alpha", "n_hypotheses", "n_tested", "n_rejected",
        "columns", "excluded", "privacy"}``.
    """
    if not organizations_to_include:
        raise UserInputError("Provide at least one organization id.")
    if test not in SUPPORTED_TESTS:
        raise UserInputError(
            f"Unknown test '{test}'. Supported: {list(SUPPORTED_TESTS)}."
        )
    if not 0.0 < alpha < 1.0:
        raise UserInputError(f"alpha must be in (0, 1), got {alpha}.")

    histogram_bins = get_env_var(
        "BENJAMINI_HOCHBERG_HISTOGRAM_BINS",
        BENJAMINI_HOCHBERG_HISTOGRAM_BINS,
        as_type="int",
    )
    max_rank_bins = get_env_var(
        "BENJAMINI_HOCHBERG_MAXIMUM_RANK_BINS",
        BENJAMINI_HOCHBERG_MAXIMUM_RANK_BINS,
        as_type="int",
    )
    min_cell_count = get_env_var(
        "BENJAMINI_HOCHBERG_MINIMUM_CELL_COUNT",
        BENJAMINI_HOCHBERG_MINIMUM_CELL_COUNT,
        as_type="int",
    )
    range_sd = _env_float("BENJAMINI_HOCHBERG_RANGE_SD", BENJAMINI_HOCHBERG_RANGE_SD)

    info(f"Federated BH: test='{test}', group_col='{group_col}', alpha={alpha}.")

    # ── Round 1: per-column moments, no values and no group labels ───────────
    task1 = client.task.create(
        input_={
            "method": "partial_moments",
            "kwargs": {"group_col": group_col, "columns": columns},
        },
        organizations=organizations_to_include,
        name="BH round 1: column moments",
        description="Collect per-column n, sum and sum of squares to lay out a shared reporting grid.",
    )
    info("Waiting for round-1 results.")
    r1_results = client.wait_for_results(task_id=task1.get("id"))
    if not r1_results:
        raise UserInputError("No round-1 results received.")

    all_columns = sorted(set().union(*((r or {}).keys() for r in r1_results)))
    if not all_columns:
        raise UserInputError("No column survived the node-side minimum size checks.")

    excluded: Dict[str, str] = {}
    reporting_edges: Dict[str, List[float]] = {}
    contributors: Dict[str, int] = {}
    for col in all_columns:
        moments = _pooled_moments(r1_results, col)
        if moments is None:
            excluded[col] = "too few records across all nodes"
            continue
        if moments["sd"] <= 0:
            excluded[col] = "no variation; all observations are identical"
            continue
        contributors[col] = int(moments["n_contributors"])
        reporting_edges[col] = bin_edges(
            moments["mean"], moments["sd"], histogram_bins, range_sd
        )

    if not reporting_edges:
        raise UserInputError(f"No column could be tested. Reasons: {excluded}")

    # ── Round 2: group-blind histogram on the reporting grid ─────────────────
    histogram_columns = sorted(reporting_edges)
    task2 = client.task.create(
        input_={
            "method": "partial_histogram",
            "kwargs": {
                "group_col": group_col,
                "columns": histogram_columns,
                "edges_per_column": reporting_edges,
            },
        },
        organizations=organizations_to_include,
        name="BH round 2: pooled histogram",
        description="Collect per-column record counts per bin, pooled across groups.",
    )
    info("Waiting for round-2 results.")
    r2_results = client.wait_for_results(task_id=task2.get("id"))
    if not r2_results:
        raise UserInputError("No round-2 results received.")

    n_suppressed_cells = 0
    pooled_counts: Dict[str, List[int]] = {}
    for node in r2_results:
        node = node or {}
        n_suppressed_cells += len(node.get("suppressed", []))
        for col, counts in node.get("counts", {}).items():
            if col not in reporting_edges or len(counts) != histogram_bins:
                continue
            current = pooled_counts.setdefault(col, [0] * histogram_bins)
            for i, c in enumerate(counts):
                current[i] += int(c)

    # ── Derive equal-count rank bins from the pooled histogram ───────────────
    # Each rank bin must hold at least `min_cell_count` records per contributing
    # node, which is also what keeps the bins non-disclosive.
    rank_edges: Dict[str, List[float]] = {}
    rank_mid_ranks: Dict[str, List[float]] = {}
    n_rank_bins: Dict[str, int] = {}
    for col in histogram_columns:
        totals = pooled_counts.get(col)
        if not totals or sum(totals) < 3:
            excluded[col] = "no records survived the node-side checks"
            continue

        n_total = sum(totals)
        minimum_per_bin = max(
            min_cell_count * contributors.get(col, 1),
            n_total // max(max_rank_bins, 1),
            1,
        )
        edges, counts = coarsen_histogram(
            reporting_edges[col], totals, minimum_per_bin
        )
        if len(counts) < 2:
            excluded[col] = "too few distinct values to rank"
            continue

        rank_edges[col] = edges
        rank_mid_ranks[col] = mid_ranks(counts)
        n_rank_bins[col] = len(counts)

    if not rank_edges:
        raise UserInputError(f"No column could be tested. Reasons: {excluded}")

    # ── Round 3: one rank sum per group, no distributions ────────────────────
    rank_columns = sorted(rank_edges)
    task3 = client.task.create(
        input_={
            "method": "partial_rank_sums",
            "kwargs": {
                "group_col": group_col,
                "columns": rank_columns,
                "edges_per_column": rank_edges,
                "mid_ranks_per_column": rank_mid_ranks,
            },
        },
        organizations=organizations_to_include,
        name="BH round 3: rank sums",
        description="Compute one rank sum and record count per group against the global mid-ranks.",
    )
    info("Waiting for round-3 results.")
    r3_results = client.wait_for_results(task_id=task3.get("id"))
    if not r3_results:
        raise UserInputError("No round-3 results received.")

    info("Computing per-column statistics from rank sums.")
    per_column: Dict[str, Dict] = {}
    raw_p_values: Dict[str, float] = {}
    for col in rank_columns:
        group_sums: Dict[str, float] = {}
        group_sq_sums: Dict[str, float] = {}
        group_ns: Dict[str, int] = {}
        for node in r3_results:
            for group, stats in (node or {}).get(col, {}).items():
                group_sums[group] = (
                    group_sums.get(group, 0.0) + float(stats["rank_sum"])
                )
                group_sq_sums[group] = (
                    group_sq_sums.get(group, 0.0) + float(stats["rank_sq_sum"])
                )
                group_ns[group] = group_ns.get(group, 0) + int(stats["n"])

        if not group_ns:
            excluded[col] = "no group survived the node-side minimum size checks"
            continue

        result = run_test(test, group_sums, group_sq_sums, group_ns)
        if "error" in result:
            excluded[col] = str(result["error"])
            continue

        result["n_rank_bins"] = n_rank_bins[col]
        per_column[col] = result
        raw_p_values[col] = float(result["p_value"])

    if not raw_p_values:
        raise UserInputError(f"No column could be tested. Reasons: {excluded}")

    # ── Benjamini-Hochberg across the family of columns ──────────────────────
    info(f"Applying Benjamini-Hochberg to {len(raw_p_values)} p-values.")
    correction = benjamini_hochberg(raw_p_values, alpha=alpha)
    for col, corrected in correction["results"].items():
        per_column[col].update(
            {
                "adjusted_p_value": corrected["adjusted_p_value"],
                "reject": corrected["reject"],
                "rank": corrected["rank"],
                "critical_value": corrected["critical_value"],
            }
        )

    info("Federated Benjamini-Hochberg finished.")
    return {
        "test": test,
        "alpha": alpha,
        "n_hypotheses": correction["n_hypotheses"],
        "n_tested": correction["n_tested"],
        "n_rejected": correction["n_rejected"],
        "columns": per_column,
        "excluded": excluded,
        "privacy": {
            "raw_values_shared": False,
            "node_output": (
                "round 1: n/sum/sum_sq per column; "
                "round 2: group-blind record counts per bin; "
                "round 3: rank-score sum, sum of squares and count per group"
            ),
            "minimum_cell_count": min_cell_count,
            "n_rank_bins_per_column": n_rank_bins,
            "n_suppressed_cells": n_suppressed_cells,
        },
    }
