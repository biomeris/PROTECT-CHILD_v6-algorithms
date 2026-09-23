"""
Three-round federated C-statistic central orchestration.

Round 1: collect per-column moments (no values, no outcome labels).
Round 2: collect an outcome-blind histogram on a grid derived from those moments.
Round 3: collect rank-score sums per outcome group -> compute the C-statistic.

The C-statistic (concordance statistic) for a binary outcome is the probability
that a randomly chosen "event" record scores higher than a randomly chosen
"non-event" record, with ties counted as half a win — the AUC of the score as a
classifier for the outcome. It is mathematically a normalized Mann-Whitney U
statistic, so the protocol mirrors the one validated for the federated
Mann-Whitney test in ``v6-benjamini-hochberg``: nodes never transmit a value,
only counts, sums and sums of squares.
"""
from __future__ import annotations

from math import sqrt
from typing import Dict, List, Optional

from vantage6.algorithm.client import AlgorithmClient
from vantage6.algorithm.tools.decorators import algorithm_client
from vantage6.algorithm.tools.exceptions import UserInputError
from vantage6.algorithm.tools.util import get_env_var, info

from .binned import bin_edges, c_statistic_from_scores, coarsen_histogram, mid_ranks
from .globals import (
    C_STATISTIC_HISTOGRAM_BINS,
    C_STATISTIC_MAXIMUM_RANK_BINS,
    C_STATISTIC_MINIMUM_CELL_COUNT,
    C_STATISTIC_RANGE_SD,
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
def central(
    client: AlgorithmClient,
    organizations_to_include: List[int],
    outcome_col: str,
    columns: Optional[List[str]] = None,
    positive_label: Optional[object] = None,
    alpha: float = 0.05,
) -> Dict[str, Dict]:
    """
    Federated C-statistic (concordance statistic / AUC) for a binary outcome.

    Round 1 collects per-column moments so the central server can lay out a
    shared reporting grid. Round 2 collects an outcome-blind histogram on that
    grid, which the central server pools and coarsens into roughly equal-count
    rank bins. Round 3 collects the sum and sum of squares of the assigned rank
    scores per outcome group, and the C-statistic, its Hanley-McNeil standard
    error, confidence interval and p-value (against C = 0.5) follow from those
    alone.

    Parameters
    ----------
    organizations_to_include : list[int]
        IDs of the organizations that participate in the computation.
    outcome_col : str
        Binary column defining the event / non-event outcome.
    columns : list[str] | None
        Numeric score columns to evaluate, one C-statistic each. If ``None``,
        all numeric columns (other than ``outcome_col``) are used.
    positive_label
        The value of ``outcome_col`` that denotes the "event"/positive
        outcome. Required unless the column is boolean or coded ``{0, 1}``, in
        which case ``True``/``1`` is assumed. Getting this backwards silently
        flips every C-statistic to ``1 - C``, so set it explicitly for
        anything else.
    alpha : float
        Significance level for the confidence interval, e.g. ``0.05`` for a
        95% CI. Default ``0.05``.

    Returns
    -------
    dict
        Maps each score column to ``{"c_statistic", "standard_error",
        "ci_lower", "ci_upper", "z_score", "p_value", "n_event",
        "n_non_event", "n_total", "n_rank_bins"}``.
    """
    if not organizations_to_include:
        raise UserInputError("Provide at least one organization id.")
    if not 0.0 < alpha < 1.0:
        raise UserInputError(f"alpha must be in (0, 1), got {alpha}.")

    histogram_bins = get_env_var(
        "C_STATISTIC_HISTOGRAM_BINS", C_STATISTIC_HISTOGRAM_BINS, as_type="int"
    )
    max_rank_bins = get_env_var(
        "C_STATISTIC_MAXIMUM_RANK_BINS", C_STATISTIC_MAXIMUM_RANK_BINS, as_type="int"
    )
    min_cell_count = get_env_var(
        "C_STATISTIC_MINIMUM_CELL_COUNT", C_STATISTIC_MINIMUM_CELL_COUNT, as_type="int"
    )
    range_sd = _env_float("C_STATISTIC_RANGE_SD", C_STATISTIC_RANGE_SD)

    info(f"Federated C-statistic, outcome_col='{outcome_col}', alpha={alpha}.")

    # ── Round 1: per-column moments, no values and no outcome labels ─────────
    task1 = client.task.create(
        input_={
            "method": "partial_moments",
            "kwargs": {
                "outcome_col": outcome_col,
                "columns": columns,
                "positive_label": positive_label,
            },
        },
        organizations=organizations_to_include,
        name="C-stat round 1: column moments",
        description="Collect per-column n, sum and sum of squares to lay out a shared reporting grid.",
    )
    info("Waiting for round-1 results.")
    r1_results = client.wait_for_results(task_id=task1.get("id"))
    if not r1_results:
        raise UserInputError("No round-1 results received.")

    all_columns = sorted(set().union(*((r or {}).keys() for r in r1_results)))
    if not all_columns:
        raise UserInputError(
            "No column survived the node-side minimum size checks. Check "
            "outcome_col, positive_label, and the minimum-record thresholds."
        )

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
        raise UserInputError(f"No column could be evaluated. Reasons: {excluded}")

    # ── Round 2: outcome-blind histogram on the reporting grid ───────────────
    histogram_columns = sorted(reporting_edges)
    task2 = client.task.create(
        input_={
            "method": "partial_histogram",
            "kwargs": {
                "outcome_col": outcome_col,
                "columns": histogram_columns,
                "edges_per_column": reporting_edges,
                "positive_label": positive_label,
            },
        },
        organizations=organizations_to_include,
        name="C-stat round 2: pooled histogram",
        description="Collect per-column record counts per bin, pooled across outcome groups.",
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
        raise UserInputError(f"No column could be evaluated. Reasons: {excluded}")

    # ── Round 3: rank-score sums per outcome group, no distributions ─────────
    rank_columns = sorted(rank_edges)
    task3 = client.task.create(
        input_={
            "method": "partial_rank_scores",
            "kwargs": {
                "outcome_col": outcome_col,
                "columns": rank_columns,
                "edges_per_column": rank_edges,
                "mid_ranks_per_column": rank_mid_ranks,
                "positive_label": positive_label,
            },
        },
        organizations=organizations_to_include,
        name="C-stat round 3: rank-score sums",
        description="Compute rank-score sum, sum of squares and count per outcome group.",
    )
    info("Waiting for round-3 results.")
    r3_results = client.wait_for_results(task_id=task3.get("id"))
    if not r3_results:
        raise UserInputError("No round-3 results received.")

    info("Computing C-statistics from rank-score sums.")
    out: Dict[str, Dict] = {}
    for col in rank_columns:
        totals: Dict[str, Dict[str, float]] = {"event": {}, "non_event": {}}
        for node in r3_results:
            for group in ("event", "non_event"):
                stats = (node or {}).get(col, {}).get(group)
                if not stats:
                    continue
                bucket = totals[group]
                bucket["rank_sum"] = bucket.get("rank_sum", 0.0) + float(
                    stats["rank_sum"]
                )
                bucket["rank_sq_sum"] = bucket.get("rank_sq_sum", 0.0) + float(
                    stats["rank_sq_sum"]
                )
                bucket["n"] = bucket.get("n", 0) + int(stats["n"])

        if not totals["event"] or not totals["non_event"]:
            excluded[col] = (
                "no node reported both outcome groups above the minimum size"
            )
            continue

        result = c_statistic_from_scores(
            totals["event"]["rank_sum"],
            totals["event"]["rank_sq_sum"],
            totals["event"]["n"],
            totals["non_event"]["rank_sum"],
            totals["non_event"]["rank_sq_sum"],
            totals["non_event"]["n"],
            alpha=alpha,
        )
        if "error" in result:
            excluded[col] = str(result["error"])
            continue

        result["n_rank_bins"] = n_rank_bins[col]
        out[col] = result

    if not out:
        raise UserInputError(f"No column could be evaluated. Reasons: {excluded}")

    if excluded:
        info(f"Columns excluded: {excluded}")
    info("Federated C-statistic finished.")
    return out
