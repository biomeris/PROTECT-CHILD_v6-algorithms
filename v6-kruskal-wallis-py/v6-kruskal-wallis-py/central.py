"""
Three-round federated Kruskal-Wallis central orchestration.

Round 1: collect per-column moments (no values, no group labels).
Round 2: collect a group-blind histogram on a grid derived from those moments.
Round 3: collect rank-score sums per group -> compute the H statistic.

Earlier versions of this algorithm had round 1 share every column's sorted
values with the central server (group labels stripped, but the values
themselves fully intact) — a real disclosure of raw local data. This version
never transmits a value: only counts, sums and sums of squares cross the node
boundary, at every round.
"""
from __future__ import annotations

from math import sqrt
from typing import Dict, List, Optional

from vantage6.algorithm.client import AlgorithmClient
from vantage6.algorithm.tools.decorators import algorithm_client
from vantage6.algorithm.tools.exceptions import UserInputError
from vantage6.algorithm.tools.util import get_env_var, info, warn

from .binned import bin_edges, coarsen_histogram, kruskal_wallis_from_scores, mid_ranks
from .globals import (
    KRUSKAL_WALLIS_HISTOGRAM_BINS,
    KRUSKAL_WALLIS_MAXIMUM_RANK_BINS,
    KRUSKAL_WALLIS_MINIMUM_CELL_COUNT,
    KRUSKAL_WALLIS_RANGE_SD,
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
    for r in results:
        stats = (r or {}).get(column)
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
    group_col: str,
    columns: Optional[List[str]] = None,
) -> Dict[str, Dict]:
    """
    Central federated Kruskal-Wallis test (three-round protocol).

    Round 1 collects per-column moments so the central server can lay out a
    shared reporting grid. Round 2 collects a group-blind histogram on that
    grid, which the central server pools and coarsens into roughly equal-count
    rank bins. Round 3 collects the sum and sum of squares of the assigned rank
    scores per group, and the H statistic follows from those alone. No node
    ever transmits an individual value.

    Parameters
    ----------
    organizations_to_include : list[int]
        IDs of the organizations that participate in the computation.
    group_col : str
        Column that defines the groups.
    columns : list[str] | None
        Numeric columns to test. If None, all numeric columns are used.

    Returns
    -------
    dict
        Maps each column to ``{"h_statistic", "p_value", "degrees_of_freedom",
        "groups", "n_per_group"}``.
    """
    if not organizations_to_include:
        raise UserInputError("Provide at least one organization id.")

    histogram_bins = get_env_var(
        "KRUSKAL_WALLIS_HISTOGRAM_BINS", KRUSKAL_WALLIS_HISTOGRAM_BINS, as_type="int"
    )
    max_rank_bins = get_env_var(
        "KRUSKAL_WALLIS_MAXIMUM_RANK_BINS", KRUSKAL_WALLIS_MAXIMUM_RANK_BINS, as_type="int"
    )
    min_cell_count = get_env_var(
        "KRUSKAL_WALLIS_MINIMUM_CELL_COUNT", KRUSKAL_WALLIS_MINIMUM_CELL_COUNT, as_type="int"
    )
    range_sd = _env_float("KRUSKAL_WALLIS_RANGE_SD", KRUSKAL_WALLIS_RANGE_SD)

    info(f"Federated KW test, group_col='{group_col}'.")

    # ── Round 1: per-column moments, no values and no group labels ───────────
    task1 = client.task.create(
        input_={
            "method": "partial_moments",
            "kwargs": {"group_col": group_col, "columns": columns},
        },
        organizations=organizations_to_include,
        name="KW round 1: column moments",
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
        name="KW round 2: pooled histogram",
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
    # node — the same disclosure floor as the round-2 histogram. At small total
    # sample sizes this floor forces very few, very wide rank bins, which costs
    # accuracy (see the warning below and the "Accuracy at small sample sizes"
    # section in the README): that is an inherent privacy/accuracy trade-off of
    # binning the data instead of centralizing it, not a bug.
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
        edges, counts = coarsen_histogram(reporting_edges[col], totals, minimum_per_bin)
        if len(counts) < 2:
            excluded[col] = "too few distinct values to rank"
            continue

        rank_edges[col] = edges
        rank_mid_ranks[col] = mid_ranks(counts)
        n_rank_bins[col] = len(counts)
        if len(counts) < 10:
            warn(
                f"Column '{col}' resolved to only {len(counts)} rank bins "
                f"(total eligible records: {n_total}). With this few bins the "
                "H statistic can deviate substantially from an exact "
                "(non-federated) Kruskal-Wallis test — treat the result as "
                "indicative rather than precise. This is the expected cost of "
                "the minimum-cell-count privacy floor at small sample sizes, "
                "not an error."
            )

    if not rank_edges:
        raise UserInputError(f"No column could be tested. Reasons: {excluded}")

    # ── Round 3: rank-score sums per group, no distributions ─────────────────
    rank_columns = sorted(rank_edges)
    task3 = client.task.create(
        input_={
            "method": "partial_rank_scores",
            "kwargs": {
                "group_col": group_col,
                "columns": rank_columns,
                "edges_per_column": rank_edges,
                "mid_ranks_per_column": rank_mid_ranks,
            },
        },
        organizations=organizations_to_include,
        name="KW round 3: rank-score sums",
        description="Compute rank-score sum, sum of squares and count per group.",
    )
    info("Waiting for round-3 results.")
    r3_results = client.wait_for_results(task_id=task3.get("id"))
    if not r3_results:
        raise UserInputError("No round-3 results received.")

    info("Computing H statistics from rank-score sums.")
    out: Dict[str, Dict] = {}
    for col in rank_columns:
        group_sums: Dict[str, float] = {}
        group_sq_sums: Dict[str, float] = {}
        group_ns: Dict[str, int] = {}
        for node in r3_results:
            for group, stats in (node or {}).get(col, {}).items():
                group_sums[group] = group_sums.get(group, 0.0) + float(stats["rank_sum"])
                group_sq_sums[group] = group_sq_sums.get(group, 0.0) + float(
                    stats["rank_sq_sum"]
                )
                group_ns[group] = group_ns.get(group, 0) + int(stats["n"])

        if len(group_ns) < 2:
            info(f"Skipping '{col}': fewer than 2 groups found.")
            continue

        result = kruskal_wallis_from_scores(group_sums, group_sq_sums, group_ns)
        if "error" in result:
            info(f"Skipping '{col}': {result['error']}.")
            continue

        result.pop("n_total", None)
        result["n_rank_bins"] = n_rank_bins[col]
        out[col] = result

    info("Federated KW test finished.")
    return out
