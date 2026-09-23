"""
Rank-based test statistics computed without any node transmitting a value.

A rank test needs a *global* ordering of observations, which is normally
obtained by sending every value to the central server. Here the ordering is
established from a histogram instead:

1. Nodes report record counts on a shared equal-width grid, blind to groups.
2. The central server pools those counts and merges adjacent bins into
   roughly equal-count *rank bins*, each large enough to be non-disclosive.
3. Every record in a rank bin is assigned that bin's mid-rank as its *score*.
   Nodes return the sum and the sum of squares of those scores per group, and
   the statistic follows from that handful of numbers.

Records sharing a rank bin are treated as tied. Both statistics below are
written in their general-scores form, which handles ties intrinsically and, more
importantly, does not assume the scores are a perfect permutation of ``1..N`` —
see :func:`kruskal_wallis_from_scores` for why that matters here.

Pure arithmetic: no vantage6 imports, no data access.
"""
from __future__ import annotations

from bisect import bisect_right
from math import sqrt
from typing import Dict, List, Optional, Sequence, Tuple

from scipy.stats import chi2, norm

KRUSKAL_WALLIS = "kruskal-wallis"
MANN_WHITNEY = "mann-whitney"
SUPPORTED_TESTS = (KRUSKAL_WALLIS, MANN_WHITNEY)


def bin_edges(mean: float, sd: float, n_bins: int, range_sd: float) -> List[float]:
    """
    Evenly spaced edges spanning ``mean +/- range_sd * sd``.

    Returns ``n_bins + 1`` edges. Values outside the span are clipped into the
    outermost bins by :func:`assign_bins`, so no record is ever lost.
    """
    if n_bins < 1:
        raise ValueError(f"n_bins must be >= 1, got {n_bins}.")
    if sd <= 0:
        raise ValueError("sd must be positive to build a binning grid.")
    lo = mean - range_sd * sd
    hi = mean + range_sd * sd
    width = (hi - lo) / n_bins
    return [lo + i * width for i in range(n_bins + 1)]


def assign_bins(values: Sequence[float], edges: Sequence[float]) -> List[int]:
    """Bin index per value, clipped into ``[0, len(edges) - 2]``."""
    n_bins = len(edges) - 1
    return [
        min(max(bisect_right(edges, v) - 1, 0), n_bins - 1) for v in values
    ]


def merge_small_cells(counts: Sequence[int], minimum_cell_count: int) -> List[int]:
    """
    Remove disclosive histogram cells while preserving the total record count.

    Adjacent bins are accumulated until the running total is large enough to
    report, and that total is then placed at the run's count-weighted centre.
    Reporting at the centre rather than at the end of the run matters: an
    end-of-run rule shifts every merged group of records towards higher values,
    and those shifts accumulate across nodes into a badly distorted pooled
    histogram. Placing the mass at its centroid preserves the run's mean
    position, so the pooled shape survives.

    The result contains no cell in ``[1, minimum_cell_count)`` and sums to the
    same total as the input.

    This runs on the node's *group-blind* histogram only. It still perturbs that
    histogram, which is why the central server re-derives its rank bins from the
    pooled counts rather than trusting the reporting grid.
    """
    if minimum_cell_count <= 1:
        return list(counts)

    out = [0] * len(counts)
    run: List[tuple] = []
    carry = 0
    last_reported: Optional[int] = None

    def centre(run: List[tuple], carry: int) -> int:
        return round(sum(i * c for i, c in run) / carry)

    for i, count in enumerate(counts):
        if count:
            run.append((i, count))
        carry += count
        if carry >= minimum_cell_count:
            last_reported = centre(run, carry)
            out[last_reported] += carry
            run, carry = [], 0

    if carry:
        if last_reported is None:
            # Whole vector is below the threshold; the caller suppresses it.
            out[len(counts) - 1] = carry
        else:
            out[last_reported] += carry
    return out


def coarsen_histogram(
    edges: Sequence[float],
    totals: Sequence[int],
    minimum_count: int,
) -> Tuple[List[float], List[int]]:
    """
    Merge adjacent bins of the pooled histogram into roughly equal-count bins.

    Walking the grid and closing a bin whenever it holds at least
    ``minimum_count`` records gives bins that are narrow where the data is dense
    and wide where it is sparse. That is what makes the ranking accurate: a
    fixed equal-width grid wastes most of its bins on empty tails, while these
    bins each carry comparable weight. It also guarantees every rank bin is
    large enough to be non-disclosive.

    Returns the coarse edges and the record count in each coarse bin.
    """
    minimum_count = max(minimum_count, 1)
    coarse_edges = [edges[0]]
    coarse_counts: List[int] = []
    carry = 0
    for i, count in enumerate(totals):
        carry += count
        if carry >= minimum_count:
            coarse_edges.append(edges[i + 1])
            coarse_counts.append(carry)
            carry = 0
    if carry and coarse_counts:
        coarse_counts[-1] += carry
    elif not coarse_counts:
        # Nothing reached the target — report the whole grid as a single bin.
        coarse_counts.append(carry)
        coarse_edges.append(edges[-1])
    # The final bin always extends to the top of the grid.
    coarse_edges[-1] = edges[-1]
    return coarse_edges, coarse_counts


def mid_ranks(bin_totals: Sequence[int]) -> List[float]:
    """
    1-indexed mid-rank of each bin, given the global count in every bin.

    All records in a bin share the average of the ranks that bin occupies.
    """
    ranks = []
    cumulative = 0
    for total in bin_totals:
        ranks.append(cumulative + (total + 1) / 2.0)
        cumulative += total
    return ranks


def _score_totals(
    group_score_sums: Dict[str, float],
    group_score_sq_sums: Dict[str, float],
    group_ns: Dict[str, int],
    groups: Sequence[str],
) -> Tuple[int, float, float]:
    """Global record count, mean score, and total sum of squares about the mean."""
    n_total = sum(group_ns[g] for g in groups)
    total = sum(group_score_sums[g] for g in groups)
    total_sq = sum(group_score_sq_sums[g] for g in groups)
    mean_score = total / n_total
    # Sum of (score - mean)^2 over every record, the dispersion of the scores
    # that were actually assigned.
    sum_squares = total_sq - total * total / n_total
    return n_total, mean_score, sum_squares


def kruskal_wallis_from_scores(
    group_score_sums: Dict[str, float],
    group_score_sq_sums: Dict[str, float],
    group_ns: Dict[str, int],
) -> Dict[str, object]:
    """
    Kruskal-Wallis in its general-scores form.

    ``H = (N - 1) * between-group sum of squares / total sum of squares``

    With exact mid-ranks this is algebraically identical to the familiar
    ``12/(N(N+1)) * sum(R^2/n) - 3(N+1)`` divided by the tie correction — the
    tie handling is built in. The reason for using this form here is that it
    only assumes the scores are a monotone function of the values, not that they
    are a perfect permutation of ``1..N``. The scores produced by the binning
    protocol are slightly off that ideal, and the classical formula — a
    difference of two large, nearly equal terms — turns a fraction of a percent
    of drift into a wildly wrong statistic.
    """
    groups = sorted(g for g, n in group_ns.items() if n > 0)
    if len(groups) < 2:
        return {"error": "fewer than 2 non-empty groups"}

    n_total, mean_score, sum_squares = _score_totals(
        group_score_sums, group_score_sq_sums, group_ns, groups
    )
    if n_total < 3:
        return {"error": "too few records"}
    if sum_squares <= 0:
        return {"error": "all observations are tied"}

    between = sum(
        group_ns[g] * (group_score_sums[g] / group_ns[g] - mean_score) ** 2
        for g in groups
    )
    h = (n_total - 1) * between / sum_squares

    df = len(groups) - 1
    return {
        "statistic_name": "h_statistic",
        "statistic": float(h),
        "p_value": float(chi2.sf(h, df=df)),
        "degrees_of_freedom": df,
        "groups": groups,
        "n_per_group": {g: int(group_ns[g]) for g in groups},
        "n_total": int(n_total),
    }


def mann_whitney_from_scores(
    group_score_sums: Dict[str, float],
    group_score_sq_sums: Dict[str, float],
    group_ns: Dict[str, int],
) -> Dict[str, object]:
    """
    Mann-Whitney U as a standardised linear rank statistic (two-sided).

    The variance uses the dispersion of the scores that were actually assigned,
    ``n_a n_b * SS / (N (N - 1))``, which reduces to the textbook tie-corrected
    variance when the scores are exact mid-ranks — again with tie handling built
    in, and again without assuming the scores are a perfect rank permutation.
    """
    groups = sorted(g for g, n in group_ns.items() if n > 0)
    if len(groups) != 2:
        return {"error": f"exactly 2 groups required, found {groups}"}

    group_a, group_b = groups
    n_a, n_b = group_ns[group_a], group_ns[group_b]
    n_total, mean_score, sum_squares = _score_totals(
        group_score_sums, group_score_sq_sums, group_ns, groups
    )
    if n_total < 3:
        return {"error": "too few records"}
    if sum_squares <= 0:
        return {"error": "zero variance; all observations are tied"}

    # Deviation of group A's score sum from what an equal-distribution would
    # give. Shifting by n_a n_b / 2 puts it on the familiar U scale, where the
    # null expectation is n_a n_b / 2.
    deviation = group_score_sums[group_a] - n_a * mean_score
    u_a = deviation + n_a * n_b / 2.0

    variance = n_a * n_b * sum_squares / (n_total * (n_total - 1))
    if variance <= 0:
        return {"error": "zero variance; all observations are tied"}

    z = deviation / sqrt(variance)
    return {
        "statistic_name": "u_statistic",
        "statistic": float(u_a),
        "p_value": float(2 * norm.sf(abs(z))),
        "z_score": float(z),
        "groups": groups,
        "n_per_group": {group_a: int(n_a), group_b: int(n_b)},
        "n_total": int(n_total),
    }


def run_test(
    test: str,
    group_score_sums: Dict[str, float],
    group_score_sq_sums: Dict[str, float],
    group_ns: Dict[str, int],
) -> Dict[str, object]:
    """Dispatch to the requested per-column test."""
    if test == KRUSKAL_WALLIS:
        return kruskal_wallis_from_scores(
            group_score_sums, group_score_sq_sums, group_ns
        )
    if test == MANN_WHITNEY:
        return mann_whitney_from_scores(
            group_score_sums, group_score_sq_sums, group_ns
        )
    raise ValueError(f"Unknown test '{test}'. Supported: {list(SUPPORTED_TESTS)}.")
