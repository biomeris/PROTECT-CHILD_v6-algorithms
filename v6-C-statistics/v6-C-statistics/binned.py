"""
The C-statistic (concordance statistic / AUC) computed without any node
transmitting a value.

The C-statistic for a binary outcome is the probability that a randomly chosen
"event" record scores higher than a randomly chosen "non-event" record, with
ties counted as half a win. That is exactly a normalized Mann-Whitney U
statistic, ``C = U / (n_event * n_non_event)``, so the ranking machinery here
mirrors the one validated for the federated Mann-Whitney test in
``v6-benjamini-hochberg``:

1. Nodes report record counts on a shared equal-width grid, blind to outcome.
2. The central server pools those counts and merges adjacent bins into
   roughly equal-count *rank bins*, each large enough to be non-disclosive.
3. Every record in a rank bin is assigned that bin's mid-rank as its *score*.
   Nodes return the sum and the sum of squares of those scores for the event
   and non-event groups — four numbers total — and C follows from those.

The standard error uses the Hanley & McNeil (1982) approximation, which needs
only C and the two group sizes — no pairwise comparison, so it adds nothing to
what already crosses the node boundary.

Pure arithmetic: no vantage6 imports, no data access.
"""
from __future__ import annotations

from bisect import bisect_right
from math import sqrt
from typing import Dict, List, Optional, Sequence, Tuple

from scipy.stats import norm


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
    report, and that total is then placed at the run's count-weighted centre
    (not at the end of the run, which would systematically shift merged mass
    towards higher values and distort the pooled histogram once repeated across
    nodes). The result contains no cell in ``[1, minimum_cell_count)`` and sums
    to the same total as the input.
    """
    if minimum_cell_count <= 1:
        return list(counts)

    out = [0] * len(counts)
    run: List[Tuple[int, int]] = []
    carry = 0
    last_reported: Optional[int] = None

    def centre(run: List[Tuple[int, int]], carry: int) -> int:
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

    Bins end up narrow where the data is dense and wide where it is sparse,
    which is what keeps the ranking accurate — a fixed equal-width grid wastes
    most of its bins on empty tails. It also guarantees every rank bin is large
    enough to be non-disclosive.

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


def c_statistic_from_scores(
    event_score_sum: float,
    event_score_sq_sum: float,
    event_n: int,
    non_event_score_sum: float,
    non_event_score_sq_sum: float,
    non_event_n: int,
    alpha: float = 0.05,
) -> Dict[str, object]:
    """
    C-statistic (AUC-equivalent) from rank-score sums of the two outcome groups.

    Uses the general-scores form of the Mann-Whitney U statistic — it only
    assumes the scores are a monotone function of the underlying values, not
    that they are a perfect permutation of ``1..N``. That matters here because
    the binning protocol produces scores that drift slightly off that ideal;
    see the equivalent note in ``v6-benjamini-hochberg`` for why the classical
    rank-sum formula is unsafe under that drift.

    The standard error follows Hanley & McNeil (1982): a closed-form
    approximation derived under an assumed score distribution, needing only C
    and the two group sizes. It is standard in medical statistics software and
    is what makes the confidence interval and p-value computable from
    aggregates alone, without ever comparing individual pairs of records.

    Parameters
    ----------
    event_score_sum, event_score_sq_sum, event_n
        Sum, sum of squares, and count of the rank scores assigned to records
        with the outcome (the "event"/positive group).
    non_event_score_sum, non_event_score_sq_sum, non_event_n
        The same, for records without the outcome.
    alpha
        Confidence level for the interval, e.g. ``0.05`` for a 95% CI.

    Returns
    -------
    dict
        ``{"c_statistic", "standard_error", "ci_lower", "ci_upper", "z_score",
        "p_value", "n_event", "n_non_event", "n_total"}``, or ``{"error": ...}``
        if the statistic cannot be computed.
    """
    n1, n0 = int(event_n), int(non_event_n)
    if n1 < 1 or n0 < 1:
        return {"error": "both outcome groups must have at least 1 record"}
    n_total = n1 + n0

    total_sum = event_score_sum + non_event_score_sum
    total_sq_sum = event_score_sq_sum + non_event_score_sq_sum
    mean_score = total_sum / n_total
    sum_squares = total_sq_sum - total_sum * total_sum / n_total
    if sum_squares <= 0:
        return {"error": "all observations are tied"}

    deviation = event_score_sum - n1 * mean_score
    u1 = deviation + n1 * n0 / 2.0
    c = u1 / (n1 * n0)
    # Clip away float noise; C is a probability and must stay in [0, 1].
    c = min(max(c, 0.0), 1.0)

    q1 = c / (2.0 - c) if c < 2.0 else 1.0
    q0 = 2.0 * c ** 2 / (1.0 + c)
    variance = (
        c * (1.0 - c) + (n1 - 1) * (q1 - c ** 2) + (n0 - 1) * (q0 - c ** 2)
    ) / (n1 * n0)
    se = sqrt(max(variance, 0.0))

    if se > 0:
        z = (c - 0.5) / se
        p_value = float(2 * norm.sf(abs(z)))
        z_crit = float(norm.ppf(1 - alpha / 2))
        ci_lower = min(max(c - z_crit * se, 0.0), 1.0)
        ci_upper = min(max(c + z_crit * se, 0.0), 1.0)
    else:
        z, p_value = float("nan"), float("nan")
        ci_lower = ci_upper = c

    return {
        "c_statistic": float(c),
        "standard_error": float(se),
        "ci_lower": float(ci_lower),
        "ci_upper": float(ci_upper),
        "z_score": float(z),
        "p_value": p_value,
        "n_event": n1,
        "n_non_event": n0,
        "n_total": n_total,
    }
