"""
Pure math for the federated Dunn's test, free of vantage6 decorators so it can be
unit tested directly (see ``test/test_dunn.py``).

Global ranks are established without any node sharing an individual value,
following the three-round protocol of the GT federated Kruskal-Wallis algorithm
(``v6-kruskal-wallis-py``, branch ``kruskal-wallis-yishan``), whose binning
functions are ported here:

Round 1  node    ``local_moments``      -> n, sum, sum of squares (no groups)
         central ``reporting_grid``     -> equal-width grid around mean +/- 4 sd
Round 2  node    ``local_histogram``    -> group-blind counts per grid bin, with
                                           cells below the minimum merged away
         central ``rank_bins``          -> roughly equal-count rank bins and the
                                           global mid-rank of each
Round 3  node    ``local_rank_scores``  -> per group: n, sum and sum of squares
                                           of the records' rank-bin mid-ranks
         central ``kruskal_wallis`` / ``dunn``

Records sharing a rank bin are treated as tied. Both tests are computed from
per-group score sums only. With true mid-ranks as scores they reduce exactly to
``scipy.stats.kruskal`` and ``scikit_posthocs.posthoc_dunn`` (tie-corrected):
the pooled rank variance ``SST / (N - 1)`` equals
``N(N + 1)/12 - sum(t^3 - t) / (12(N - 1))``.
"""

import itertools
import numbers
from bisect import bisect_right

import numpy as np
import pandas as pd
from scipy import stats

from vantage6.algorithm.tools.exceptions import InputError

N_THRESHOLD_FLAG = "N-Threshold not met"
P_ADJUST_METHODS = ("bonferroni", "holm", "fdr_bh", "none")


# --------------------------------------------------------------------------- #
# Node side
# --------------------------------------------------------------------------- #
def normalize_label(value) -> str:
    """Stringify a group label consistently across nodes (1 and 1.0 -> "1")."""
    if isinstance(value, numbers.Real) and not isinstance(value, bool):
        if float(value).is_integer():
            return str(int(value))
    return str(value)


def eligible_records(
    df: pd.DataFrame, group_column: str, column: str, minimum_group_size: int
) -> tuple[np.ndarray, pd.Series, list[str]]:
    """Values and group labels of the records analysed at this node.

    Rows with a missing group or value are dropped, and so are groups with fewer
    than ``minimum_group_size`` records. Every round calls this, so all three
    analyse exactly the same records.
    """
    for col in (group_column, column):
        if col not in df.columns:
            raise InputError(f"Column '{col}' not found in DataFrame.")
    if pd.api.types.is_bool_dtype(df[column]) or not pd.api.types.is_numeric_dtype(
        df[column]
    ):
        raise InputError(
            f"Column '{column}' must be numeric, got dtype '{df[column].dtype}'."
        )

    data = df[[group_column, column]].dropna()
    labels = data[group_column].map(normalize_label)
    sizes = labels.value_counts()
    suppressed = sorted(sizes[sizes < minimum_group_size].index)
    keep = ~labels.isin(suppressed)
    return data.loc[keep, column].to_numpy(dtype=float), labels[keep], suppressed


def local_moments(
    df: pd.DataFrame,
    group_column: str,
    column: str,
    minimum_records: int,
    minimum_group_size: int,
) -> dict:
    """Round 1: count, sum and sum of squares of the eligible values."""
    values, _, suppressed = eligible_records(
        df, group_column, column, minimum_group_size
    )
    if len(values) <= minimum_records:
        return {N_THRESHOLD_FLAG: True}
    return {
        "n": int(len(values)),
        "sum": float(values.sum()),
        "sum_sq": float((values**2).sum()),
        "suppressed_groups": suppressed,
    }


def assign_bins(values, edges) -> np.ndarray:
    """Bin index per value, clipped into ``[0, len(edges) - 2]``."""
    n_bins = len(edges) - 1
    idx = np.array([bisect_right(edges, v) - 1 for v in values], dtype=int)
    return np.clip(idx, 0, n_bins - 1)


def merge_small_cells(counts, minimum_cell_count: int) -> list[int]:
    """Remove disclosive histogram cells while preserving the total count.

    Adjacent bins are accumulated until the running total reaches
    ``minimum_cell_count``; that total is placed at the run's count-weighted
    centre. The result has no cell in ``[1, minimum_cell_count)``, except when
    the whole vector is below the threshold.
    """
    if minimum_cell_count <= 1:
        return [int(c) for c in counts]

    out = [0] * len(counts)
    run: list[tuple[int, int]] = []
    carry = 0
    last_reported = None
    for i, count in enumerate(counts):
        if count:
            run.append((i, int(count)))
        carry += int(count)
        if carry >= minimum_cell_count:
            last_reported = round(sum(j * c for j, c in run) / carry)
            out[last_reported] += carry
            run, carry = [], 0
    if carry:
        out[len(counts) - 1 if last_reported is None else last_reported] += carry
    return out


def local_histogram(
    df: pd.DataFrame,
    group_column: str,
    column: str,
    edges: list[float],
    minimum_group_size: int,
    minimum_cell_count: int,
) -> list[int]:
    """Round 2: group-blind counts per bin of the shared grid."""
    values, _, _ = eligible_records(df, group_column, column, minimum_group_size)
    raw = np.bincount(assign_bins(values, edges), minlength=len(edges) - 1)
    return merge_small_cells(raw.tolist(), minimum_cell_count)


def local_rank_scores(
    df: pd.DataFrame,
    group_column: str,
    column: str,
    edges: list[float],
    mid_ranks: list[float],
    minimum_group_size: int,
) -> dict:
    """Round 3: per group, the count, sum and sum of squares of the rank scores."""
    values, labels, _ = eligible_records(df, group_column, column, minimum_group_size)
    scores = np.asarray(mid_ranks, dtype=float)[assign_bins(values, edges)]
    out = {}
    for label in sorted(labels.unique()):
        s = scores[(labels == label).to_numpy()]
        out[label] = {
            "n": int(len(s)),
            "score_sum": float(s.sum()),
            "score_sq_sum": float((s**2).sum()),
        }
    return out


# --------------------------------------------------------------------------- #
# Central side: grid and rank bins
# --------------------------------------------------------------------------- #
def reporting_grid(moments: list[dict], n_bins: int, range_sd: float) -> list[float]:
    """Equal-width grid spanning the pooled mean +/- ``range_sd`` pooled sd."""
    n = sum(m["n"] for m in moments)
    total = sum(m["sum"] for m in moments)
    total_sq = sum(m["sum_sq"] for m in moments)
    mean = total / n
    sd = np.sqrt(max(total_sq / n - mean**2, 0.0))
    if sd <= 1e-12 * max(1.0, abs(mean)):
        raise InputError("All values are identical; there is nothing to rank.")
    lo, hi = mean - range_sd * sd, mean + range_sd * sd
    return np.linspace(lo, hi, n_bins + 1).tolist()


def coarsen_histogram(edges, totals, minimum_count: int) -> tuple[list, list]:
    """Merge adjacent bins of the pooled histogram into roughly equal-count bins.

    Returns the coarse edges and the record count in each coarse bin. The
    outer edges are kept, and values beyond them are clipped into the outer
    bins by ``assign_bins``.
    """
    minimum_count = max(minimum_count, 1)
    coarse_edges = [edges[0]]
    coarse_counts: list[int] = []
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
        coarse_counts.append(carry)
        coarse_edges.append(edges[-1])
    coarse_edges[-1] = edges[-1]
    return coarse_edges, coarse_counts


def mid_ranks(bin_totals) -> list[float]:
    """1-indexed mid-rank of each bin, given the global count in every bin."""
    ranks, cumulative = [], 0
    for total in bin_totals:
        ranks.append(cumulative + (total + 1) / 2.0)
        cumulative += total
    return ranks


def rank_bins(
    edges: list[float],
    histograms: list[list[int]],
    minimum_cell_count: int,
    maximum_rank_bins: int,
) -> tuple[list[float], list[float], int]:
    """Pool the node histograms and derive rank bins with their mid-ranks.

    Each rank bin holds at least ``minimum_cell_count`` records per contributing
    node and at least ``N / maximum_rank_bins`` records overall.
    """
    totals = np.sum(histograms, axis=0).astype(int).tolist()
    n_total = sum(totals)
    minimum_per_bin = max(
        minimum_cell_count * len(histograms), n_total // max(maximum_rank_bins, 1), 1
    )
    coarse_edges, coarse_counts = coarsen_histogram(edges, totals, minimum_per_bin)
    return coarse_edges, mid_ranks(coarse_counts), len(coarse_counts)


# --------------------------------------------------------------------------- #
# Central side: tests
# --------------------------------------------------------------------------- #
def combine_scores(results: list[dict]) -> dict[str, dict]:
    """Add up per-group score statistics across nodes."""
    pooled: dict[str, dict] = {}
    for r in results:
        for label, g in r.items():
            acc = pooled.setdefault(label, {"n": 0, "score_sum": 0.0, "score_sq_sum": 0.0})
            acc["n"] += g["n"]
            acc["score_sum"] += g["score_sum"]
            acc["score_sq_sum"] += g["score_sq_sum"]
    return dict(sorted(pooled.items()))


def score_variance(groups: dict[str, dict]) -> tuple[int, float, float]:
    """(N, grand mean score, pooled score variance SST / (N - 1))."""
    n = sum(g["n"] for g in groups.values())
    total = sum(g["score_sum"] for g in groups.values())
    total_sq = sum(g["score_sq_sum"] for g in groups.values())
    if n < 3:
        raise InputError(f"Too few records ({n}) to rank.")
    sst = total_sq - total**2 / n
    if sst <= 0:
        raise InputError("All observations are tied; there is nothing to compare.")
    return n, total / n, sst / (n - 1)


def kruskal_wallis(groups: dict[str, dict]) -> dict:
    """Kruskal-Wallis H in its general-scores form, H = (N - 1) SSB / SST."""
    if len(groups) < 2:
        raise InputError(f"At least 2 groups are needed, found {len(groups)}.")
    n, mean, variance = score_variance(groups)
    ssb = sum(
        g["n"] * (g["score_sum"] / g["n"] - mean) ** 2 for g in groups.values()
    )
    h = ssb / variance
    df = len(groups) - 1
    return {"h_statistic": float(h), "df": df, "p_value": float(stats.chi2.sf(h, df))}


def adjust_p_values(p_values, method: str) -> np.ndarray:
    """Bonferroni, Holm or Benjamini-Hochberg adjustment (as in statsmodels)."""
    p = np.asarray(p_values, dtype=float)
    m = len(p)
    if method == "none" or m == 0:
        return p.copy()
    if method == "bonferroni":
        return np.minimum(p * m, 1.0)
    order = np.argsort(p)
    if method == "holm":
        adjusted = np.maximum.accumulate((m - np.arange(m)) * p[order])
    elif method == "fdr_bh":
        adjusted = np.minimum.accumulate((m / np.arange(m, 0, -1)) * p[order[::-1]])[::-1]
    else:
        raise InputError(
            f"'p_adjust' must be one of {list(P_ADJUST_METHODS)}, got {method!r}."
        )
    out = np.empty(m)
    out[order] = np.minimum(adjusted, 1.0)
    return out


def dunn(groups: dict[str, dict], p_adjust: str, alpha: float) -> list[dict]:
    """Pairwise Dunn's z-tests on mean rank scores.

    ``z = (mean_i - mean_j) / sqrt(variance * (1/n_i + 1/n_j))`` with two-sided
    normal p-values, adjusted over all pairs.
    """
    _, _, variance = score_variance(groups)
    labels = list(groups)
    rows = []
    for a, b in itertools.combinations(labels, 2):
        ga, gb = groups[a], groups[b]
        diff = ga["score_sum"] / ga["n"] - gb["score_sum"] / gb["n"]
        z = diff / np.sqrt(variance * (1 / ga["n"] + 1 / gb["n"]))
        rows.append(
            {
                "group_1": a,
                "group_2": b,
                "mean_rank_difference": float(diff),
                "z": float(z),
                "p_value": float(2 * stats.norm.sf(abs(z))),
            }
        )
    adjusted = adjust_p_values([r["p_value"] for r in rows], p_adjust)
    for r, p in zip(rows, adjusted):
        r["p_adjusted"] = float(p)
        r["reject"] = bool(p <= alpha)
    return rows
