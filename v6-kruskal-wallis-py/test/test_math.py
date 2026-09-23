"""
Verify the statistical core of the algorithm without needing vantage6 or Docker.

`binned.py` deliberately has no vantage6 imports, so it can be checked directly
against scipy and against a naive reference implementation. Run as:

    python test_math.py

Requires only numpy and scipy. For the end-to-end federated run against the
vantage6 mock client, see `test.py`.
"""
import importlib.util
import sys
from pathlib import Path

import numpy as np
from scipy.stats import kruskal, rankdata

PKG = Path(__file__).parent.parent / "v6-kruskal-wallis-py"


def _load(name):
    """Import a module by path so the vantage6-dependent package is bypassed."""
    spec = importlib.util.spec_from_file_location(name, PKG / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


binned = _load("binned")

failures = []


def check(label, ok, detail=""):
    print(f"{'PASS' if ok else 'FAIL'}  {label}{('  ' + detail) if detail else ''}")
    if not ok:
        failures.append(label)


def scores_by_group(scores, groups):
    """Aggregate a score vector into the 3 numbers round 3 returns, per group."""
    sums, sq_sums, ns = {}, {}, {}
    for score, group in zip(scores, groups):
        group = str(group)
        sums[group] = sums.get(group, 0.0) + score
        sq_sums[group] = sq_sums.get(group, 0.0) + score * score
        ns[group] = ns.get(group, 0) + 1
    return sums, sq_sums, ns


def federated_pipeline(nodes, group_col, column, histogram_bins=200, min_cell=5, max_rank_bins=50):
    """
    Reproduce the three-round protocol in-process, node by node.

    Mirrors `partial_moments` -> `partial_histogram` -> `partial_rank_scores` and
    the central aggregation between them, including the fact that only moments,
    counts and rank-score sums ever cross a node boundary.
    """
    n_total = sum(len(node[column]) for node in nodes)
    total = sum(float(np.sum(node[column])) for node in nodes)
    total_sq = sum(float(np.sum(np.square(node[column]))) for node in nodes)
    mean = total / n_total
    sd = np.sqrt(max(total_sq / n_total - mean ** 2, 0.0))
    reporting_edges = binned.bin_edges(mean, sd, histogram_bins, 4.0)

    pooled = [0] * histogram_bins
    for node in nodes:
        raw = [0] * histogram_bins
        for idx in binned.assign_bins(list(node[column]), reporting_edges):
            raw[idx] += 1
        for i, count in enumerate(binned.merge_small_cells(raw, min_cell)):
            pooled[i] += count

    minimum_per_bin = max(min_cell * len(nodes), n_total // max_rank_bins, 1)
    rank_edges, rank_counts = binned.coarsen_histogram(reporting_edges, pooled, minimum_per_bin)
    ranks = binned.mid_ranks(rank_counts)

    sums, sq_sums, group_ns = {}, {}, {}
    for node in nodes:
        scores = [ranks[i] for i in binned.assign_bins(list(node[column]), rank_edges)]
        s, sq, n = scores_by_group(scores, node[group_col])
        for g in s:
            sums[g] = sums.get(g, 0.0) + s[g]
            sq_sums[g] = sq_sums.get(g, 0.0) + sq[g]
            group_ns[g] = group_ns.get(g, 0) + n[g]

    result = binned.kruskal_wallis_from_scores(sums, sq_sums, group_ns)
    result["n_rank_bins"] = len(rank_counts)
    return result


def split(values, groups, n_nodes, column, group_col="Group"):
    """Partition a dataset across nodes, as horizontally partitioned data."""
    chunks = np.array_split(np.arange(len(values)), n_nodes)
    return [{group_col: groups[idx], column: values[idx]} for idx in chunks]


# ── The general-scores H is exact when fed exact mid-ranks ──────────────────
# This licenses the whole approach: the formula itself is not an approximation,
# only the scores the binning protocol produces are.
rng = np.random.default_rng(3)
n = 600
groups = rng.choice(["A", "B", "C"], size=n)
values = rng.normal(0, 1, n) + (groups == "B") * 0.45 + (groups == "C") * 0.9

got = binned.kruskal_wallis_from_scores(*scores_by_group(rankdata(values), groups))
exact_h, _ = kruskal(*[values[groups == g] for g in ["A", "B", "C"]])
check(
    "general-scores H equals scipy.stats.kruskal exactly on true mid-ranks",
    abs(got["h_statistic"] - exact_h) < 1e-9,
    f"H={got['h_statistic']:.10f} vs {exact_h:.10f}",
)

tied_values = np.round(values, 0)
got = binned.kruskal_wallis_from_scores(*scores_by_group(rankdata(tied_values), groups))
exact_h, _ = kruskal(*[tied_values[groups == g] for g in ["A", "B", "C"]])
check(
    "general-scores H equals scipy under heavy ties (tie correction is intrinsic)",
    abs(got["h_statistic"] - exact_h) < 1e-9,
    f"H={got['h_statistic']:.10f} vs {exact_h:.10f}",
)

# ── Through the full three-round pipeline ────────────────────────────────────
nodes = split(values, groups, 3, "x")
exact_h, exact_p = kruskal(*[values[groups == g] for g in ["A", "B", "C"]])

got = federated_pipeline(nodes, "Group", "x")
check(
    "three-round Kruskal-Wallis H is within 5% of scipy.stats.kruskal",
    abs(got["h_statistic"] - exact_h) / exact_h < 0.05,
    f"H={got['h_statistic']:.4f} vs {exact_h:.4f} "
    f"({100*(got['h_statistic']-exact_h)/exact_h:+.2f}%, {got['n_rank_bins']} rank bins)",
)
check(
    "p-value agrees with scipy to within an order of magnitude",
    0.1 < got["p_value"] / exact_p < 10,
    f"p={got['p_value']:.3e} vs {exact_p:.3e}",
)
check(
    "every record and every group is accounted for",
    got["n_total"] == n
    and got["groups"] == ["A", "B", "C"]
    and sum(got["n_per_group"].values()) == n,
)

# A column with no group effect must stay comfortably non-significant.
null_values = rng.normal(0, 1, n)
null_nodes = split(null_values, groups, 3, "x")
_, null_exact_p = kruskal(*[null_values[groups == g] for g in ["A", "B", "C"]])
null_got = federated_pipeline(null_nodes, "Group", "x")
check(
    "a column with no group effect stays non-significant",
    null_got["p_value"] > 0.05 and null_exact_p > 0.05,
    f"p={null_got['p_value']:.3f} vs exact {null_exact_p:.3f}",
)

# Accuracy must not depend on how the data happens to be split across nodes.
spreads = [federated_pipeline(split(values, groups, k, "x"), "Group", "x")["h_statistic"] for k in (1, 2, 5, 10)]
check(
    "the statistic is stable across 1, 2, 5 and 10 node partitions",
    (max(spreads) - min(spreads)) / exact_h < 0.05,
    f"H range {min(spreads):.4f}-{max(spreads):.4f} vs exact {exact_h:.4f}",
)

# ── Disclosure control ───────────────────────────────────────────────────────
import random

random.seed(11)
ok = True
for _ in range(500):
    counts = [random.choice([0, 0, 0, 1, 1, 2, 3, 7, 20]) for _ in range(random.randint(1, 60))]
    merged = binned.merge_small_cells(counts, 5)
    if sum(merged) != sum(counts):
        ok = False
        break
    if sum(counts) >= 5 and any(0 < c < 5 for c in merged):
        ok = False
        break
check("merge_small_cells preserves the record count and leaves no cell in [1, 5)", ok)

# ── Degenerate inputs ────────────────────────────────────────────────────────
check(
    "a single group returns an error rather than raising",
    "error" in binned.kruskal_wallis_from_scores({"A": 55.0}, {"A": 385.0}, {"A": 10}),
)
check(
    "all observations tied returns an error rather than raising",
    "error" in binned.kruskal_wallis_from_scores(
        {"A": 5 * 5.5, "B": 5 * 5.5}, {"A": 5 * 5.5 ** 2, "B": 5 * 5.5 ** 2}, {"A": 5, "B": 5}
    ),
)

print()
print("FAILURES:", failures if failures else "none")
sys.exit(1 if failures else 0)
