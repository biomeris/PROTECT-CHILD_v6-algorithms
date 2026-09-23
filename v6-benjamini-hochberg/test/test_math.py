"""
Verify the statistical core of the algorithm without needing vantage6 or Docker.

The two modules under test (`bh.py` and `binned.py`) deliberately have no
vantage6 imports, so they can be checked directly against scipy and against
naive reference implementations. Run as:

    python test_math.py

Requires only numpy and scipy. For the end-to-end federated run against the
vantage6 mock client, see `test.py`.
"""
import importlib.util
import random
import sys
from pathlib import Path

import numpy as np
from scipy.stats import kruskal, mannwhitneyu, rankdata

PKG = Path(__file__).parent.parent / "v6-benjamini-hochberg"


def _load(name):
    """Import a module by path so the vantage6-dependent package is bypassed."""
    spec = importlib.util.spec_from_file_location(name, PKG / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


bh = _load("bh")
binned = _load("binned")

failures = []


def check(label, ok, detail=""):
    print(f"{'PASS' if ok else 'FAIL'}  {label}{('  ' + detail) if detail else ''}")
    if not ok:
        failures.append(label)


def federated_pipeline(
    nodes,
    group_col,
    column,
    test,
    histogram_bins=200,
    min_cell=5,
    max_rank_bins=50,
):
    """
    Reproduce the three-round protocol in-process, node by node.

    Mirrors `partial_moments` -> `partial_histogram` -> `partial_rank_sums` and
    the central aggregation between them, including the fact that only moments,
    counts and rank sums ever cross a node boundary.
    """
    # Round 1: moments only.
    n_total = sum(len(node[column]) for node in nodes)
    total = sum(float(np.sum(node[column])) for node in nodes)
    total_sq = sum(float(np.sum(np.square(node[column]))) for node in nodes)
    mean = total / n_total
    sd = np.sqrt(max(total_sq / n_total - mean ** 2, 0.0))
    reporting_edges = binned.bin_edges(mean, sd, histogram_bins, 4.0)

    # Round 2: group-blind histogram, small cells merged at the node.
    pooled = [0] * histogram_bins
    for node in nodes:
        raw = [0] * histogram_bins
        for idx in binned.assign_bins(list(node[column]), reporting_edges):
            raw[idx] += 1
        for i, count in enumerate(binned.merge_small_cells(raw, min_cell)):
            pooled[i] += count

    # Central: coarsen into roughly equal-count rank bins.
    minimum_per_bin = max(min_cell * len(nodes), n_total // max_rank_bins, 1)
    rank_edges, rank_counts = binned.coarsen_histogram(
        reporting_edges, pooled, minimum_per_bin
    )
    ranks = binned.mid_ranks(rank_counts)

    # Round 3: score sum, score sum of squares and count per group.
    sums, sq_sums, group_ns = {}, {}, {}
    for node in nodes:
        per_group = {}
        for group, value in zip(node[group_col], node[column]):
            per_group.setdefault(str(group), []).append(value)
        for group, values in per_group.items():
            scores = [ranks[i] for i in binned.assign_bins(values, rank_edges)]
            sums[group] = sums.get(group, 0.0) + sum(scores)
            sq_sums[group] = sq_sums.get(group, 0.0) + sum(s * s for s in scores)
            group_ns[group] = group_ns.get(group, 0) + len(scores)

    result = binned.run_test(test, sums, sq_sums, group_ns)
    result["n_rank_bins"] = len(rank_counts)
    return result


def scores_by_group(scores, groups):
    """Aggregate a score vector into the three per-group numbers round 3 returns."""
    sums, sq_sums, ns = {}, {}, {}
    for score, group in zip(scores, groups):
        group = str(group)
        sums[group] = sums.get(group, 0.0) + score
        sq_sums[group] = sq_sums.get(group, 0.0) + score * score
        ns[group] = ns.get(group, 0) + 1
    return sums, sq_sums, ns


# ── Benjamini-Hochberg procedure ─────────────────────────────────────────────
def naive_bh(ps, alpha):
    """Textbook definition, O(m^2), as an independent reference."""
    m = len(ps)
    order = sorted(range(m), key=lambda i: ps[i])
    adjusted = [0.0] * m
    for rank, idx in enumerate(order, start=1):
        adjusted[idx] = min(
            1.0, min(m / j * ps[order[j - 1]] for j in range(rank, m + 1))
        )
    largest = 0
    for j in range(m, 0, -1):
        if ps[order[j - 1]] <= j / m * alpha:
            largest = j
            break
    rejected = [False] * m
    for rank, idx in enumerate(order, start=1):
        rejected[idx] = rank <= largest
    return adjusted, rejected


random.seed(7)
ok = True
for _ in range(200):
    m = random.randint(1, 40)
    ps = [round(random.random() ** random.choice([1, 3, 6]), 6) for _ in range(m)]
    alpha = random.choice([0.01, 0.05, 0.1])
    got = bh.benjamini_hochberg(ps, alpha=alpha)
    expected_adjusted, expected_rejected = naive_bh(ps, alpha)
    for i in range(m):
        result = got["results"][f"hypothesis_{i + 1}"]
        if abs(result["adjusted_p_value"] - expected_adjusted[i]) > 1e-12:
            ok = False
        if result["reject"] != expected_rejected[i]:
            ok = False
    if got["n_rejected"] != sum(expected_rejected):
        ok = False
check("BH q-values and rejections match the naive reference (200 random families)", ok)

result = bh.benjamini_hochberg([0.9, 0.01, 0.03, 0.5, 0.001, 0.2, 0.04], alpha=0.05)
by_rank = sorted(result["results"].values(), key=lambda r: r["rank"])
check(
    "q-values are non-decreasing in rank",
    all(
        a["adjusted_p_value"] <= b["adjusted_p_value"] + 1e-15
        for a, b in zip(by_rank, by_rank[1:])
    ),
)

# The 15 p-values of Benjamini & Hochberg (1995), Table 1. At alpha = 0.05 the
# procedure rejects the first four hypotheses.
paper = [
    0.0001, 0.0004, 0.0019, 0.0095, 0.0201, 0.0278, 0.0298, 0.0344,
    0.0459, 0.3240, 0.4262, 0.5719, 0.6528, 0.7590, 1.000,
]
result = bh.benjamini_hochberg(paper, alpha=0.05)
check(
    "Benjamini & Hochberg (1995) Table 1 reproduces 4 rejections at alpha=0.05",
    result["n_rejected"] == 4,
    f"got {result['n_rejected']}",
)

result = bh.benjamini_hochberg([0.001, 0.02, 0.04, 0.3], alpha=0.05)
check(
    "BH is never more conservative than Bonferroni",
    all(
        r["adjusted_p_value"] <= min(1.0, r["p_value"] * 4) + 1e-12
        for r in result["results"].values()
    ),
)

result = bh.benjamini_hochberg({"a": 0.01, "b": None, "c": float("nan"), "d": 0.4})
check(
    "None/NaN p-values are excluded from m and reported untested",
    result["n_tested"] == 2
    and not result["results"]["b"]["tested"]
    and not result["results"]["c"]["tested"]
    and abs(result["results"]["a"]["adjusted_p_value"] - 0.02) < 1e-12,
)

# ── Disclosure control ───────────────────────────────────────────────────────
random.seed(11)
ok = True
for _ in range(500):
    counts = [
        random.choice([0, 0, 0, 1, 1, 2, 3, 7, 20])
        for _ in range(random.randint(1, 60))
    ]
    merged = binned.merge_small_cells(counts, 5)
    if sum(merged) != sum(counts):
        ok = False
        break
    if sum(counts) >= 5 and any(0 < c < 5 for c in merged):
        ok = False
        break
check("merge_small_cells preserves the record count and leaves no cell in [1, 5)", ok)

random.seed(13)
ok = True
for _ in range(500):
    n_bins = random.randint(1, 80)
    totals = [random.choice([0, 0, 1, 2, 5, 11, 30]) for _ in range(n_bins)]
    edges = list(range(n_bins + 1))
    target = random.randint(2, 25)
    coarse_edges, coarse_counts = binned.coarsen_histogram(edges, totals, target)
    if sum(coarse_counts) != sum(totals):
        ok = False
        break
    if len(coarse_edges) != len(coarse_counts) + 1:
        ok = False
        break
    if coarse_edges[0] != edges[0] or coarse_edges[-1] != edges[-1]:
        ok = False
        break
    if coarse_edges != sorted(coarse_edges):
        ok = False
        break
    # Every rank bin but the last is at or above the target.
    if any(c < target for c in coarse_counts[:-1]):
        ok = False
        break
check(
    "coarsen_histogram conserves records, spans the grid, and meets the target count",
    ok,
)

# ── Rank tests through the full three-round pipeline ─────────────────────────
def split(values, groups, n_nodes, column, group_col="Group"):
    """Partition a dataset across nodes, as horizontally partitioned data."""
    chunks = np.array_split(np.arange(len(values)), n_nodes)
    return [
        {group_col: groups[idx], column: values[idx]} for idx in chunks
    ]


rng = np.random.default_rng(3)
n = 600
groups = rng.choice(["A", "B", "C"], size=n)
values = rng.normal(0, 1, n) + (groups == "B") * 0.45 + (groups == "C") * 0.9
nodes = split(values, groups, 3, "x")
exact_h, exact_p = kruskal(*[values[groups == g] for g in ["A", "B", "C"]])

got = federated_pipeline(nodes, "Group", "x", binned.KRUSKAL_WALLIS)
check(
    "three-round Kruskal-Wallis H is within 5% of scipy.stats.kruskal",
    abs(got["statistic"] - exact_h) / exact_h < 0.05,
    f"H={got['statistic']:.4f} vs {exact_h:.4f} "
    f"({100 * (got['statistic'] - exact_h) / exact_h:+.2f}%, {got['n_rank_bins']} rank bins)",
)
check(
    "Kruskal-Wallis p-value agrees with scipy to within an order of magnitude",
    0.1 < got["p_value"] / exact_p < 10,
    f"p={got['p_value']:.3e} vs {exact_p:.3e}",
)
check(
    "Kruskal-Wallis accounts for every record and every group",
    got["n_total"] == n
    and got["groups"] == ["A", "B", "C"]
    and sum(got["n_per_group"].values()) == n,
)

# A column with no group effect must stay comfortably non-significant.
null_values = rng.normal(0, 1, n)
null_nodes = split(null_values, groups, 3, "x")
_, null_exact_p = kruskal(*[null_values[groups == g] for g in ["A", "B", "C"]])
null_got = federated_pipeline(null_nodes, "Group", "x", binned.KRUSKAL_WALLIS)
check(
    "a column with no group effect stays non-significant",
    null_got["p_value"] > 0.05 and null_exact_p > 0.05,
    f"p={null_got['p_value']:.3f} vs exact {null_exact_p:.3f}",
)

rng = np.random.default_rng(5)
n = 600
groups2 = rng.choice(["case", "ctrl"], size=n)
values2 = rng.normal(0, 1, n) + (groups2 == "case") * 0.4
nodes2 = split(values2, groups2, 3, "y")
exact_u, exact_p = mannwhitneyu(
    values2[groups2 == "case"],
    values2[groups2 == "ctrl"],
    alternative="two-sided",
    use_continuity=False,
    method="asymptotic",
)
got = federated_pipeline(nodes2, "Group", "y", binned.MANN_WHITNEY)
check(
    "three-round Mann-Whitney U is within 3% of scipy.stats.mannwhitneyu",
    abs(got["statistic"] - exact_u) / exact_u < 0.03,
    f"U={got['statistic']:.1f} vs {exact_u:.1f} "
    f"({100 * (got['statistic'] - exact_u) / exact_u:+.2f}%, {got['n_rank_bins']} rank bins)",
)
check(
    "Mann-Whitney p-value agrees with scipy to within an order of magnitude",
    0.1 < got["p_value"] / exact_p < 10,
    f"p={got['p_value']:.3e} vs {exact_p:.3e}",
)

# Accuracy must not depend on how the data happens to be split across nodes.
spreads = [
    federated_pipeline(split(values2, groups2, k, "y"), "Group", "y",
                       binned.MANN_WHITNEY)["statistic"]
    for k in (1, 2, 5, 10)
]
check(
    "the statistic is stable across 1, 2, 5 and 10 node partitions",
    (max(spreads) - min(spreads)) / exact_u < 0.05,
    f"U range {min(spreads):.1f}-{max(spreads):.1f} vs exact {exact_u:.1f}",
)

# ── The general-scores forms are exact when fed exact mid-ranks ──────────────
# This is what licenses the whole approach: the statistic itself is not an
# approximation, only the scores the binning protocol produces are.
rng = np.random.default_rng(3)
n = 600
groups = rng.choice(["A", "B", "C"], size=n)
values = rng.normal(0, 1, n) + (groups == "B") * 0.45 + (groups == "C") * 0.9
got = binned.kruskal_wallis_from_scores(*scores_by_group(rankdata(values), groups))
exact_h, _ = kruskal(*[values[groups == g] for g in ["A", "B", "C"]])
check(
    "general-scores H equals scipy.stats.kruskal exactly on true mid-ranks",
    abs(got["statistic"] - exact_h) < 1e-9,
    f"H={got['statistic']:.10f} vs {exact_h:.10f}",
)

# Heavy ties: rounding collapses the sample onto a handful of distinct values.
tied_values = np.round(values, 0)
got = binned.kruskal_wallis_from_scores(*scores_by_group(rankdata(tied_values), groups))
exact_h, _ = kruskal(*[tied_values[groups == g] for g in ["A", "B", "C"]])
check(
    "general-scores H equals scipy under heavy ties (tie correction is intrinsic)",
    abs(got["statistic"] - exact_h) < 1e-9,
    f"H={got['statistic']:.10f} vs {exact_h:.10f}",
)

groups2 = rng.choice(["case", "ctrl"], size=n)
values2 = rng.normal(0, 1, n) + (groups2 == "case") * 0.4
for label, sample in (("", values2), (" under heavy ties", np.round(values2, 0))):
    got = binned.mann_whitney_from_scores(*scores_by_group(rankdata(sample), groups2))
    exact_u, exact_p = mannwhitneyu(
        sample[groups2 == "case"],
        sample[groups2 == "ctrl"],
        alternative="two-sided",
        use_continuity=False,
        method="asymptotic",
    )
    check(
        f"general-scores U and p equal scipy.stats.mannwhitneyu exactly{label}",
        abs(got["statistic"] - exact_u) < 1e-6
        and abs(got["p_value"] - exact_p) < 1e-12,
        f"U={got['statistic']:.6f} vs {exact_u:.6f}, "
        f"p={got['p_value']:.6e} vs {exact_p:.6e}",
    )

# ── Degenerate inputs ────────────────────────────────────────────────────────
check(
    "a single group returns an error rather than raising",
    "error" in binned.kruskal_wallis_from_scores(
        {"A": 55.0}, {"A": 385.0}, {"A": 10}
    ),
)
check(
    "all observations tied returns an error rather than raising",
    "error" in binned.kruskal_wallis_from_scores(
        {"A": 5 * 5.5, "B": 5 * 5.5},
        {"A": 5 * 5.5 ** 2, "B": 5 * 5.5 ** 2},
        {"A": 5, "B": 5},
    ),
)
check(
    "Mann-Whitney with three groups returns an error rather than raising",
    "error" in binned.mann_whitney_from_scores(
        {"A": 10.0, "B": 20.0, "C": 30.0},
        {"A": 60.0, "B": 210.0, "C": 460.0},
        {"A": 2, "B": 2, "C": 2},
    ),
)

print()
print("FAILURES:", failures if failures else "none")
sys.exit(1 if failures else 0)
