"""
Verify the statistical core of the algorithm without needing vantage6 or Docker.

`binned.py` deliberately has no vantage6 imports, so it can be checked directly
against scikit-learn's AUC and against a Monte Carlo check of the Hanley-McNeil
approximation. Run as:

    python test_math.py

Requires numpy, scipy and scikit-learn. For the end-to-end federated run against
the vantage6 mock client, see `test.py`.
"""
import importlib.util
import sys
from pathlib import Path

import numpy as np
from scipy.stats import norm, rankdata
from sklearn.metrics import roc_auc_score

PKG = Path(__file__).parent.parent / "v6-C-statistics"


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


def scores_by_group(scores, is_event):
    """Aggregate a score vector into the 6 numbers round 3 returns."""
    out = {}
    for label, mask in (("event", is_event), ("non_event", ~is_event)):
        s = np.asarray(scores)[mask]
        out[label] = {"sum": float(s.sum()), "sq_sum": float((s ** 2).sum()), "n": int(len(s))}
    return out


def c_stat(scores, outcome, alpha=0.05):
    """Exact-rank C-statistic through the shipped formula, for cross-checking."""
    ranks = rankdata(scores)
    g = scores_by_group(ranks, outcome == 1)
    return binned.c_statistic_from_scores(
        g["event"]["sum"], g["event"]["sq_sum"], g["event"]["n"],
        g["non_event"]["sum"], g["non_event"]["sq_sum"], g["non_event"]["n"],
        alpha=alpha,
    )


def federated_c(scores, outcome, n_nodes, histogram_bins=200, min_cell=5, max_rank_bins=50):
    """Reproduce the three-round protocol in-process, node by node."""
    scores = np.asarray(scores)
    outcome = np.asarray(outcome)
    chunks = np.array_split(np.arange(len(scores)), n_nodes)

    # Round 1: moments only.
    n_total = len(scores)
    mean = scores.mean()
    sd = scores.std(ddof=0)
    edges = binned.bin_edges(mean, sd, histogram_bins, 4.0)

    # Round 2: outcome-blind histogram, small cells merged at the node.
    pooled = [0] * histogram_bins
    for idx in chunks:
        raw = [0] * histogram_bins
        for i in binned.assign_bins(scores[idx].tolist(), edges):
            raw[i] += 1
        for i, count in enumerate(binned.merge_small_cells(raw, min_cell)):
            pooled[i] += count

    # Central: coarsen into roughly equal-count rank bins.
    minimum_per_bin = max(min_cell * n_nodes, n_total // max_rank_bins, 1)
    rank_edges, rank_counts = binned.coarsen_histogram(edges, pooled, minimum_per_bin)
    ranks = binned.mid_ranks(rank_counts)

    # Round 3: rank-score sum, sum of squares and count per outcome group.
    totals = {"event": {"sum": 0.0, "sq_sum": 0.0, "n": 0},
              "non_event": {"sum": 0.0, "sq_sum": 0.0, "n": 0}}
    for idx in chunks:
        node_scores, node_outcome = scores[idx], outcome[idx]
        rank_idx = binned.assign_bins(node_scores.tolist(), rank_edges)
        node_ranks = np.array([ranks[i] for i in rank_idx])
        for label, mask in (("event", node_outcome == 1), ("non_event", node_outcome == 0)):
            s = node_ranks[mask]
            totals[label]["sum"] += float(s.sum())
            totals[label]["sq_sum"] += float((s ** 2).sum())
            totals[label]["n"] += int(len(s))

    result = binned.c_statistic_from_scores(
        totals["event"]["sum"], totals["event"]["sq_sum"], totals["event"]["n"],
        totals["non_event"]["sum"], totals["non_event"]["sq_sum"], totals["non_event"]["n"],
    )
    result["n_rank_bins"] = len(rank_counts)
    return result


# ── The general-scores C-statistic is exact when fed exact mid-ranks ────────
# This licenses the whole approach: the formula itself is not an approximation,
# only the scores the binning protocol produces are.
rng = np.random.default_rng(3)
n = 600
outcome = (rng.random(n) < 0.4).astype(int)
scores = rng.normal(0, 1, n) + outcome * 0.9

got = c_stat(scores, outcome)
exact = roc_auc_score(outcome, scores)
check(
    "general-scores C equals sklearn.metrics.roc_auc_score exactly on true mid-ranks",
    abs(got["c_statistic"] - exact) < 1e-9,
    f"C={got['c_statistic']:.10f} vs {exact:.10f}",
)

tied_scores = np.round(scores, 1)
got = c_stat(tied_scores, outcome)
exact = roc_auc_score(outcome, tied_scores)
check(
    "general-scores C equals sklearn under heavy ties",
    abs(got["c_statistic"] - exact) < 1e-9,
    f"C={got['c_statistic']:.10f} vs {exact:.10f}",
)

# A perfectly discriminating score must give C = 1.
perfect_scores = outcome.astype(float) + rng.normal(0, 1e-6, n)
got = c_stat(perfect_scores, outcome)
check(
    "a near-perfectly separating score gives C close to 1",
    got["c_statistic"] > 0.999,
    f"C={got['c_statistic']:.6f}",
)

# A non-informative score must sit near C = 0.5, non-significant most of the time.
null_scores = rng.normal(0, 1, n)
got = c_stat(null_scores, outcome)
check(
    "a non-informative score sits near C=0.5 and is non-significant here",
    abs(got["c_statistic"] - 0.5) < 0.1 and got["p_value"] > 0.05,
    f"C={got['c_statistic']:.4f} p={got['p_value']:.4f}",
)

# ── Hanley-McNeil: 95% CI coverage and Type-I error by simulation ───────────
# Ground truth: X ~ N(delta, 1) for events, Y ~ N(0, 1) for non-events, so the
# true AUC is Phi(delta / sqrt(2)).
print()
rng = np.random.default_rng(11)
ok = True
detail_parts = []
for delta in (0.0, 0.5, 1.0, 2.0):
    true_auc = norm.cdf(delta / np.sqrt(2))
    covered, trials = 0, 600
    for _ in range(trials):
        x = rng.normal(delta, 1, 60)
        y = rng.normal(0, 1, 60)
        s = np.concatenate([x, y])
        o = np.concatenate([np.ones(60), np.zeros(60)])
        r = c_stat(s, o)
        if r["ci_lower"] <= true_auc <= r["ci_upper"]:
            covered += 1
    coverage = covered / trials
    detail_parts.append(f"delta={delta}:{coverage:.3f}")
    if not (0.90 <= coverage <= 0.99):
        ok = False
check(
    "Hanley-McNeil 95% CI coverage stays close to nominal across true AUC values",
    ok,
    " ".join(detail_parts),
)

rejections, trials = 0, 1000
for _ in range(trials):
    x = rng.normal(0, 1, 80)
    y = rng.normal(0, 1, 80)
    s = np.concatenate([x, y])
    o = np.concatenate([np.ones(80), np.zeros(80)])
    r = c_stat(s, o)
    if r["p_value"] < 0.05:
        rejections += 1
check(
    "Type-I error under H0: C=0.5 stays close to the nominal 5%",
    0.02 <= rejections / trials <= 0.09,
    f"empirical alpha={rejections/trials:.4f}",
)

# ── Through the full three-round pipeline ────────────────────────────────────
rng = np.random.default_rng(5)
n = 600
outcome = (rng.random(n) < 0.4).astype(int)
scores = rng.normal(0, 1, n) + outcome * 0.9
exact = roc_auc_score(outcome, scores)

got = federated_c(scores, outcome, n_nodes=3)
check(
    "three-round C-statistic is within 3% of sklearn.metrics.roc_auc_score",
    abs(got["c_statistic"] - exact) / exact < 0.03,
    f"C={got['c_statistic']:.4f} vs {exact:.4f} "
    f"({100*(got['c_statistic']-exact)/exact:+.2f}%, {got['n_rank_bins']} rank bins)",
)

null_scores = rng.normal(0, 1, n)
null_exact = roc_auc_score(outcome, null_scores)
null_got = federated_c(null_scores, outcome, n_nodes=3)
check(
    "a non-informative score stays non-significant through the federated pipeline",
    null_got["p_value"] > 0.05 and null_exact < 0.55,
    f"C={null_got['c_statistic']:.4f} p={null_got['p_value']:.4f} (exact C={null_exact:.4f})",
)

spreads = [federated_c(scores, outcome, n_nodes=k)["c_statistic"] for k in (1, 2, 5, 10)]
check(
    "the federated C-statistic is stable across 1, 2, 5 and 10 node partitions",
    (max(spreads) - min(spreads)) < 0.03,
    f"C range {min(spreads):.4f}-{max(spreads):.4f} vs exact {exact:.4f}",
)

# ── Degenerate inputs ────────────────────────────────────────────────────────
check(
    "an empty outcome group returns an error rather than raising",
    "error" in binned.c_statistic_from_scores(10.0, 100.0, 5, 0.0, 0.0, 0),
)
check(
    "all observations tied returns an error rather than raising",
    "error" in binned.c_statistic_from_scores(15.0, 45.0, 5, 15.0, 45.0, 5),
)

print()
print("FAILURES:", failures if failures else "none")
sys.exit(1 if failures else 0)
