"""
Plain unit tests for the Dunn's test math, run without any vantage6 scaffolding
(no MockNetwork, no Docker).

Two kinds of checks:

- exact: fed true mid-ranks, the score-based Kruskal-Wallis and Dunn's test
  must equal ``scipy.stats.kruskal`` and ``scikit_posthocs.posthoc_dunn``;
- binned: the full three-round pipeline must stay close to the exact test.

Run as:

    uv run pytest test/test_dunn.py
"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import scikit_posthocs as skp
from scipy.stats import kruskal, rankdata
from statsmodels.stats.multitest import multipletests

sys.path.insert(0, str(Path(__file__).parent.parent / "v6-dunn-py"))

from dunn_math import (  # noqa: E402
    N_THRESHOLD_FLAG,
    adjust_p_values,
    assign_bins,
    coarsen_histogram,
    combine_scores,
    dunn,
    kruskal_wallis,
    local_histogram,
    local_moments,
    local_rank_scores,
    merge_small_cells,
    mid_ranks,
    rank_bins,
    reporting_grid,
)
from vantage6.algorithm.tools.exceptions import InputError  # noqa: E402


def make_nodes(n_per_node, n_nodes, seed, shift=0.5, discrete=False, groups="ABC"):
    rng = np.random.default_rng(seed)
    nodes = []
    for _ in range(n_nodes):
        g = rng.choice(list(groups), n_per_node)
        y = rng.normal(size=n_per_node) + shift * np.array([groups.index(x) for x in g])
        if discrete:
            y = np.round(y * 2)
        nodes.append(pd.DataFrame({"g": g, "y": y}))
    return nodes


def exact_groups(df):
    """Per-group score statistics using true mid-ranks of the pooled data."""
    ranks = rankdata(df["y"])
    out = {}
    for label in sorted(df["g"].unique()):
        r = ranks[(df["g"] == label).to_numpy()]
        out[label] = {"n": len(r), "score_sum": r.sum(), "score_sq_sum": (r**2).sum()}
    return out


def federated_groups(nodes, minimum_cell_count=5, maximum_rank_bins=50):
    moments = [local_moments(d, "g", "y", 10, 5) for d in nodes]
    edges = reporting_grid(moments, 200, 4.0)
    histograms = [local_histogram(d, "g", "y", edges, 5, minimum_cell_count) for d in nodes]
    r_edges, mids, n_bins = rank_bins(edges, histograms, minimum_cell_count, maximum_rank_bins)
    scores = [local_rank_scores(d, "g", "y", r_edges, mids, 5) for d in nodes]
    return combine_scores(scores), n_bins


def reference_p(df, p_adjust):
    return skp.posthoc_dunn(
        df, val_col="y", group_col="g", p_adjust=None if p_adjust == "none" else p_adjust
    )


# --------------------------------------------------------------------------- #
# exact: true mid-ranks
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("discrete", [False, True])
@pytest.mark.parametrize("p_adjust", ["none", "bonferroni", "holm", "fdr_bh"])
def test_exact_scores_match_scikit_posthocs(discrete, p_adjust):
    df = pd.concat(make_nodes(60, 2, seed=1, discrete=discrete, groups="ABCD"))
    rows = dunn(exact_groups(df), p_adjust, 0.05)
    ref = reference_p(df, p_adjust)
    for r in rows:
        np.testing.assert_allclose(r["p_adjusted"], ref.loc[r["group_1"], r["group_2"]], rtol=1e-10)


@pytest.mark.parametrize("discrete", [False, True])
def test_exact_scores_match_scipy_kruskal(discrete):
    df = pd.concat(make_nodes(50, 3, seed=2, discrete=discrete))
    kw = kruskal_wallis(exact_groups(df))
    ref = kruskal(*[df.y[df.g == x] for x in "ABC"])
    np.testing.assert_allclose([kw["h_statistic"], kw["p_value"]], [ref.statistic, ref.pvalue], rtol=1e-10)
    assert kw["df"] == 2


def test_z_sign_follows_mean_rank_difference():
    df = pd.concat(make_nodes(80, 1, seed=3, shift=1.0))
    rows = {(r["group_1"], r["group_2"]): r for r in dunn(exact_groups(df), "none", 0.05)}
    assert rows[("A", "C")]["z"] < 0 and rows[("A", "C")]["mean_rank_difference"] < 0


@pytest.mark.parametrize("method", ["bonferroni", "holm", "fdr_bh"])
def test_p_adjust_matches_statsmodels(method):
    p = np.random.default_rng(4).uniform(0, 0.2, 15)
    p[3] = p[7]  # a tie
    np.testing.assert_allclose(adjust_p_values(p, method), multipletests(p, method=method)[1], rtol=1e-12)


def test_p_adjust_rejects_unknown_method():
    with pytest.raises(InputError):
        adjust_p_values([0.01, 0.02], "sidak")


# --------------------------------------------------------------------------- #
# binning building blocks
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("seed", range(5))
def test_merge_small_cells_has_no_small_cells_and_keeps_total(seed):
    counts = np.random.default_rng(seed).poisson(2, 60).tolist()
    merged = merge_small_cells(counts, 5)
    assert sum(merged) == sum(counts)
    assert all(c == 0 or c >= 5 for c in merged)


def test_assign_bins_clips_out_of_range_values():
    edges = [0.0, 1.0, 2.0, 3.0]
    assert assign_bins([-5, 0, 0.5, 1, 2.99, 3, 50], edges).tolist() == [0, 0, 0, 1, 2, 2, 2]


def test_coarsen_histogram_and_mid_ranks():
    edges, counts = coarsen_histogram(list(range(7)), [3, 3, 0, 6, 1, 2], 5)
    assert counts == [6, 9] and edges == [0, 2, 6]
    assert mid_ranks([2, 3]) == [1.5, 4.0]


def test_rank_bins_respect_per_node_floor():
    nodes = make_nodes(200, 3, seed=6)
    moments = [local_moments(d, "g", "y", 10, 5) for d in nodes]
    edges = reporting_grid(moments, 200, 4.0)
    hists = [local_histogram(d, "g", "y", edges, 5, 5) for d in nodes]
    _, mids, n_bins = rank_bins(edges, hists, 5, 50)
    assert n_bins == len(mids) >= 10
    # mid-ranks imply the bin counts: each bin holds >= 5 records per node (3
    # nodes) and >= N / 50, and together they hold all N = 600 records
    counts = np.array(_counts_from_mid_ranks(mids))
    assert counts.min() >= max(5 * 3, 600 // 50) and counts.sum() == 600


def _counts_from_mid_ranks(mids):
    counts, cumulative = [], 0
    for m in mids:
        c = round(2 * (m - cumulative) - 1)
        counts.append(c)
        cumulative += c
    return counts


# --------------------------------------------------------------------------- #
# binned pipeline vs exact
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("seed", range(5))
@pytest.mark.parametrize("discrete", [False, True])
def test_binned_pipeline_close_to_exact_at_moderate_n(seed, discrete):
    nodes = make_nodes(150, 3, seed=seed, discrete=discrete)
    pooled = pd.concat(nodes)
    groups, _ = federated_groups(nodes)

    assert {g: v["n"] for g, v in groups.items()} == pooled.g.value_counts().sort_index().to_dict()
    kw = kruskal_wallis(groups)
    ref_h = kruskal(*[pooled.y[pooled.g == x] for x in "ABC"]).statistic
    assert abs(kw["h_statistic"] / ref_h - 1) < 0.05

    ref = reference_p(pooled, "bonferroni")
    for r in dunn(groups, "bonferroni", 0.05):
        assert r["reject"] == (ref.loc[r["group_1"], r["group_2"]] <= 0.05)


def test_exact_when_every_value_has_its_own_bin():
    """With no cell floor and enough bins, binned ranks are the true ranks."""
    df = pd.DataFrame({"g": list("AABBBCCC") * 3, "y": np.arange(24.0)})
    groups, n_bins = federated_groups([df], minimum_cell_count=1, maximum_rank_bins=1000)
    assert n_bins == 24
    rows = dunn(groups, "none", 0.05)
    ref = reference_p(df, "none")
    for r in rows:
        np.testing.assert_allclose(r["p_value"], ref.loc[r["group_1"], r["group_2"]], rtol=1e-10)


# --------------------------------------------------------------------------- #
# node guards and validation
# --------------------------------------------------------------------------- #
def test_small_groups_suppressed_in_every_round():
    df = pd.concat(make_nodes(60, 1, seed=7))
    df = pd.concat([df[df.g != "C"], df[df.g == "C"].iloc[:3]])
    m = local_moments(df, "g", "y", 10, 5)
    assert m["suppressed_groups"] == ["C"]
    assert m["n"] == (df.g != "C").sum()
    edges = reporting_grid([m], 50, 4.0)
    assert sum(local_histogram(df, "g", "y", edges, 5, 5)) == m["n"]
    assert "C" not in local_rank_scores(df, "g", "y", edges, list(range(50)), 5)


def test_node_below_threshold_shares_nothing():
    df = pd.concat(make_nodes(10, 1, seed=8, groups="A"))
    assert local_moments(df, "g", "y", 10, 5) == {N_THRESHOLD_FLAG: True}


def test_payloads_are_json_serialisable():
    nodes = make_nodes(60, 2, seed=9)
    groups, _ = federated_groups(nodes)
    m = local_moments(nodes[0], "g", "y", 10, 5)
    json.dumps([m, groups, dunn(groups, "holm", 0.05)])


def test_labels_match_across_int_and_float_columns():
    a = pd.DataFrame({"g": [1, 2] * 10, "y": np.arange(20.0)})
    b = a.assign(g=a.g.astype(float))
    edges = reporting_grid([local_moments(a, "g", "y", 0, 1)], 10, 4.0)
    sa = local_rank_scores(a, "g", "y", edges, list(range(10)), 1)
    sb = local_rank_scores(b, "g", "y", edges, list(range(10)), 1)
    assert set(sa) == set(sb) == {"1", "2"}


def test_rejects_bad_columns_and_constant_values():
    df = pd.DataFrame({"g": list("AB") * 10, "y": 1.0, "t": "x"})
    with pytest.raises(InputError):
        local_moments(df, "g", "missing", 0, 1)
    with pytest.raises(InputError):
        local_moments(df, "g", "t", 0, 1)
    with pytest.raises(InputError, match="identical"):
        reporting_grid([local_moments(df, "g", "y", 0, 1)], 10, 4.0)


def test_rejects_single_group():
    df = pd.concat(make_nodes(40, 1, seed=10, groups="A"))
    with pytest.raises(InputError, match="2 groups"):
        kruskal_wallis(exact_groups(df))
