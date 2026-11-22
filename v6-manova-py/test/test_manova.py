"""
Plain unit tests for the MANOVA math, run without any vantage6 scaffolding (no
MockNetwork, no Docker). Every federated result is compared with statsmodels'
MANOVA on the pooled data.

Run as:

    uv run pytest test/test_manova.py
"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from scipy.stats import f_oneway
from statsmodels.multivariate.manova import MANOVA

sys.path.insert(0, str(Path(__file__).parent.parent / "v6-manova-py"))

from manova_math import (  # noqa: E402
    N_THRESHOLD_FLAG,
    combine,
    local_summary,
    manova_tests,
    normalize_label,
    sscp_matrices,
)
from vantage6.algorithm.tools.exceptions import InputError  # noqa: E402


def make_data(n_groups, n_outcomes, seed, shift=0.3):
    rng = np.random.default_rng(seed)
    cols = [f"y{i}" for i in range(n_outcomes)]
    A = rng.normal(size=(n_outcomes, n_outcomes))
    cov = A @ A.T + n_outcomes * np.eye(n_outcomes)
    frames = []
    for g in range(n_groups):
        mu = rng.normal(scale=shift, size=n_outcomes)
        n = int(rng.integers(15, 60))
        frames.append(
            pd.DataFrame(rng.multivariate_normal(mu, cov, size=n), columns=cols)
            .assign(group=f"g{g}")
        )
    df = pd.concat(frames, ignore_index=True).sample(frac=1, random_state=seed)
    return df.reset_index(drop=True), cols


def split_rows(df, cuts):
    bounds = [0, *cuts, len(df)]
    return [df.iloc[a:b] for a, b in zip(bounds[:-1], bounds[1:])]


def summarise(nodes, cols, minimum_records=0, minimum_group_size=1):
    return [
        local_summary(h, "group", cols, minimum_records, minimum_group_size)
        for h in nodes
    ]


def federated_table(summaries):
    W, B, N, k = sscp_matrices(combine(summaries))
    return manova_tests(W, B, N, k)


def pooled_reference(df, cols):
    res = MANOVA.from_formula(" + ".join(cols) + " ~ group", data=df).mv_test()
    return res.results["group"]["stat"].astype(float)


def assert_matches(fed, ref):
    ref = ref.loc[fed.index, fed.columns]
    np.testing.assert_allclose(fed.to_numpy(), ref.to_numpy(), rtol=1e-7, atol=1e-10)


@pytest.mark.parametrize("n_groups", [2, 3, 5])
@pytest.mark.parametrize("n_outcomes", [2, 3, 5])
@pytest.mark.parametrize("n_nodes", [1, 3, 4])
def test_matches_pooled_statsmodels(n_groups, n_outcomes, n_nodes):
    seed = 100 * n_groups + 10 * n_outcomes + n_nodes
    df, cols = make_data(n_groups, n_outcomes, seed)
    rng = np.random.default_rng(seed)
    cuts = sorted(rng.choice(np.arange(5, len(df) - 5), n_nodes - 1, replace=False))
    fed = federated_table(summarise(split_rows(df, cuts), cols))
    assert_matches(fed, pooled_reference(df, cols))


def test_single_outcome_reduces_to_anova():
    """With one outcome all four statistics give the one-way ANOVA F."""
    df, cols = make_data(4, 1, seed=5)
    fed = federated_table(summarise(split_rows(df, [30, 70]), cols))
    anova = f_oneway(*[g[cols[0]].to_numpy() for _, g in df.groupby("group")])
    np.testing.assert_allclose(fed["F Value"], anova.statistic, rtol=1e-9)
    np.testing.assert_allclose(fed["Pr > F"], anova.pvalue, rtol=1e-7)


def test_group_missing_at_some_nodes():
    df, cols = make_data(3, 3, seed=7)
    nodes = [df[df.group != "g2"], df[df.group == "g2"]]
    assert_matches(federated_table(summarise(nodes, cols)), pooled_reference(df, cols))


def test_missing_values_dropped_listwise():
    df, cols = make_data(3, 3, seed=11)
    df.loc[[0, 5, 9], "y1"] = np.nan
    df.loc[[3], "group"] = None
    fed = federated_table(summarise(split_rows(df, [40, 80]), cols))
    assert_matches(fed, pooled_reference(df.dropna(), cols))


def test_labels_match_across_int_and_float_columns():
    df, cols = make_data(2, 2, seed=13)
    df["group"] = (df["group"] == "g1").astype(int)
    a, b = df.iloc[:40].copy(), df.iloc[40:].copy()
    b["group"] = b["group"].astype(float)  # e.g. read with NaNs at that node
    summaries = summarise([a, b], cols)
    assert set(summaries[0]["groups"]) == set(summaries[1]["groups"]) == {"0", "1"}
    assert normalize_label(1.0) == normalize_label(1) == "1"
    assert normalize_label(1.5) == "1.5"


def test_small_groups_are_suppressed_locally():
    df, cols = make_data(3, 2, seed=17)
    node = pd.concat([df[df.group != "g2"], df[df.group == "g2"].iloc[:3]])
    summary = local_summary(node, "group", cols, minimum_records=0, minimum_group_size=5)
    assert summary["suppressed_groups"] == ["g2"]
    assert "g2" not in summary["groups"]


def test_node_below_threshold_shares_nothing():
    df, cols = make_data(2, 2, seed=19)
    summary = local_summary(df.iloc[:8], "group", cols, minimum_records=10, minimum_group_size=1)
    assert summary == {N_THRESHOLD_FLAG: True}


def test_threshold_counts_records_after_suppression():
    df, cols = make_data(2, 2, seed=23)
    node = pd.concat([df[df.group == "g0"].iloc[:8], df[df.group == "g1"].iloc[:4]])
    # 12 complete cases, but only 8 remain once g1 (4 < 5) is suppressed
    summary = local_summary(node, "group", cols, minimum_records=10, minimum_group_size=5)
    assert summary == {N_THRESHOLD_FLAG: True}


def test_summary_is_json_serialisable():
    df, cols = make_data(2, 2, seed=3)
    json.dumps(local_summary(df, "group", cols, 0, 1))


def test_rejects_mismatched_columns():
    df, cols = make_data(2, 2, seed=3)
    a = local_summary(df, "group", cols, 0, 1)
    b = local_summary(df, "group", cols[::-1], 0, 1)
    with pytest.raises(InputError):
        combine([a, b])


def test_rejects_missing_and_non_numeric_columns():
    df, cols = make_data(2, 2, seed=3)
    with pytest.raises(InputError):
        local_summary(df, "group", [*cols, "nope"], 0, 1)
    df["text"] = "x"
    with pytest.raises(InputError):
        local_summary(df, "group", [cols[0], "text"], 0, 1)


def test_rejects_single_group():
    df, cols = make_data(1, 2, seed=29)
    with pytest.raises(InputError, match="at least 2 groups"):
        federated_table(summarise([df], cols))


def test_rejects_collinear_outcomes():
    df, cols = make_data(3, 2, seed=31)
    df["y_dup"] = 2 * df["y0"] + 1
    with pytest.raises(InputError, match="singular"):
        federated_table(summarise([df], [*cols, "y_dup"]))
