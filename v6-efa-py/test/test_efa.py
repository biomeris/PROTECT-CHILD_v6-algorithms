"""
Plain unit tests for the EFA math, run without any vantage6 scaffolding (no
MockNetwork, no Docker). Results on federated moments are compared with
``factor_analyzer`` (0.5.1) fitted on the pooled raw data.

Run as:

    uv run pytest test/test_efa.py
"""

import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from factor_analyzer import FactorAnalyzer, calculate_bartlett_sphericity, calculate_kmo
from scipy.optimize import linear_sum_assignment

sys.path.insert(0, str(Path(__file__).parent.parent / "v6-efa-py"))

from efa_math import (  # noqa: E402
    METHODS,
    N_THRESHOLD_FLAG,
    ROTATIONS,
    bartlett,
    combine,
    correlation_matrix,
    fit_efa,
    kaiser_n_factors,
    kmo,
    local_moments,
)
from vantage6.algorithm.tools.exceptions import InputError  # noqa: E402


def make_data(n_vars, n_factors, n, seed):
    rng = np.random.default_rng(seed)
    L = rng.uniform(0.4, 0.8, size=(n_vars, n_factors))
    L *= rng.random((n_vars, n_factors)) > 0.5
    L[np.arange(n_vars), np.arange(n_vars) % n_factors] = rng.uniform(0.5, 0.8, n_vars)
    X = rng.normal(size=(n, n_factors)) @ L.T + rng.normal(scale=0.6, size=(n, n_vars))
    X = X * rng.uniform(1, 20, n_vars) + rng.uniform(-50, 50, n_vars)
    return pd.DataFrame(X, columns=[f"v{i}" for i in range(n_vars)])


def split_rows(df, cuts):
    bounds = [0, *cuts, len(df)]
    return [df.iloc[a:b] for a, b in zip(bounds[:-1], bounds[1:])]


def federated_R(nodes, cols):
    return correlation_matrix(combine([local_moments(h, cols, 0) for h in nodes]))


def reference(df, n_factors, method, rotation):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return FactorAnalyzer(n_factors=n_factors, method=method, rotation=rotation).fit(df)


def align(A, B):
    """Permute and sign-flip the columns of A to match B (factor order and sign
    are arbitrary; factor_analyzer does not re-sort principal factors after
    rotation)."""
    if A.shape[1] == 1:
        return A * np.sign(np.sum(A * B)), [0]
    _, idx = linear_sum_assignment(-np.abs(B.T @ A))
    A = A[:, idx]
    return A * np.sign(np.sum(A * B, axis=0)), idx


@pytest.mark.parametrize("n_nodes", [1, 3])
def test_correlation_matrix_is_exact(n_nodes):
    df = make_data(7, 2, 300, seed=n_nodes)
    cols = list(df.columns)
    R = federated_R(split_rows(df, [80, 190][: n_nodes - 1]), cols)
    np.testing.assert_allclose(R, df.corr().to_numpy(), atol=1e-12)


def test_kmo_and_bartlett_match_reference():
    df = make_data(8, 2, 250, seed=3)
    R = federated_R(split_rows(df, [100, 170]), list(df.columns))
    per_item, overall = kmo(R)
    ref_items, ref_overall = calculate_kmo(df)
    np.testing.assert_allclose(per_item, ref_items, rtol=1e-10)
    np.testing.assert_allclose(overall, ref_overall, rtol=1e-10)
    stat, _, p = bartlett(R, len(df))
    ref_stat, ref_p = calculate_bartlett_sphericity(df)
    np.testing.assert_allclose([stat, p], [ref_stat, ref_p], rtol=1e-9, atol=1e-300)


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("rotation", ROTATIONS)
@pytest.mark.parametrize("n_factors,n_vars", [(1, 5), (2, 8), (3, 10)])
def test_matches_factor_analyzer(method, rotation, n_factors, n_vars):
    seed = 10 * n_factors + METHODS.index(method)
    df = make_data(n_vars, n_factors, 400, seed)
    R = federated_R(split_rows(df, [120, 260]), list(df.columns))
    fit = fit_efa(R, n_factors, method, rotation)
    ref = reference(df, n_factors, method, rotation)

    tol = 1e-5 if method == "ml" else 1e-6
    A, idx = align(fit["loadings"], ref.loadings_)
    np.testing.assert_allclose(A, ref.loadings_, atol=tol)
    np.testing.assert_allclose(fit["communalities"], ref.get_communalities(), atol=tol)
    np.testing.assert_allclose(
        np.sort(fit["ss_loadings"]), np.sort(ref.get_factor_variance()[0]), atol=tol
    )
    assert fit["converged"]

    if rotation in ("promax", "oblimin") and n_factors > 1:
        S, _ = align(fit["structure"], ref.structure_)
        np.testing.assert_allclose(S, ref.structure_, atol=tol)
        # factor correlations: a correlation matrix consistent with the loadings
        phi = fit["phi"]
        np.testing.assert_allclose(np.diag(phi), 1, atol=1e-10)
        np.testing.assert_allclose(fit["loadings"] @ phi, fit["structure"], atol=1e-12)
    else:
        assert fit["phi"] is None


def test_factors_sorted_and_signed():
    df = make_data(9, 3, 400, seed=5)
    fit = fit_efa(df.corr().to_numpy(), 3, "minres", "promax")
    ss = fit["ss_loadings"]
    assert np.all(np.diff(ss) <= 1e-12)
    assert np.all(fit["loadings"].sum(axis=0) > 0)


def test_recovers_known_structure():
    rng = np.random.default_rng(0)
    L = np.zeros((8, 2))
    L[:4, 0] = 0.8
    L[4:, 1] = 0.7
    X = rng.normal(size=(5000, 2)) @ L.T + rng.normal(size=(5000, 8)) * np.sqrt(
        1 - (L**2).sum(1)
    )
    fit = fit_efa(np.corrcoef(X.T), 2, "ml", "varimax")
    np.testing.assert_allclose(np.abs(fit["loadings"]), L, atol=0.05)


def test_kaiser_criterion():
    df = make_data(9, 3, 1000, seed=8)
    R = df.corr().to_numpy()
    assert kaiser_n_factors(R) == int(np.sum(np.linalg.eigvalsh(R) > 1))
    assert kaiser_n_factors(np.eye(4)) == 1


def test_missing_values_dropped_listwise():
    df = make_data(6, 2, 200, seed=9)
    df.iloc[[0, 5, 9], 1] = np.nan
    df.iloc[[3], 4] = np.nan
    R = federated_R(split_rows(df, [70, 140]), list(df.columns))
    np.testing.assert_allclose(R, df.dropna().corr().to_numpy(), atol=1e-12)


def test_node_below_threshold_shares_nothing():
    df = make_data(5, 1, 10, seed=11)
    assert local_moments(df, list(df.columns), minimum_records=10) == {
        N_THRESHOLD_FLAG: True
    }
    assert "xtx" in local_moments(df, list(df.columns), minimum_records=9)


def test_moments_are_json_serialisable():
    df = make_data(4, 1, 50, seed=12)
    json.dumps(local_moments(df, list(df.columns), 0))


def test_rejects_mismatched_columns():
    df = make_data(4, 1, 50, seed=13)
    cols = list(df.columns)
    with pytest.raises(InputError):
        combine([local_moments(df, cols, 0), local_moments(df, cols[::-1], 0)])


def test_rejects_missing_and_non_numeric_columns():
    df = make_data(4, 1, 50, seed=14)
    with pytest.raises(InputError):
        local_moments(df, ["v0", "nope"], 0)
    df["text"] = "x"
    with pytest.raises(InputError):
        local_moments(df, ["v0", "text"], 0)


def test_rejects_constant_column():
    df = make_data(4, 1, 50, seed=15)
    df["v3"] = 7.0
    with pytest.raises(InputError, match="zero variance"):
        federated_R([df], list(df.columns))
