"""
Pure exploratory factor analysis (EFA) math, free of vantage6 decorators so it
can be unit tested directly (see ``test/test_efa.py``).

Node side
    ``local_moments``      -> n, column sums (p-vector), X^T X (p x p)
Central side
    ``combine``            -> moments summed over all nodes
    ``correlation_matrix`` -> pooled Pearson correlation matrix R
    ``kmo`` / ``bartlett`` -> sampling adequacy and sphericity test on R
    ``extract``            -> unrotated loadings (minres, ml or principal)
    ``rotate``             -> varimax, promax or oblimin
    ``fit_efa``            -> the full pipeline on R

With N = sum n, S = sum of column sums and Q = sum X^T X:

    cov = (Q - S S^T / N) / (N - 1),   R = D^{-1/2} cov D^{-1/2}

so the pooled correlation matrix is exact. Everything after that works on R
alone, so the federated EFA equals an EFA on the pooled data.

The extraction and rotation follow ``factor_analyzer`` 0.5.1 (which in turn
follows R's ``psych::fa``): same objective functions, starting values, bounds,
optimizer and rotation algorithms.
"""

import numpy as np
import pandas as pd
from scipy import linalg, stats
from scipy.optimize import minimize

from vantage6.algorithm.tools.exceptions import InputError

N_THRESHOLD_FLAG = "N-Threshold not met"

METHODS = ("minres", "ml", "principal")
ROTATIONS = (None, "varimax", "promax", "oblimin")
OBLIQUE_ROTATIONS = ("promax", "oblimin")

UNIQUENESS_BOUNDS = (0.005, 1.0)


# --------------------------------------------------------------------------- #
# Node side
# --------------------------------------------------------------------------- #
def validate_columns(df: pd.DataFrame, columns: list[str]) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise InputError(f"Column(s) {missing} not found in DataFrame.")
    for col in columns:
        if pd.api.types.is_bool_dtype(df[col]) or not pd.api.types.is_numeric_dtype(
            df[col]
        ):
            raise InputError(
                f"Column '{col}' must be numeric, got dtype '{df[col].dtype}'."
            )


def local_moments(df: pd.DataFrame, columns: list[str], minimum_records: int) -> dict:
    """Count, column sums and cross-product matrix of the complete cases at one node.

    Rows with a missing value in any of ``columns`` are dropped (listwise
    deletion). If ``minimum_records`` or fewer remain, nothing is shared and the
    node flags itself instead.
    """
    validate_columns(df, columns)
    X = df[columns].dropna().to_numpy(dtype=float)
    if X.shape[0] <= minimum_records:
        return {N_THRESHOLD_FLAG: True}
    return {
        "columns": list(columns),
        "n": int(X.shape[0]),
        "sum": X.sum(axis=0).tolist(),
        "xtx": (X.T @ X).tolist(),
    }


# --------------------------------------------------------------------------- #
# Central side: pooled correlation and adequacy tests
# --------------------------------------------------------------------------- #
def combine(moments: list[dict]) -> dict:
    """Add up the moments of all nodes."""
    columns = moments[0]["columns"]
    for m in moments[1:]:
        if m["columns"] != columns:
            raise InputError("Nodes returned different columns.")
    return {
        "columns": columns,
        "n": sum(m["n"] for m in moments),
        "sum": sum(np.asarray(m["sum"], dtype=float) for m in moments),
        "xtx": sum(np.asarray(m["xtx"], dtype=float) for m in moments),
    }


def correlation_matrix(pooled: dict) -> np.ndarray:
    """Pearson correlation matrix from pooled moments."""
    n, s, q = pooled["n"], pooled["sum"], pooled["xtx"]
    if n < 2:
        raise InputError("At least 2 records are needed to compute correlations.")
    cov = (q - np.outer(s, s) / n) / (n - 1)
    var = np.diag(cov)
    constant = [c for c, v in zip(pooled["columns"], var) if v <= 1e-12 * max(1.0, abs(v))]
    if constant:
        raise InputError(f"Column(s) {constant} have zero variance.")
    d = 1 / np.sqrt(var)
    R = cov * np.outer(d, d)
    R = (R + R.T) / 2
    np.fill_diagonal(R, 1.0)
    return R


def _inverse(R: np.ndarray) -> np.ndarray:
    try:
        return linalg.inv(R)
    except linalg.LinAlgError:
        return linalg.pinv(R)


def kmo(R: np.ndarray) -> tuple[np.ndarray, float]:
    """Kaiser-Meyer-Olkin measure of sampling adequacy (per item, overall)."""
    inv = _inverse(R)
    d = 1 / np.sqrt(np.diag(inv))
    partial = -inv * np.outer(d, d)
    r2, p2 = R**2, partial**2
    np.fill_diagonal(r2, 0)
    np.fill_diagonal(p2, 0)
    per_item = r2.sum(axis=0) / (r2.sum(axis=0) + p2.sum(axis=0))
    overall = r2.sum() / (r2.sum() + p2.sum())
    return per_item, float(overall)


def bartlett(R: np.ndarray, n: int) -> tuple[float, float, float]:
    """Bartlett's test of sphericity (statistic, df, p-value)."""
    p = R.shape[0]
    sign, logdet = np.linalg.slogdet(R)
    if sign <= 0:
        return float("inf"), p * (p - 1) / 2, 0.0
    statistic = -logdet * (n - 1 - (2 * p + 5) / 6)
    df = p * (p - 1) / 2
    return float(statistic), float(df), float(stats.chi2.sf(statistic, df))


# --------------------------------------------------------------------------- #
# Extraction
# --------------------------------------------------------------------------- #
def _uls_objective(psi, R, k):
    Rr = R.copy()
    np.fill_diagonal(Rr, 1 - psi)
    values, vectors = linalg.eigh(Rr)
    values = np.maximum(values[::-1][:k], np.finfo(float).eps * 100)
    loadings = vectors[:, ::-1][:, :k] * np.sqrt(values)
    return np.sum((Rr - loadings @ loadings.T) ** 2)


def _uls_loadings(psi, R, k):
    Rr = R.copy()
    np.fill_diagonal(Rr, 1 - psi)
    values, vectors = np.linalg.eigh(Rr)
    values = values[::-1][:k]
    return vectors[:, ::-1][:, :k] * np.sqrt(np.maximum(values, 0))


def _ml_objective(psi, R, k):
    sc = 1 / np.sqrt(psi)
    values = np.linalg.eigvalsh(R * np.outer(sc, sc))[::-1][k:]
    return -(np.sum(np.log(values) - values) - k + R.shape[0])


def _ml_loadings(psi, R, k):
    sc = 1 / np.sqrt(psi)
    values, vectors = np.linalg.eigh(R * np.outer(sc, sc))
    values = np.maximum(values[::-1][:k] - 1, 0)
    loadings = vectors[:, ::-1][:, :k] * np.sqrt(values)
    return np.sqrt(psi)[:, None] * loadings


def extract(R: np.ndarray, n_factors: int, method: str) -> tuple[np.ndarray, bool]:
    """Unrotated loadings and whether the optimizer converged."""
    if method == "principal":
        values, vectors = np.linalg.eigh(R)
        values, vectors = values[::-1][:n_factors], vectors[:, ::-1][:, :n_factors]
        return vectors * np.sqrt(np.maximum(values, 0)), True

    # start from the uniquenesses implied by the squared multiple correlations
    start = 1 / np.diag(_inverse(R))
    start = np.clip(start, *UNIQUENESS_BOUNDS)
    objective, loadings = (
        (_ml_objective, _ml_loadings) if method == "ml" else (_uls_objective, _uls_loadings)
    )
    res = minimize(
        objective,
        start,
        method="L-BFGS-B",
        bounds=[UNIQUENESS_BOUNDS] * R.shape[0],
        options={"maxiter": 1000},
        args=(R, n_factors),
    )
    return loadings(res.x, R, n_factors), bool(res.success)


# --------------------------------------------------------------------------- #
# Rotation
# --------------------------------------------------------------------------- #
def _varimax(L, normalize=True, max_iter=500, tol=1e-5):
    X = L.copy()
    n_rows, n_cols = X.shape
    if normalize:
        h = np.sqrt((X**2).sum(axis=1))
        X = X / h[:, None]
    T = np.eye(n_cols)
    d = 0
    for _ in range(max_iter):
        old_d = d
        B = X @ T
        U, S, Vt = np.linalg.svd(X.T @ (B**3 - B * (B**2).sum(axis=0) / n_rows))
        T = U @ Vt
        d = np.sum(S)
        if d < old_d * (1 + tol):
            break
    X = X @ T
    if normalize:
        X = X * h[:, None]
    return X, T


def _promax(L, power=4):
    h = np.sqrt((L**2).sum(axis=1))
    X, _ = _varimax(L / h[:, None])
    Y = X * np.abs(X) ** (power - 1)
    coef = linalg.solve(X.T @ X, X.T @ Y)
    try:
        diag_inv = np.diag(linalg.inv(coef.T @ coef))
    except linalg.LinAlgError:
        diag_inv = np.diag(linalg.pinv(coef.T @ coef))
    coef = coef * np.sqrt(diag_inv)
    loadings = (X @ coef) * h[:, None]
    coef_inv = np.linalg.inv(coef)
    return loadings, coef_inv @ coef_inv.T


def _oblimin(L, max_iter=500, tol=1e-5):
    """Direct oblimin with gamma = 0 by gradient projection (Jennrich, 2002)."""
    k = L.shape[1]
    off_diag = 1 - np.eye(k)

    def objective(A):
        X = (A**2) @ off_diag
        return A * X, np.sum(A**2 * X) / 4

    T = np.eye(k)
    A = L @ np.linalg.inv(T).T
    grad, f = objective(A)
    G = -(A.T @ grad @ np.linalg.inv(T)).T
    alpha = 1.0
    for _ in range(max_iter + 1):
        Gp = G - T * np.sum(T * G, axis=0)
        s = np.sqrt(np.sum(Gp**2))
        if s < tol:
            break
        alpha *= 2
        for _ in range(11):
            X = T - alpha * Gp
            Tt = X / np.sqrt(np.sum(X**2, axis=0))
            A = L @ np.linalg.inv(Tt).T
            grad, ft = objective(A)
            if f - ft > 0.5 * s**2 * alpha:
                break
            alpha /= 2
        T, f = Tt, ft
        G = -(A.T @ grad @ np.linalg.inv(Tt)).T
    return A, T.T @ T


def rotate(L: np.ndarray, rotation: str | None) -> tuple[np.ndarray, np.ndarray | None]:
    """Rotated loadings and, for oblique rotations, the factor correlations."""
    if rotation is None or L.shape[1] < 2:
        return L, None
    if rotation == "varimax":
        return _varimax(L)[0], None
    if rotation == "promax":
        return _promax(L)
    if rotation == "oblimin":
        return _oblimin(L)
    raise InputError(f"Unknown rotation '{rotation}'.")


# --------------------------------------------------------------------------- #
# Full pipeline
# --------------------------------------------------------------------------- #
def kaiser_n_factors(R: np.ndarray) -> int:
    """Number of eigenvalues of R greater than 1 (at least 1)."""
    return max(1, int(np.sum(np.linalg.eigvalsh(R) > 1)))


def fit_efa(R: np.ndarray, n_factors: int, method: str, rotation: str | None) -> dict:
    """Extract, rotate and summarise.

    Factors are signed so that each loading column sums to a positive number,
    and ordered by decreasing sum of squared loadings; ``phi`` is reordered
    along with the loadings.
    """
    loadings, converged = extract(R, n_factors, method)
    loadings, phi = rotate(loadings, rotation)

    signs = np.sign(loadings.sum(axis=0))
    signs[signs == 0] = 1
    loadings = loadings * signs
    if phi is not None:
        phi = phi * np.outer(signs, signs)

    order = np.argsort(-(loadings**2).sum(axis=0), kind="stable")
    loadings = loadings[:, order]
    if phi is not None:
        phi = phi[np.ix_(order, order)]

    ss = (loadings**2).sum(axis=0)
    communalities = (loadings**2).sum(axis=1)
    return {
        "loadings": loadings,
        "phi": phi,
        "structure": loadings @ phi if phi is not None else None,
        "communalities": communalities,
        "uniquenesses": 1 - communalities,
        "ss_loadings": ss,
        "proportion_variance": ss / R.shape[0],
        "cumulative_variance": np.cumsum(ss / R.shape[0]),
        "converged": converged,
    }
