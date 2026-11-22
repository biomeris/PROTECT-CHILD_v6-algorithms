"""
Pure MANOVA math, free of vantage6 decorators so it can be unit tested directly
(see ``test/test_manova.py``).

Node side
    ``local_summary``  -> per group g: n_g, sum_g (p-vector), XtX_g (p x p)
Central side
    ``combine``        -> per-group statistics summed over all nodes
    ``sscp_matrices``  -> W (within-group / error SSCP), B (between-group SSCP)
    ``manova_tests``   -> Wilks, Pillai, Hotelling-Lawley, Roy + F approximations

With N = sum n_g, grand sum S = sum sum_g and Q = sum XtX_g:

    W = Q - sum_g sum_g sum_g^T / n_g
    B = sum_g sum_g sum_g^T / n_g - S S^T / N

so the federated result equals a pooled one-way MANOVA up to round-off.
"""

import numbers

import numpy as np
import pandas as pd
from scipy import linalg, stats

from vantage6.algorithm.tools.exceptions import InputError

N_THRESHOLD_FLAG = "N-Threshold not met"

TEST_NAMES = [
    "Wilks' lambda",
    "Pillai's trace",
    "Hotelling-Lawley trace",
    "Roy's greatest root",
]


def normalize_label(value) -> str:
    """Stringify a group label consistently across nodes.

    A column that holds integers at one node may be read as float at another
    (e.g. when it contains missing values), so 1 and 1.0 both become "1".
    """
    if isinstance(value, numbers.Real) and not isinstance(value, bool):
        if float(value).is_integer():
            return str(int(value))
    return str(value)


def validate_columns(
    df: pd.DataFrame, group_column: str, outcome_columns: list[str]
) -> None:
    missing = [c for c in [group_column, *outcome_columns] if c not in df.columns]
    if missing:
        raise InputError(f"Column(s) {missing} not found in DataFrame.")
    for col in outcome_columns:
        if pd.api.types.is_bool_dtype(df[col]) or not pd.api.types.is_numeric_dtype(
            df[col]
        ):
            raise InputError(
                f"Outcome column '{col}' must be numeric, got dtype '{df[col].dtype}'."
            )


def local_summary(
    df: pd.DataFrame,
    group_column: str,
    outcome_columns: list[str],
    minimum_records: int,
    minimum_group_size: int,
) -> dict:
    """Per-group sufficient statistics at one node.

    Rows with a missing group or outcome are dropped (listwise deletion). Groups
    with fewer than ``minimum_group_size`` complete cases are suppressed. If the
    remaining number of records is ``minimum_records`` or fewer, nothing is
    shared and the node flags itself instead.
    """
    validate_columns(df, group_column, outcome_columns)

    data = df[[group_column, *outcome_columns]].dropna()
    labels = data[group_column].map(normalize_label)

    sizes = labels.value_counts()
    suppressed = sorted(sizes[sizes < minimum_group_size].index)
    keep = ~labels.isin(suppressed)
    data, labels = data[keep], labels[keep]

    if len(data) <= minimum_records:
        return {N_THRESHOLD_FLAG: True}

    groups = {}
    for label, idx in labels.groupby(labels, sort=True).groups.items():
        X = data.loc[idx, outcome_columns].to_numpy(dtype=float)
        groups[label] = {
            "n": int(X.shape[0]),
            "sum": X.sum(axis=0).tolist(),
            "xtx": (X.T @ X).tolist(),
        }

    return {
        "outcome_columns": list(outcome_columns),
        "groups": groups,
        "suppressed_groups": suppressed,
    }


def combine(summaries: list[dict]) -> dict:
    """Add up per-group statistics across nodes (a group may be absent at a node)."""
    columns = summaries[0]["outcome_columns"]
    for s in summaries[1:]:
        if s["outcome_columns"] != columns:
            raise InputError("Nodes returned different outcome columns.")

    p = len(columns)
    pooled: dict[str, dict] = {}
    for s in summaries:
        for label, g in s["groups"].items():
            acc = pooled.setdefault(
                label, {"n": 0, "sum": np.zeros(p), "xtx": np.zeros((p, p))}
            )
            acc["n"] += g["n"]
            acc["sum"] += np.asarray(g["sum"], dtype=float)
            acc["xtx"] += np.asarray(g["xtx"], dtype=float)
    return {"outcome_columns": columns, "groups": dict(sorted(pooled.items()))}


def sscp_matrices(pooled: dict) -> tuple[np.ndarray, np.ndarray, int, int]:
    """Return (W, B, N, k) from pooled per-group statistics."""
    groups = [g for g in pooled["groups"].values() if g["n"] > 0]
    k = len(groups)
    N = sum(g["n"] for g in groups)
    S = sum(g["sum"] for g in groups)
    Q = sum(g["xtx"] for g in groups)
    between_part = sum(np.outer(g["sum"], g["sum"]) / g["n"] for g in groups)

    W = Q - between_part
    B = between_part - np.outer(S, S) / N
    # symmetrise against round-off
    return (W + W.T) / 2, (B + B.T) / 2, N, k


def manova_tests(W: np.ndarray, B: np.ndarray, N: int, k: int) -> pd.DataFrame:
    """Four multivariate test statistics with the SAS / statsmodels F approximations.

    Returns a DataFrame indexed by test name with columns
    ``Value, Num DF, Den DF, F Value, Pr > F`` (the layout of statsmodels'
    ``MANOVA.mv_test()``).
    """
    p = W.shape[0]
    q = k - 1  # hypothesis df
    v = N - k  # error df
    if k < 2:
        raise InputError(f"MANOVA needs at least 2 groups, found {k}.")
    if v < p:
        raise InputError(
            f"Error degrees of freedom (N - k = {v}) must be at least the number "
            f"of outcome columns ({p})."
        )
    try:
        np.linalg.cholesky(W)
    except np.linalg.LinAlgError:
        raise InputError(
            "The within-group SSCP matrix is singular: an outcome is constant "
            "within groups or the outcomes are linearly dependent."
        ) from None

    # eigenvalues of W^{-1} B via the symmetric generalized problem B x = lam W x
    lam = linalg.eigh(B, W, eigvals_only=True)
    lam = np.clip(lam, 0, None)[::-1]

    s = min(p, q)
    m = (abs(p - q) - 1) / 2
    n = (v - p - 1) / 2

    wilks = float(np.prod(1 / (1 + lam)))
    pillai = float(np.sum(lam / (1 + lam)))
    hotelling = float(np.sum(lam))
    roy = float(lam[0])

    rows = {}

    # Wilks' lambda (Rao's F)
    r = v - (p - q + 1) / 2
    u = (p * q - 2) / 4
    t = np.sqrt((p**2 * q**2 - 4) / (p**2 + q**2 - 5)) if p**2 + q**2 - 5 > 0 else 1.0
    df1, df2 = p * q, r * t - 2 * u
    lt = wilks ** (1 / t)
    rows["Wilks' lambda"] = (wilks, df1, df2, (1 - lt) / lt * df2 / df1)

    # Pillai's trace
    df1, df2 = s * (2 * m + s + 1), s * (2 * n + s + 1)
    rows["Pillai's trace"] = (pillai, df1, df2, df2 / df1 * pillai / (s - pillai))

    # Hotelling-Lawley trace
    if n > 0:
        b = (p + 2 * n) * (q + 2 * n) / 2 / (2 * n + 1) / (n - 1)
        df1 = p * q
        df2 = 4 + (p * q + 2) / (b - 1)
        c = (df2 - 2) / 2 / n
        F = df2 / df1 * hotelling / c
    else:
        df1, df2 = s * (2 * m + s + 1), s * (s * n + 1)
        F = df2 / df1 / s * hotelling
    rows["Hotelling-Lawley trace"] = (hotelling, df1, df2, F)

    # Roy's greatest root (an upper bound on F)
    rr = max(p, q)
    df1, df2 = rr, v - rr + q
    rows["Roy's greatest root"] = (roy, df1, df2, df2 / df1 * roy)

    out = pd.DataFrame.from_dict(
        rows, orient="index", columns=["Value", "Num DF", "Den DF", "F Value"]
    )
    out["Pr > F"] = stats.f.sf(out["F Value"], out["Num DF"], out["Den DF"])
    return out.loc[TEST_NAMES]
