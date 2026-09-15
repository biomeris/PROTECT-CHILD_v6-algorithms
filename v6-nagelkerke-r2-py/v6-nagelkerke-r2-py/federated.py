"""
Federated Nagelkerke's R2 - COMPUTE functions (vantage6 v5 "Uluru").

Given an already-fitted logistic regression model (intercept + coefficients,
fit elsewhere - not by this algorithm), computes its Nagelkerke pseudo-R2
across federated nodes without centralizing patient-level data.

Log-likelihood is additive over independent observations, so this is exact
in a SINGLE round: each node reports 3 scalars (n, sum_y, loglik_full); the
central function sums them, derives the null-model log-likelihood in closed
form, and computes the Cox-Snell / Nagelkerke R2.

Convention: errors as {"error": msg}, never raise. Outputs JSON-serializable.
"""

from typing import Any
import numpy as np
import pandas as pd

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.action import federated, central
from vantage6.algorithm.decorator.data import dataframe
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.client import AlgorithmClient


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def _validate(df: pd.DataFrame, features: list, outcome_column: str,
              min_samples: int) -> Any:
    """Clean subset or error dict (privacy guard K=min_samples)."""
    missing = [c for c in list(features) + [outcome_column] if c not in df.columns]
    if missing:
        warn(f"Missing columns: {missing}")
        return {"error": f"missing columns: {missing}"}
    sub = df[list(features) + [outcome_column]].dropna()
    if len(sub) < min_samples:
        warn(f"Node has {len(sub)} < {min_samples} samples — skipped (privacy guard)")
        return {"error": f"insufficient samples (< {min_samples})"}
    return sub


# --------------------------------------------------------------------------
# FEDERATED function (runs on each node, sees local data)
# --------------------------------------------------------------------------
@federated
@dataframe(1)
def partial_loglik(
    df1: pd.DataFrame, features: list, outcome_column: str,
    intercept: float, coefficients: list, min_samples: int = 5,
) -> dict:
    """Local n, outcome sum, and fitted-model log-likelihood contribution."""
    info("Nagelkerke R2 — partial_loglik")
    sub = _validate(df1, features, outcome_column, min_samples)
    if isinstance(sub, dict):
        return sub
    X = sub[list(features)].to_numpy(dtype=float)
    y = sub[outcome_column].to_numpy(dtype=float)
    eta = intercept + X @ np.asarray(coefficients, dtype=float)
    p_hat = 1.0 / (1.0 + np.exp(-eta))
    p_hat = np.clip(p_hat, 1e-12, 1 - 1e-12)  # avoid log(0)
    loglik_full = float(np.sum(y * np.log(p_hat) + (1 - y) * np.log(1 - p_hat)))
    return {
        "n": int(len(sub)),
        "sum_y": float(y.sum()),
        "loglik_full": loglik_full,
    }


# --------------------------------------------------------------------------
# CENTRAL function (orchestrates, does not see data)
# --------------------------------------------------------------------------
@central
@algorithm_client
def central(
    client: AlgorithmClient,
    features: list,
    outcome_column: str,
    intercept: float,
    coefficients: list,
    min_samples: int = 5,
) -> dict:
    """Coordinator for the federated Nagelkerke R2 (single round)."""
    org_ids = [o["id"] for o in client.organization.list()]
    info(f"Federated Nagelkerke R2 | {len(org_ids)} nodes | {len(features)} features")

    task = client.task.create(
        method="partial_loglik",
        arguments={
            "features": features,
            "outcome_column": outcome_column,
            "intercept": intercept,
            "coefficients": coefficients,
            "min_samples": min_samples,
        },
        organizations=org_ids,
        name="Nagelkerke R2 (partial log-likelihoods)",
    )
    results = [r for r in client.wait_for_results(task_id=task.get("id")) if "error" not in r]
    if not results:
        return {"error": "no valid nodes"}

    n_total = sum(r["n"] for r in results)
    sum_y = sum(r["sum_y"] for r in results)
    loglik_full = sum(r["loglik_full"] for r in results)

    p_bar = sum_y / n_total
    if p_bar <= 0.0 or p_bar >= 1.0:
        return {"error": "degenerate outcome: global event rate is 0 or 1"}
    loglik_null = n_total * (p_bar * np.log(p_bar) + (1 - p_bar) * np.log(1 - p_bar))

    r2_cox_snell = 1 - np.exp((2.0 / n_total) * (loglik_null - loglik_full))
    r2_nagelkerke = r2_cox_snell / (1 - np.exp((2.0 / n_total) * loglik_null))

    info(f"Nagelkerke R2 done | n_total={n_total} | R2_N={r2_nagelkerke:.4f}")
    return {
        "n_total": int(n_total),
        "n_nodes": len(results),
        "p_bar": float(p_bar),
        "loglik_null": float(loglik_null),
        "loglik_full": float(loglik_full),
        "r2_cox_snell": float(r2_cox_snell),
        "r2_nagelkerke": float(r2_nagelkerke),
    }
