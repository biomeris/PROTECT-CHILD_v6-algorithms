"""
Pure arithmetic for federated logistic regression via Newton-Raphson (IRLS) and
the resulting likelihood ratio test. No vantage6 imports, no data access — every
function here operates on aggregates already collected from the nodes (record
counts, sums, gradients, Fisher information matrices), never on raw records.

Federated Newton-Raphson for logistic regression is exact, not an approximation:
the gradient (score) and the Fisher information at a given coefficient vector
are both sums over records, so summing each node's local score/information gives
exactly the same result as computing them centrally over the pooled data. Only
the coefficients themselves are ever this module's numeric output; a node's
individual records are never recoverable from a score vector or an information
matrix once summed over a large enough group (see the minimum-group-size checks
in ``partial.py``).
"""
from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
from scipy.stats import chi2


def sigmoid(eta: np.ndarray) -> np.ndarray:
    """Logistic function, clipped to avoid overflow for extreme linear predictors."""
    return 1.0 / (1.0 + np.exp(-np.clip(eta, -35.0, 35.0)))


def pool_moments(
    node_results: Sequence[Optional[Dict]], covariates: Sequence[str]
) -> Optional[Dict[str, object]]:
    """
    Pool per-node ``n``/``n_event``/``n_non_event``/``sums``/``sum_sqs`` into
    global record counts and, per covariate, a mean and standard deviation used
    only to standardize the design matrix for numerical stability. Standardizing
    does not change a logistic model's log-likelihood at its optimum — it is a
    reparameterization of the linear predictor — so it never affects the
    likelihood ratio statistic, only how many Newton iterations it takes to get
    there.
    """
    n_total = 0
    n_event = 0
    n_non_event = 0
    sums = {c: 0.0 for c in covariates}
    sum_sqs = {c: 0.0 for c in covariates}
    n_contributors = 0

    for result in node_results:
        if not result:
            continue
        n = int(result.get("n", 0))
        if n <= 0:
            continue
        n_total += n
        n_event += int(result["n_event"])
        n_non_event += int(result["n_non_event"])
        n_contributors += 1
        for c in covariates:
            sums[c] += float(result["sums"][c])
            sum_sqs[c] += float(result["sum_sqs"][c])

    if n_total == 0 or n_contributors == 0:
        return None

    means: Dict[str, float] = {}
    sds: Dict[str, float] = {}
    for c in covariates:
        mean = sums[c] / n_total
        variance = max(sum_sqs[c] / n_total - mean * mean, 0.0)
        means[c] = mean
        sds[c] = float(np.sqrt(variance))

    return {
        "n_total": n_total,
        "n_event": n_event,
        "n_non_event": n_non_event,
        "means": means,
        "sds": sds,
        "n_contributors": n_contributors,
    }


def aggregate_score_information(
    node_results: Sequence[Optional[Dict]], n_params: int
) -> Optional[Tuple[np.ndarray, np.ndarray, int, int]]:
    """Sum per-node score vectors and Fisher information matrices."""
    score = np.zeros(n_params)
    information = np.zeros((n_params, n_params))
    n_total = 0
    n_contributors = 0

    for result in node_results:
        if not result:
            continue
        score += np.asarray(result["score"], dtype=float)
        information += np.asarray(result["information"], dtype=float)
        n_total += int(result["n"])
        n_contributors += 1

    if n_contributors == 0:
        return None
    return score, information, n_total, n_contributors


def newton_step(score: np.ndarray, information: np.ndarray) -> np.ndarray:
    """
    Solve ``information @ delta = score`` for the Newton-Raphson update.

    ``information`` is the (positive semi-definite) Fisher information matrix
    ``X^T W X``; ``score`` is the log-likelihood gradient ``X^T (y - p)``. The
    update ``beta_new = beta_old + delta`` maximizes the log-likelihood.
    """
    try:
        return np.linalg.solve(information, score)
    except np.linalg.LinAlgError as exc:
        raise ValueError(
            "The Fisher information matrix is singular. This usually means "
            "the covariates are collinear, or the outcome is (quasi-)perfectly "
            "separated by the covariates."
        ) from exc


def has_converged(
    score: np.ndarray,
    delta: np.ndarray,
    tol_score: float = 1e-6,
    tol_delta: float = 1e-8,
) -> bool:
    """True once the gradient is flat or the last step was negligible."""
    return bool(
        np.max(np.abs(score)) < tol_score or np.max(np.abs(delta)) < tol_delta
    )


def warm_start(beta_full: np.ndarray, dropped_index: int) -> np.ndarray:
    """
    Starting coefficients for a reduced model: the full model's converged
    coefficients with the dropped covariate's entry removed.

    Because Newton-Raphson converges quadratically near the optimum, and the
    reduced model's optimum is typically close to the full model's coefficients
    restricted to the remaining covariates, this routinely halves the number of
    iterations compared to starting from zero.
    """
    return np.delete(beta_full, dropped_index)


def lrt_result(
    logl_full: float, logl_reduced: float, df: int = 1
) -> Dict[str, object]:
    """
    Likelihood ratio test: ``LR = -2 (logL_reduced - logL_full) ~ chi2(df)``.

    The full model nests the reduced model (setting the dropped coefficient(s)
    to zero recovers it exactly), so at the true MLE ``logL_full >=
    logL_reduced`` always holds; a tiny negative value can appear only from
    finite optimizer tolerance and is clipped to zero.
    """
    raw_lr = -2.0 * (logl_reduced - logl_full)
    lr = max(raw_lr, 0.0)
    return {
        "lr_statistic": float(lr),
        "degrees_of_freedom": int(df),
        "p_value": float(chi2.sf(lr, df=df)),
        "log_likelihood_full": float(logl_full),
        "log_likelihood_reduced": float(logl_reduced),
    }
