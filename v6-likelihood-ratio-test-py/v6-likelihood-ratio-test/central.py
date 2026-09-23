"""
Federated likelihood ratio test central orchestration.

Round 0: collect per-covariate moments (no values, no outcome labels) to
         standardize the design matrix and check there is enough data.
Rounds 1..k: fit the full logistic regression model via federated
         Newton-Raphson / IRLS — each round the nodes evaluate the score and
         Fisher information at the current coefficients and return only those
         aggregates; the central server takes the Newton step and repeats until
         convergence.
Round k+1: collect the full model's log-likelihood at its converged
         coefficients.
Then, for every covariate: refit a reduced model (that covariate dropped, all
         others kept) the same way — warm-started from the full model's
         coefficients — collect its log-likelihood, and compute
         LR = -2 (logL_reduced - logL_full) ~ chi2(1).

Because the score and Fisher information at any coefficient vector are sums
over records, summing each node's contribution gives exactly the value a
centralized fit would — federated Newton-Raphson is exact here, not an
approximation. No individual record, score, or information matrix from a
single record is ever transmitted; only totals over each node's eligible
sample, gated by the minimum-record and minimum-group-size thresholds in
``partial.py``.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np

from vantage6.algorithm.client import AlgorithmClient
from vantage6.algorithm.tools.decorators import algorithm_client
from vantage6.algorithm.tools.exceptions import UserInputError
from vantage6.algorithm.tools.util import get_env_var, info

from .globals import (
    LIKELIHOOD_RATIO_TEST_MAX_ITERATIONS,
    LIKELIHOOD_RATIO_TEST_TOLERANCE,
)
from .model import (
    aggregate_score_information,
    has_converged,
    lrt_result,
    newton_step,
    pool_moments,
    warm_start,
)


def _fit_model(
    client: AlgorithmClient,
    organizations_to_include: List[int],
    outcome_col: str,
    covariates: List[str],
    model_covariates: List[str],
    means: Dict[str, float],
    sds: Dict[str, float],
    positive_label: Optional[object],
    beta_start: np.ndarray,
    max_iterations: int,
    tolerance: float,
    label: str,
) -> Tuple[np.ndarray, int, bool]:
    """Run federated Newton-Raphson to convergence for one covariate set."""
    n_params = len(model_covariates) + 1
    beta = beta_start.copy()
    converged = False
    iterations_used = 0

    for iteration in range(1, max_iterations + 1):
        task = client.task.create(
            input_={
                "method": "partial_score_information",
                "kwargs": {
                    "outcome_col": outcome_col,
                    "covariates": covariates,
                    "model_covariates": model_covariates,
                    "means": means,
                    "sds": sds,
                    "beta": beta.tolist(),
                    "positive_label": positive_label,
                },
            },
            organizations=organizations_to_include,
            name=f"LRT {label}: Newton-Raphson iteration {iteration}",
            description=f"Score and Fisher information at the current coefficients ({label}).",
        )
        results = client.wait_for_results(task_id=task.get("id"))
        if not results:
            raise UserInputError(f"No results received while fitting the {label} model.")

        aggregated = aggregate_score_information(results, n_params)
        if aggregated is None:
            raise UserInputError(
                f"No node contributed to the {label} model. Check the minimum-record "
                "and minimum-group-size thresholds."
            )
        score, information, n_total, n_contributors = aggregated

        try:
            delta = newton_step(score, information)
        except ValueError as exc:
            raise UserInputError(f"Failed to fit the {label} model: {exc}") from exc

        beta = beta + delta
        iterations_used = iteration
        if has_converged(score, delta, tol_score=tolerance):
            converged = True
            break

    return beta, iterations_used, converged


def _log_likelihood(
    client: AlgorithmClient,
    organizations_to_include: List[int],
    outcome_col: str,
    covariates: List[str],
    model_covariates: List[str],
    means: Dict[str, float],
    sds: Dict[str, float],
    positive_label: Optional[object],
    beta: np.ndarray,
    label: str,
) -> float:
    """One round: the summed log-likelihood at the given coefficients."""
    task = client.task.create(
        input_={
            "method": "partial_log_likelihood",
            "kwargs": {
                "outcome_col": outcome_col,
                "covariates": covariates,
                "model_covariates": model_covariates,
                "means": means,
                "sds": sds,
                "beta": beta.tolist(),
                "positive_label": positive_label,
            },
        },
        organizations=organizations_to_include,
        name=f"LRT {label}: log-likelihood",
        description=f"Log-likelihood at the converged coefficients ({label}).",
    )
    results = client.wait_for_results(task_id=task.get("id"))
    if not results:
        raise UserInputError(f"No results received computing the {label} log-likelihood.")

    total = 0.0
    n_contributors = 0
    for result in results:
        if not result:
            continue
        total += float(result["log_likelihood"])
        n_contributors += 1
    if n_contributors == 0:
        raise UserInputError(f"No node contributed to the {label} log-likelihood.")
    return total


@algorithm_client
def central(
    client: AlgorithmClient,
    organizations_to_include: List[int],
    outcome_col: str,
    covariates: List[str],
    positive_label: Optional[object] = None,
    max_iterations: int = LIKELIHOOD_RATIO_TEST_MAX_ITERATIONS,
    tolerance: float = LIKELIHOOD_RATIO_TEST_TOLERANCE,
) -> Dict[str, Dict]:
    """
    Federated likelihood ratio test for a logistic regression model.

    Fits a full logistic model of ``outcome_col`` on all of ``covariates``, then
    for every covariate fits the reduced model with just that covariate dropped,
    and reports the likelihood ratio test of its significance:
    ``LR = -2 (logL_reduced - logL_full) ~ chi2(1)``.

    Parameters
    ----------
    organizations_to_include : list[int]
        IDs of the organizations that participate in the computation.
    outcome_col : str
        Binary column defining the event / non-event outcome.
    covariates : list[str]
        Numeric predictor columns for the full model. Categorical predictors
        must be encoded as numeric dummy columns beforehand.
    positive_label
        The value of ``outcome_col`` that denotes the "event"/positive outcome.
        Required unless the column is boolean or coded ``{0, 1}``.
    max_iterations : int
        Safety cap on Newton-Raphson iterations per model fit. Default 25.
    tolerance : float
        Convergence tolerance on the score (log-likelihood gradient), in
        standardized covariate units. Default ``1e-6``.

    Returns
    -------
    dict
        Maps each covariate to ``{"lr_statistic", "degrees_of_freedom",
        "p_value", "log_likelihood_full", "log_likelihood_reduced",
        "n_iterations_reduced", "converged_reduced"}``, plus the shared full-model
        context ``"log_likelihood_full"``, ``"n_total"``, ``"n_event"``,
        ``"n_non_event"``, ``"n_iterations_full"``, ``"converged_full"``.
    """
    if not organizations_to_include:
        raise UserInputError("Provide at least one organization id.")
    if not covariates:
        raise UserInputError("Provide at least one covariate.")
    if len(set(covariates)) != len(covariates):
        raise UserInputError(f"Duplicate covariates: {covariates}.")
    if outcome_col in covariates:
        raise UserInputError("outcome_col must not also appear in covariates.")
    if max_iterations < 1:
        raise UserInputError(f"max_iterations must be >= 1, got {max_iterations}.")

    info(f"Federated LRT, outcome_col='{outcome_col}', covariates={covariates}.")

    # ── Round 0: per-covariate moments, no values and no outcome labels ──────
    task0 = client.task.create(
        input_={
            "method": "partial_moments",
            "kwargs": {
                "outcome_col": outcome_col,
                "covariates": covariates,
                "positive_label": positive_label,
            },
        },
        organizations=organizations_to_include,
        name="LRT round 0: covariate moments",
        description="Collect n, n_event, n_non_event and per-covariate sum/sum_sq to standardize the model.",
    )
    info("Waiting for round-0 results.")
    r0_results = client.wait_for_results(task_id=task0.get("id"))
    if not r0_results:
        raise UserInputError("No round-0 results received.")

    moments = pool_moments(r0_results, covariates)
    if moments is None:
        raise UserInputError(
            "No node met the minimum-record and minimum-group-size thresholds "
            "for this outcome/covariate combination."
        )

    n_params = len(covariates) + 1
    if moments["n_event"] <= n_params or moments["n_non_event"] <= n_params:
        raise UserInputError(
            f"Too few events ({moments['n_event']}) or non-events "
            f"({moments['n_non_event']}) to fit {n_params} parameters reliably."
        )
    zero_variance = [c for c in covariates if moments["sds"][c] <= 0]
    if zero_variance:
        raise UserInputError(
            f"Covariates {zero_variance} have zero variance across all "
            "contributing nodes and cannot be used as predictors."
        )

    means, sds = moments["means"], moments["sds"]

    # ── Fit the full model ────────────────────────────────────────────────────
    info(f"Fitting the full model ({len(covariates)} covariates).")
    beta_full, iters_full, converged_full = _fit_model(
        client, organizations_to_include, outcome_col, covariates, covariates,
        means, sds, positive_label, np.zeros(n_params), max_iterations, tolerance,
        label="full",
    )
    if not converged_full:
        raise UserInputError(
            f"The full model did not converge within {max_iterations} iterations. "
            "This often indicates separation or near-collinearity among the covariates."
        )
    logl_full = _log_likelihood(
        client, organizations_to_include, outcome_col, covariates, covariates,
        means, sds, positive_label, beta_full, label="full",
    )
    info(f"Full model converged in {iters_full} iterations, logL={logl_full:.4f}.")

    # ── Fit each reduced (drop-one) model, warm-started from the full fit ────
    out: Dict[str, Dict] = {}
    for index, dropped in enumerate(covariates):
        reduced_covariates = [c for c in covariates if c != dropped]
        beta_start = warm_start(beta_full, index + 1)  # +1 to skip the intercept

        info(f"Fitting the reduced model dropping '{dropped}'.")
        beta_reduced, iters_reduced, converged_reduced = _fit_model(
            client, organizations_to_include, outcome_col, covariates,
            reduced_covariates, means, sds, positive_label, beta_start,
            max_iterations, tolerance, label=f"reduced (drop {dropped})",
        )
        if not converged_reduced:
            raise UserInputError(
                f"The reduced model dropping '{dropped}' did not converge within "
                f"{max_iterations} iterations."
            )
        logl_reduced = _log_likelihood(
            client, organizations_to_include, outcome_col, covariates,
            reduced_covariates, means, sds, positive_label, beta_reduced,
            label=f"reduced (drop {dropped})",
        )

        result = lrt_result(logl_full, logl_reduced, df=1)
        result.update(
            {
                "n_iterations_reduced": iters_reduced,
                "converged_reduced": converged_reduced,
                "n_total": moments["n_total"],
                "n_event": moments["n_event"],
                "n_non_event": moments["n_non_event"],
                "n_iterations_full": iters_full,
                "converged_full": converged_full,
            }
        )
        out[dropped] = result
        info(f"  {dropped}: LR={result['lr_statistic']:.4f}, p={result['p_value']:.3e}.")

    info("Federated likelihood ratio test finished.")
    return out
