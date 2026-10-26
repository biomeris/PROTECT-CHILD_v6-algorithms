"""Federated coordinator for AIC of a previously fitted common model.

The central function ONLY aggregates local log-likelihood contributions and
applies the information criterion. It never fits the underlying model.
"""
from __future__ import annotations

import math
from typing import Any
from vantage6.algorithm.client import AlgorithmClient
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from .metrics import compute_aic


@central
@algorithm_client
def central_function(
    client: AlgorithmClient,
    n_parameters: int,
    input_mode: str = "contributions",
    log_likelihood_column: str = "log_likelihood",
    count_column: str = "n_observations",
    label_column: str = "y_true",
    probability_column: str = "p_positive",
    model_id_column: str | None = None,
    organizations_to_include: list[int] | None = None,
) -> dict[str, Any]:
    """Evaluate AIC from additive log-likelihood (no training step).

    Three options for input_mode:
      contributions: a log-likelihood contribution per observation
      aggregated:    one or more local sums with associated observation counts
      binary:        0/1 labels and probabilities from the SAME fitted classifier

    n_parameters must count ALL parameters of the global fitted model,
    including intercept and dispersion parameters when applicable.
    """
    if isinstance(n_parameters, bool) or not isinstance(n_parameters, int) or n_parameters < 0:
        return {"error": "n_parameters must be a nonnegative integer."}
    if input_mode not in ("contributions", "aggregated", "binary"):
        return {"error": "input_mode must be contributions, aggregated or binary."}
    available = [org["id"] for org in client.organization.list()]
    selected = available if organizations_to_include is None else list(organizations_to_include)
    if not selected or len(set(selected)) != len(selected) or not set(selected).issubset(set(available)):
        return {"error": "Select a nonempty list of distinct organizations in the collaboration."}

    task = client.task.create(
        method="federated_function",
        arguments={
            "input_mode": input_mode,
            "log_likelihood_column": log_likelihood_column,
            "count_column": count_column,
            "label_column": label_column,
            "probability_column": probability_column,
            "model_id_column": model_id_column,
        },
        organizations=selected,
        name="Federated AIC log-likelihood contributions",
        description="Aggregate local fitted-model log-likelihood without sharing records",
    )
    replies = client.wait_for_results(task_id=task["id"])
    if not isinstance(replies, list) or len(replies) != len(selected):
        return {"error": "Expected exactly one local result for each selected organization."}

    likelihoods: list[float] = []
    total_n = 0
    ids: set[str] = set()
    for index, reply in enumerate(replies):
        if not isinstance(reply, dict):
            return {"error": f"Invalid result from organization at position {index}."}
        if "error" in reply:
            return {"error": f"Organization at position {index}: {reply['error']}"}
        if reply.get("input_mode") != input_mode:
            return {"error": f"Input mode mismatch at organization position {index}."}
        n = reply.get("n_observations")
        if isinstance(n, bool) or not isinstance(n, int) or n < 1:
            return {"error": f"Invalid observation count at organization position {index}."}
        try:
            value = float(reply["log_likelihood"])
        except (KeyError, TypeError, ValueError, OverflowError):
            return {"error": f"Invalid log-likelihood at organization position {index}."}
        if not math.isfinite(value):
            return {"error": f"Non-finite log-likelihood at organization position {index}."}
        total_n += n
        likelihoods.append(value)
        if model_id_column is not None:
            ident = reply.get("model_id")
            if not isinstance(ident, str) or not ident.strip():
                return {"error": f"Missing model identifier at organization position {index}."}
            ids.add(ident)
    if model_id_column is not None and len(ids) != 1:
        return {"error": "Participating nodes report different fitted model identifiers."}

    try:
        global_log_likelihood = math.fsum(likelihoods)
        score = compute_aic(global_log_likelihood, n_parameters)
        if not math.isfinite(score):
            raise ValueError("AIC result is not finite")
    except (ValueError, OverflowError) as exc:
        return {"error": str(exc)}

    return {
        "criterion": "AIC",
        "aic": score,
        "log_likelihood": global_log_likelihood,
        "n_parameters": n_parameters,
        "n_observations": total_n,
        "input_mode": input_mode,
        "model_id": next(iter(ids)) if ids else None,
        "organizations_included": selected,
        "model_fitted_by_algorithm": False,
    }
