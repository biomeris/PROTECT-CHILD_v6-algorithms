"""
Node-side functions for the federated likelihood ratio test algorithm.

None returns an individual record, and none returns a value that can be traced
back to one:

``partial_moments``
    returns only ``n``, ``n_event``, ``n_non_event``, and per-covariate
    ``sum``/``sum of squares``. No minimum, no maximum, no quantile, no outcome
    label attached to any value.

``partial_score_information``
    returns the *score* (log-likelihood gradient) and *Fisher information*
    matrix at the coefficients the central server is currently evaluating —
    both are sums over every eligible record at the node, the standard
    quantities exchanged in federated Newton-Raphson / IRLS fitting.

``partial_log_likelihood``
    returns a single number: the sum of the per-record log-likelihood
    contributions at the final coefficients.

All three analyse exactly the same records: rows with a non-null outcome and a
non-null value in *every* covariate of the full covariate list (not just the
ones used in a particular reduced model) — so the full model and every reduced
model are fit, and their log-likelihoods compared, on an identical sample, and
that sample must clear ``LIKELIHOOD_RATIO_TEST_MINIMUM_NUMBER_OF_RECORDS`` and
``LIKELIHOOD_RATIO_TEST_MINIMUM_GROUP_SIZE`` before a node contributes anything.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pandas.api.types as ptypes

from vantage6.algorithm.tools.decorators import data
from vantage6.algorithm.tools.exceptions import InputError
from vantage6.algorithm.tools.util import get_env_var, info

from .globals import (
    LIKELIHOOD_RATIO_TEST_MINIMUM_GROUP_SIZE,
    LIKELIHOOD_RATIO_TEST_MINIMUM_NUMBER_OF_RECORDS,
)
from .model import sigmoid


def _minimum_records() -> int:
    return get_env_var(
        "LIKELIHOOD_RATIO_TEST_MINIMUM_NUMBER_OF_RECORDS",
        LIKELIHOOD_RATIO_TEST_MINIMUM_NUMBER_OF_RECORDS,
        as_type="int",
    )


def _minimum_group_size() -> int:
    return get_env_var(
        "LIKELIHOOD_RATIO_TEST_MINIMUM_GROUP_SIZE",
        LIKELIHOOD_RATIO_TEST_MINIMUM_GROUP_SIZE,
        as_type="int",
    )


def _validate_covariates(df: pd.DataFrame, outcome_col: str, covariates: List[str]) -> None:
    if outcome_col not in df.columns:
        raise InputError(f"Outcome column '{outcome_col}' not found in dataframe.")
    if not covariates:
        raise InputError("Provide at least one covariate.")
    if len(set(covariates)) != len(covariates):
        raise InputError(f"Duplicate covariates: {covariates}.")
    if outcome_col in covariates:
        raise InputError("outcome_col must not also appear in covariates.")
    non_existing = [c for c in covariates if c not in df.columns]
    if non_existing:
        raise InputError(f"Covariates {non_existing} do not exist in the dataframe.")
    non_numeric = [c for c in covariates if not ptypes.is_numeric_dtype(df[c])]
    if non_numeric:
        raise InputError(
            f"Covariates {non_numeric} are not numeric. Encode categorical "
            "covariates as numeric dummy columns before calling this algorithm."
        )


def _resolve_positive_label(series: pd.Series, positive_label: Optional[object]):
    """
    Decide which raw outcome value denotes the "event"/positive group.

    Resolved node-locally with no coordination round, so it must be
    deterministic given only this node's data. If ``positive_label`` is
    supplied explicitly it is used as-is; otherwise only the unambiguous
    conventions (booleans -> ``True``, a 0/1 column -> ``1``) are inferred.
    Anything else raises rather than guess, since a wrong guess silently
    reverses the sign of every coefficient.
    """
    if positive_label is not None:
        return positive_label
    if ptypes.is_bool_dtype(series):
        return True
    if ptypes.is_numeric_dtype(series):
        distinct = set(series.dropna().unique().tolist())
        if distinct and distinct <= {0, 1}:
            return 1
    raise InputError(
        "Cannot infer the event/positive outcome automatically from column "
        f"'{series.name}'. Pass positive_label explicitly (e.g. positive_label=1)."
    )


def _eligible_rows(
    df: pd.DataFrame,
    outcome_col: str,
    covariates: List[str],
    positive_label: Optional[object],
) -> Optional[Tuple[pd.DataFrame, np.ndarray]]:
    """
    The complete-case sample used by every model fit for this call: outcome and
    every covariate in the full list present, and the node's minimum-record and
    minimum-group-size thresholds cleared. Returns ``None`` if the node should
    not contribute at all.
    """
    mask = df[outcome_col].notna()
    for c in covariates:
        mask &= df[c].notna()
    eligible = df.loc[mask]

    if len(eligible) <= _minimum_records():
        return None

    positive = _resolve_positive_label(eligible[outcome_col], positive_label)
    is_event = (eligible[outcome_col] == positive).to_numpy()

    minimum_group_size = _minimum_group_size()
    if is_event.sum() < minimum_group_size or (~is_event).sum() < minimum_group_size:
        return None

    return eligible, is_event


def _design_matrix(
    eligible: pd.DataFrame,
    model_covariates: List[str],
    means: Dict[str, float],
    sds: Dict[str, float],
) -> np.ndarray:
    """Intercept column + standardized covariate columns, in ``model_covariates`` order."""
    n = len(eligible)
    columns = [np.ones(n)]
    for c in model_covariates:
        sd = sds[c]
        if sd <= 0:
            raise InputError(f"Covariate '{c}' has zero variance; it cannot be standardized.")
        columns.append((eligible[c].to_numpy(dtype=float) - means[c]) / sd)
    return np.column_stack(columns)


@data(1)
def partial_moments(
    df: pd.DataFrame,
    outcome_col: str,
    covariates: List[str],
    positive_label: Optional[object] = None,
) -> Dict[str, object]:
    """
    Round 0: per-covariate count, sum and sum of squares over the complete-case
    sample — nothing else. Used by the central server to standardize the design
    matrix and to confirm there are enough records and events to fit the model.

    Returns
    -------
    dict
        ``{"n", "n_event", "n_non_event", "sums": {cov: float}, "sum_sqs": {cov: float}}``,
        or ``{}`` if this node does not meet the minimum thresholds.
    """
    _validate_covariates(df, outcome_col, covariates)
    info("LRT round 0: computing per-covariate moments (no values leave the node).")

    result = _eligible_rows(df, outcome_col, covariates, positive_label)
    if result is None:
        info("LRT round 0: node suppressed (too few eligible records or events).")
        return {}
    eligible, is_event = result

    return {
        "n": int(len(eligible)),
        "n_event": int(is_event.sum()),
        "n_non_event": int((~is_event).sum()),
        "sums": {c: float(eligible[c].sum()) for c in covariates},
        "sum_sqs": {c: float((eligible[c] ** 2).sum()) for c in covariates},
    }


@data(1)
def partial_score_information(
    df: pd.DataFrame,
    outcome_col: str,
    covariates: List[str],
    model_covariates: List[str],
    means: Dict[str, float],
    sds: Dict[str, float],
    beta: List[float],
    positive_label: Optional[object] = None,
) -> Dict[str, object]:
    """
    One Newton-Raphson round: the log-likelihood score and Fisher information
    at the coefficients ``beta``, for the model using ``model_covariates`` as
    predictors (a subset of ``covariates`` for a reduced model, or all of them
    for the full model). Both are sums over every eligible record — never a
    record's own contribution in isolation.

    Returns
    -------
    dict
        ``{"score": [float, ...], "information": [[float, ...], ...], "n": int}``,
        or ``{}`` if this node does not contribute.
    """
    _validate_covariates(df, outcome_col, covariates)
    info(
        f"LRT: computing score and information for {len(model_covariates)} "
        "covariate(s) (aggregates only)."
    )

    result = _eligible_rows(df, outcome_col, covariates, positive_label)
    if result is None:
        return {}
    eligible, is_event = result

    X = _design_matrix(eligible, model_covariates, means, sds)
    y = is_event.astype(float)
    p = sigmoid(X @ np.asarray(beta, dtype=float))
    p = np.clip(p, 1e-10, 1 - 1e-10)

    score = X.T @ (y - p)
    weights = p * (1.0 - p)
    information = (X * weights[:, None]).T @ X

    return {
        "score": score.tolist(),
        "information": information.tolist(),
        "n": int(len(eligible)),
    }


@data(1)
def partial_log_likelihood(
    df: pd.DataFrame,
    outcome_col: str,
    covariates: List[str],
    model_covariates: List[str],
    means: Dict[str, float],
    sds: Dict[str, float],
    beta: List[float],
    positive_label: Optional[object] = None,
) -> Dict[str, object]:
    """
    The log-likelihood at the final coefficients ``beta``, summed over every
    eligible record at this node — a single number, not a per-record value.

    Returns
    -------
    dict
        ``{"log_likelihood": float, "n": int}``, or ``{}`` if this node does
        not contribute.
    """
    _validate_covariates(df, outcome_col, covariates)
    info("LRT: computing log-likelihood contribution (aggregate only).")

    result = _eligible_rows(df, outcome_col, covariates, positive_label)
    if result is None:
        return {}
    eligible, is_event = result

    X = _design_matrix(eligible, model_covariates, means, sds)
    y = is_event.astype(float)
    p = np.clip(sigmoid(X @ np.asarray(beta, dtype=float)), 1e-10, 1 - 1e-10)
    log_likelihood = float(np.sum(y * np.log(p) + (1 - y) * np.log(1 - p)))

    return {"log_likelihood": log_likelihood, "n": int(len(eligible))}
