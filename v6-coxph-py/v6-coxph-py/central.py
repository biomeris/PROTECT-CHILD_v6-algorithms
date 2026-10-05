"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""

import numpy as np
import pandas as pd

from scipy.stats import norm, chi2
from scipy.linalg import solve

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.tools.exceptions import InputError
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.client import AlgorithmClient

from .federated import _format_indicator_name
from .globals import (
    COXPH_MAX_EPOCHS,
    COXPH_MAX_THRESHOLD_RETRIES,
    COXPH_MINIMUM_RISK_SET_SIZE,
    COXPH_TOLERANCE,
)


def _assert_results_complete(ids: list, results: list, context: str) -> None:
    """Guard against silently misattributed results.

    ``client.wait_for_results`` drops any run that has no result (e.g. a node
    that crashed or hasn't finished) from the returned list. Every call site
    in this module pairs ``results`` positionally with ``ids`` (via ``zip``),
    so a dropped result would otherwise silently shift every subsequent
    organization's attribution instead of failing loudly.
    """
    if len(results) != len(ids):
        raise InputError(
            f"Expected {len(ids)} results for organizations {ids} during "
            f"'{context}', but received {len(results)}. At least one "
            "organization's task run produced no result (it may have failed "
            "or not finished). Aborting rather than risk misattributing "
            "results to the wrong organization."
        )


@central
@algorithm_client
def coxph(
    client: AlgorithmClient,
    time_col: str,
    outcome_col: str,
    expl_vars: list[str],
    organizations_to_include: list[int] | None = None,
    category_reference_values: dict[str, str] | None = None,
):
    """Central part of the federated Cox Proportional Hazards algorithm.

    Any column in ``expl_vars`` with pandas ``category`` dtype at the data
    stations is auto-detected and one-hot encoded before fitting. Levels are
    discovered via an extra federated round so the encoded column set is
    consistent across nodes. By default the alphabetically-first level of each
    categorical variable serves as the dropped reference; this can be
    overridden per-variable via ``category_reference_values``.

    Parameters
    ----------
    client : AlgorithmClient
        The client instance used to interact with the vantage6 server.
    time_col : str
        The column holding the (numeric) time-to-event/censoring.
    outcome_col : str
        The column holding the (boolean-like) event indicator.
    expl_vars : list[str]
        The explanatory variables (covariates) of the model.
    organizations_to_include : list[int], optional
        The organizations to include in the analysis. Defaults to every
        organization in the collaboration.
    category_reference_values : dict[str, str], optional
        Mapping from categorical column name to the level that should serve as
        the dropped reference category.

    Returns
    -------
    dict
        The fitted model table (as JSON), fit statistics, and metadata about
        which organizations were included/excluded and whether the model
        converged.
    """
    category_reference_values = dict(category_reference_values or {})
    expl_vars = list(expl_vars)

    # --- Upfront argument validation (fail fast, before spending any rounds) ---
    if not expl_vars:
        raise InputError("expl_vars must contain at least one explanatory variable.")
    if len(set(expl_vars)) != len(expl_vars):
        raise InputError(f"expl_vars contains duplicate entries: {expl_vars}")
    if time_col == outcome_col:
        raise InputError("time_col and outcome_col must be different columns.")
    if time_col in expl_vars or outcome_col in expl_vars:
        raise InputError(
            "expl_vars must not contain time_col or outcome_col (this would "
            "leak the outcome into the model)."
        )

    invalid_refs = [c for c in category_reference_values if c not in expl_vars]
    if invalid_refs:
        warn(
            f"category_reference_values entries for {invalid_refs} are ignored "
            "because the columns are not in expl_vars."
        )

    if organizations_to_include is None:
        organizations = client.organization.list()
        ids = [organization.get("id") for organization in organizations]
    else:
        ids = list(organizations_to_include)

    info(f"Sending task to organizations {ids}")

    excluded_ids: list[int] = []

    # --- get_unique_event_times: retry while excluding orgs below the N-threshold ---
    aggregated_time_events = None
    for attempt in range(COXPH_MAX_THRESHOLD_RETRIES + 1):
        if attempt == COXPH_MAX_THRESHOLD_RETRIES:
            error(
                "Sample size violations should be eliminated yet criteria are not "
                "met. Exiting"
            )
            raise ValueError(
                "Sample size violations should be eliminated yet criteria are not "
                "met. Exiting"
            )

        info("Creating subtask to retrieve unique event times")
        task = client.task.create(
            method="get_unique_event_times",
            arguments={"time_col": time_col, "outcome_col": outcome_col},
            organizations=ids,
            name="Unique event times",
            description="Getting unique event times and their counts",
        )
        info("Waiting for results")
        results = client.wait_for_results(task_id=task.get("id"))
        _assert_results_complete(ids, results, "get_unique_event_times")
        info("Results obtained!")

        unique_time_events_parts = []
        newly_excluded = []
        for org_id, output in zip(ids, results):
            if "N-Threshold not met" in output:
                warn(
                    f"Insufficient samples for organization {org_id}. Excluding "
                    "organization from analysis."
                )
                newly_excluded.append(org_id)
                continue
            unique_time_events_parts.append(pd.DataFrame.from_dict(output["times"]))

        if not newly_excluded:
            break

        for org_id in newly_excluded:
            ids.remove(org_id)
            excluded_ids.append(org_id)

        if not ids:
            warn(
                "No organizations meet the minimal sample size threshold, "
                "returning NaN."
            )
            return {
                "included_organizations": [],
                "excluded_organizations": excluded_ids,
                "model": None,
                "converged": False,
            }

    aggregated_time_events = pd.concat(unique_time_events_parts)
    aggregated_time_events = aggregated_time_events.groupby(
        time_col, as_index=False
    ).sum()

    if aggregated_time_events.empty:
        info("No events found across the included organizations; cannot fit CoxPH.")
        return {
            "included_organizations": ids,
            "excluded_organizations": excluded_ids,
            "model": None,
            "converged": False,
        }

    unique_time_events = aggregated_time_events[time_col].tolist()

    # --- Auto-detect categorical predictors via federated discovery ---
    info(f"Discovering categorical levels for {list(expl_vars)}")
    task = client.task.create(
        method="get_categorical_levels",
        arguments={"expl_vars": expl_vars},
        organizations=ids,
        name="Categorical levels",
        description="Detecting categorical predictors and their levels per organization",
    )
    info("Waiting for results")
    cat_results = client.wait_for_results(task_id=task.get("id"))
    _assert_results_complete(ids, cat_results, "get_categorical_levels")
    info("Results obtained!")

    per_col_union: dict[str, set] = {}
    for output in cat_results:
        for col, lvls in (output or {}).items():
            per_col_union.setdefault(col, set()).update(lvls)

    categorical_specs: dict[str, dict] = {}
    for col in expl_vars:
        if col not in per_col_union:
            continue
        levels_sorted = sorted(per_col_union[col])
        if not levels_sorted:
            continue
        if len(levels_sorted) < 2:
            warn(
                f"Categorical variable '{col}' has only one observed level "
                f"({levels_sorted!r}) across the included organizations; it "
                "carries no information and will be dropped from the model."
            )
        if col in category_reference_values:
            reference = category_reference_values[col]
            if reference not in levels_sorted:
                raise InputError(
                    f"category_reference_values['{col}']={reference!r} is not "
                    f"present in the observed levels {levels_sorted}."
                )
        else:
            reference = levels_sorted[0]
        categorical_specs[col] = {"levels": levels_sorted, "reference": reference}

    unmatched_category_refs = [
        c
        for c in category_reference_values
        if c in expl_vars and c not in categorical_specs
    ]
    if unmatched_category_refs:
        warn(
            f"category_reference_values entries for {unmatched_category_refs} "
            "are ignored because those columns were never observed as "
            "categorical (category dtype) at any included organization."
        )

    resolved_expl_vars: list[str] = []
    for var in expl_vars:
        if var in categorical_specs:
            spec = categorical_specs[var]
            resolved_expl_vars.extend(
                _format_indicator_name(var, lvl)
                for lvl in spec["levels"]
                if lvl != spec["reference"]
            )
        else:
            resolved_expl_vars.append(var)

    n_covs = len(resolved_expl_vars)
    if n_covs == 0:
        raise InputError(
            "No explanatory variables remain after resolving categorical "
            f"predictors (from expl_vars={expl_vars}). A CoxPH model needs at "
            "least one covariate."
        )

    # --- compute_summed_z ---
    info("Creating subtask to compute the summed Z statistic")
    task = client.task.create(
        method="compute_summed_z",
        arguments={
            "outcome_col": outcome_col,
            "expl_vars": expl_vars,
            "categorical_specs": categorical_specs,
        },
        organizations=ids,
        name="Summed Z statistic",
        description="Computing the summed Z statistic",
    )
    info("Waiting for results")
    results = client.wait_for_results(task_id=task.get("id"))
    _assert_results_complete(ids, results, "compute_summed_z")
    info("Results obtained!")

    z_sum = pd.Series(0.0, index=resolved_expl_vars)
    for output in results:
        z_sum = z_sum.add(pd.Series(output["sum"]), fill_value=0.0)

    # --- Newton-Raphson iteration loop ---
    beta = np.zeros(n_covs)
    converged = False
    secondary_derivative = None
    summed_agg1 = None
    summed_risk_set_size = None
    iteration = 0

    for epoch in range(COXPH_MAX_EPOCHS):
        iteration = epoch + 1
        info(f"Iteration {iteration}")

        task = client.task.create(
            method="perform_iteration",
            arguments={
                "time_col": time_col,
                "expl_vars": expl_vars,
                "beta": beta.tolist(),
                "unique_time_events": unique_time_events,
                "categorical_specs": categorical_specs,
            },
            organizations=ids,
            name="Start iteration",
            description="Iterating to find the optimal beta",
        )
        info("Waiting for results")
        results = client.wait_for_results(task_id=task.get("id"))
        _assert_results_complete(ids, results, f"perform_iteration (epoch {iteration})")
        info("Results obtained!")

        summed_agg1 = sum(np.array(output["agg1"]) for output in results)
        summed_agg2 = sum(
            np.array(pd.DataFrame.from_dict(output["agg2"])) for output in results
        )
        summed_agg3 = sum(
            np.array([np.array(lst) for lst in output["agg3"]]) for output in results
        )
        # Risk-set membership only depends on time_col, not beta, so this is
        # the same every epoch - kept from whichever epoch last ran, used only
        # as a privacy signal in the final result (see COXPH_MINIMUM_RISK_SET_SIZE).
        summed_risk_set_size = sum(
            np.array(output["risk_set_size"]) for output in results
        )

        primary_derivative, secondary_derivative = compute_derivatives(
            summed_agg1, summed_agg2, summed_agg3, aggregated_time_events, z_sum
        )

        beta_old = beta
        try:
            beta = beta_old - solve(secondary_derivative, primary_derivative)
        except Exception as e:
            msg = (
                "Newton step failed: cannot solve the Fisher information system "
                "(singular matrix). Likely causes: perfect multicollinearity "
                "between covariates, complete separation, or numerical overflow "
                "from diverging betas. Consider removing collinear or redundant "
                "predictors."
            )
            warn(msg)
            return {
                "included_organizations": ids,
                "excluded_organizations": excluded_ids,
                "model": None,
                "converged": False,
                "warnings": [msg],
            }

        if np.any(np.isinf(beta)):
            msg = (
                "Newton-Raphson step produced an infinite coefficient (numerical "
                "overflow), likely from diverging betas due to near-perfect "
                "separation. Stopping iteration; results are unreliable."
            )
            warn(msg)
            return {
                "included_organizations": ids,
                "excluded_organizations": excluded_ids,
                "model": None,
                "converged": False,
                "warnings": [msg],
            }

        delta = float(np.max(np.abs(beta - beta_old)))

        if np.isnan(delta):
            info("Delta turned into a NaN, stopping iteration.")
            break

        info(f"Delta: {delta}")
        if delta <= COXPH_TOLERANCE:
            info("Betas have settled! Finished iterating!")
            converged = True
            break

    return _prepare_result(
        beta,
        aggregated_time_events,
        z_sum,
        summed_agg1,
        secondary_derivative,
        resolved_expl_vars,
        ids,
        excluded_ids,
        converged,
        iteration,
        summed_risk_set_size,
    )


def compute_derivatives(
    summed_agg1, summed_agg2, summed_agg3, aggregated_time_events, z_sum
):
    """
    Compute the primary (score) and secondary (Hessian) derivatives of the Cox
    partial log-likelihood, needed for one Newton-Raphson step.

    Parameters
    ----------
    summed_agg1 : numpy.ndarray
        The globally aggregated risk-set sum S0 per unique event time.
    summed_agg2 : numpy.ndarray
        The globally aggregated risk-set sum S1 per unique event time.
    summed_agg3 : numpy.ndarray
        The globally aggregated risk-set sum S2 per unique event time.
    aggregated_time_events : pandas.DataFrame
        The DataFrame containing the frequency (tie count) of each unique
        event time.
    z_sum : pandas.Series
        The globally summed covariate vector over all event subjects.

    Returns
    -------
    tuple
        The primary derivative (score, U(beta)) and secondary derivative
        (Hessian, H(beta)).
    """
    tot_p1 = 0
    tot_p2 = 0

    for index, row in aggregated_time_events.iterrows():
        s1 = row["freq"] * (summed_agg2[index] / summed_agg1[index])

        first_part = summed_agg3[index] / summed_agg1[index]
        numerator = np.outer(summed_agg2[index], summed_agg2[index])
        denominator = summed_agg1[index] * summed_agg1[index]
        second_part = numerator / denominator

        s2 = row["freq"] * (first_part - second_part)

        tot_p1 += s1
        tot_p2 += s2

    primary_derivative = z_sum - tot_p1
    secondary_derivative = -tot_p2

    return primary_derivative, secondary_derivative


def _prepare_result(
    beta: np.ndarray,
    aggregated_time_events: pd.DataFrame,
    z_sum: pd.Series,
    summed_agg1: np.ndarray,
    secondary_derivative: np.ndarray,
    expl_vars: list[str],
    ids_included: list,
    excluded_ids: list,
    converged: bool,
    iterations: int,
    summed_risk_set_size: np.ndarray | None = None,
) -> dict:
    """Build the final result dict (model table, AIC, warnings, etc.)."""
    n_params = len(beta)

    try:
        fisher = np.linalg.inv(-secondary_derivative)
    except np.linalg.LinAlgError:
        msg = (
            "Cannot compute standard errors: Fisher information matrix is "
            "singular. Likely causes: perfect multicollinearity or complete "
            "separation."
        )
        warn(msg)
        return {
            "included_organizations": ids_included,
            "excluded_organizations": excluded_ids,
            "model": None,
            "converged": converged,
            "warnings": [msg],
        }

    SErrors = [np.sqrt(fisher[k, k]) for k in range(fisher.shape[0])]

    zvalues = (np.exp(beta) - 1) / np.array(SErrors)
    pvalues = 2 * norm.cdf(-abs(zvalues))
    degrees_of_freedom = n_params
    wald_statistic = np.dot(beta, np.dot(-secondary_derivative, beta))
    overall_p_value = float(chi2.sf(wald_statistic, degrees_of_freedom))

    try:
        linear_part = np.dot(z_sum, beta)
        risk_set_part = 0
        if hasattr(summed_agg1, "__len__") and len(summed_agg1) > 0:
            for i in range(len(aggregated_time_events)):
                if i < len(summed_agg1) and summed_agg1[i] > 0:
                    freq = aggregated_time_events.iloc[i]["freq"]
                    risk_set_part += freq * np.log(summed_agg1[i])
        log_likelihood = linear_part - risk_set_part
        if np.isnan(log_likelihood) or np.isinf(log_likelihood):
            raise ValueError(f"Invalid log-likelihood: {log_likelihood}")
        aic = float(-2 * log_likelihood + 2 * n_params)
    except (ValueError, IndexError, FloatingPointError) as e:
        warn(f"Could not compute AIC due to numerical/data issue: {e}")
        aic = np.nan
    except Exception as e:
        warn(f"Unexpected error computing AIC: {e}")
        aic = np.nan

    results_df = pd.DataFrame(
        np.array(
            [
                np.around(beta, 5),
                np.around(np.exp(beta), 5),
                np.around(np.array(SErrors), 5),
            ]
        ).T,
        columns=["Coef", "Exp(coef)", "SE"],
    )
    results_df["Var"] = expl_vars
    results_df["lower_CI"] = np.around(
        np.exp(results_df["Coef"] - 1.96 * results_df["SE"]), 5
    )
    results_df["upper_CI"] = np.around(
        np.exp(results_df["Coef"] + 1.96 * results_df["SE"]), 5
    )
    results_df["Z"] = zvalues
    results_df["p-value"] = pvalues
    results_df = results_df.set_index("Var")

    warnings_list = []
    threshold = 10
    for idx, row in results_df.iterrows():
        coef, se = row["Coef"], row["SE"]
        if (
            abs(coef) > threshold
            or np.isinf(coef)
            or np.isnan(coef)
            or abs(se) > threshold
            or np.isinf(se)
            or np.isnan(se)
        ):
            msg = (
                f"Warning: Covariate '{idx}' may perfectly predict the event "
                f"(coef={coef}, SE={se}). Results may be unreliable."
            )
            warn(msg)
            warnings_list.append(msg)

    if summed_risk_set_size is not None and len(summed_risk_set_size) > 0:
        min_risk_set_size = int(np.min(summed_risk_set_size))
        if min_risk_set_size < COXPH_MINIMUM_RISK_SET_SIZE:
            msg = (
                f"The smallest global risk set used in this fit has only "
                f"{min_risk_set_size} subject(s) (across all included "
                "organizations combined), below the "
                f"COXPH_MINIMUM_RISK_SET_SIZE threshold "
                f"({COXPH_MINIMUM_RISK_SET_SIZE}). At that time point, the "
                "aggregate risk-set statistics could plausibly be used to "
                "infer an individual subject's covariates. See "
                "docs/v6-coxph-py/privacy.rst."
            )
            warn(msg)
            warnings_list.append(msg)

    return {
        "included_organizations": ids_included,
        "excluded_organizations": excluded_ids,
        "model": results_df.to_json(),
        "overall_p_value": overall_p_value,
        "aic": aic,
        "degrees_of_freedom": int(n_params),
        "warnings": warnings_list,
        "converged": converged,
        "iterations": iterations,
    }
