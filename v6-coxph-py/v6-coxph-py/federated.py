"""
This file contains all federated algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the federated task
or directly to the user (if they requested federated results).
"""

import numpy as np
import pandas as pd

from vantage6.algorithm.tools.util import info, warn, get_env_var
from vantage6.algorithm.tools.exceptions import InputError
from vantage6.algorithm.decorator.action import federated
from vantage6.algorithm.decorator.data import dataframe

from .globals import COXPH_MINIMUM_EVENTS


def _is_boolean_like_column(series: pd.Series) -> bool:
    """Whether ``series`` can be used as a 0/1 event indicator.

    Accepts a real boolean dtype, as well as numeric columns that only contain
    the values 0 and 1 (the common survival-analysis encoding for event
    indicators, e.g. ``event_overall_survival`` columns).
    """
    if pd.api.types.is_bool_dtype(series):
        return True
    if pd.api.types.is_numeric_dtype(series):
        return series.dropna().isin([0, 1]).all()
    return False


def _is_categorical_column(series: pd.Series) -> bool:
    """Whether ``series`` is a pandas ``category`` dtype column."""
    return isinstance(series.dtype, pd.CategoricalDtype)


def _require_column(df: pd.DataFrame, column: str) -> None:
    if column not in df.columns:
        raise InputError(f"Column '{column}' not found in DataFrame.")


def _require_numeric_columns(df: pd.DataFrame, columns: list[str]) -> None:
    """Require that each column is numeric (boolean columns are accepted too,
    since they are used as 0/1 covariates/times in the Cox computations)."""
    for col in columns:
        _require_column(df, col)
        if not pd.api.types.is_numeric_dtype(df[col]):
            raise InputError(
                f"Column '{col}' must be numeric, got dtype '{df[col].dtype}'."
            )


def _require_boolean_like_column(df: pd.DataFrame, column: str) -> None:
    _require_column(df, column)
    if not _is_boolean_like_column(df[column]):
        raise InputError(
            f"Column '{column}' must be a boolean event indicator (True/False or "
            f"0/1), got dtype '{df[column].dtype}'."
        )
    if df[column].isna().any():
        warn(
            f"Column '{column}' contains missing values; those rows are treated "
            "as censored (no event) rather than raising an error."
        )


def _require_time_column(df: pd.DataFrame, column: str) -> None:
    """Validate ``column`` can be used as a time-to-event/censoring column.

    Unlike a generic numeric covariate, a time column must not be boolean
    (nonsensical as a duration), must not contain missing values (a NaN would
    silently drop that subject out of every risk set with no warning), and
    must not contain negative values.
    """
    _require_column(df, column)
    series = df[column]
    if pd.api.types.is_bool_dtype(series):
        raise InputError(
            f"Column '{column}' must be a numeric time value, got boolean dtype."
        )
    if not pd.api.types.is_numeric_dtype(series):
        raise InputError(
            f"Column '{column}' must be numeric, got dtype '{series.dtype}'."
        )
    if series.isna().any():
        raise InputError(
            f"Column '{column}' contains missing values; every subject must have "
            "a known time-to-event/censoring value."
        )
    if (series < 0).any():
        raise InputError(f"Column '{column}' must not contain negative values.")


def _format_indicator_name(column: str, level) -> str:
    """Render a one-hot indicator column name in GLM-style: ``column[T.level]``."""
    return f"{column}[T.{level}]"


def _apply_one_hot_encoding(
    df: pd.DataFrame,
    expl_vars: list[str],
    categorical_specs: dict[str, dict],
) -> tuple[pd.DataFrame, list[str]]:
    """Expand categorical columns in ``expl_vars`` into indicator columns.

    ``categorical_specs`` maps each categorical column to
    ``{"levels": [...], "reference": <value>}``. Returns the encoded dataframe and
    the resolved (post-encoding) ``expl_vars`` list, preserving the original
    variable order. Indicator columns are cast to ``Float64`` so the downstream
    numeric dtype checks pass.

    Source values are normalized to strings to match the discovered levels (which
    are also stringified). Rows with a missing value in the source column have
    ``pd.NA`` in every indicator column so the existing ``.dropna()`` calls still
    drop those rows.
    """
    if not categorical_specs:
        return df, list(expl_vars)

    encoded_df = df.copy()
    resolved: list[str] = []

    for var in expl_vars:
        if var not in categorical_specs:
            resolved.append(var)
            continue

        spec = categorical_specs[var]
        levels = list(spec["levels"])
        reference = spec["reference"]
        non_reference_levels = [lvl for lvl in levels if lvl != reference]
        indicator_names = [
            _format_indicator_name(var, lvl) for lvl in non_reference_levels
        ]

        source = encoded_df[var].astype("string")
        na_mask = source.isna().to_numpy()
        cat_series = source.astype(pd.CategoricalDtype(categories=levels))

        dummies = pd.get_dummies(
            cat_series, prefix=var, prefix_sep="__SEP__", dummy_na=False
        )
        rename_map = {
            f"{var}__SEP__{lvl}": _format_indicator_name(var, lvl) for lvl in levels
        }
        dummies = dummies.rename(columns=rename_map)
        dummies = dummies.reindex(columns=indicator_names, fill_value=0).astype(
            "Float64"
        )
        if na_mask.any() and indicator_names:
            dummies.loc[na_mask, indicator_names] = pd.NA

        encoded_df = encoded_df.drop(columns=[var])
        for name in indicator_names:
            encoded_df[name] = dummies[name].values

        resolved.extend(indicator_names)

    return encoded_df, resolved


@federated
@dataframe(1)
def get_categorical_levels(df: pd.DataFrame, expl_vars: list[str]) -> dict[str, list]:
    """Return the sorted unique non-null values for each ``category``-dtype column.

    Columns with a non-categorical dtype are skipped, so the central function can
    use the keys of the returned mapping as the auto-detected categorical
    predictor set.

    Parameters
    ----------
    df : pd.DataFrame
        The data for the data station.
    expl_vars : list[str]
        The candidate explanatory variables to inspect.

    Returns
    -------
    dict[str, list]
        Mapping from categorical column name to its sorted, stringified levels.
    """
    info("Collecting categorical levels")
    levels: dict[str, list] = {}
    for col in expl_vars:
        if col not in df.columns:
            continue
        if not _is_categorical_column(df[col]):
            continue
        series = df[col].dropna()
        unique_values = sorted({str(v) for v in series.unique()})
        levels[col] = unique_values

    info("Returning results!")
    return levels


@federated
@dataframe(1)
def get_unique_event_times(df: pd.DataFrame, time_col: str, outcome_col: str):
    """Retrieve unique event times (and their local counts) from ``df``.

    If the number of local events is too small, the sub-task refuses to compute
    anything and instead flags itself, so the central function can exclude this
    organization from the analysis (privacy guard).

    Parameters
    ----------
    df : pd.DataFrame
        The data for the data station.
    time_col : str
        The column holding the (numeric) time-to-event/censoring.
    outcome_col : str
        The column holding the (boolean-like) event indicator.

    Returns
    -------
    dict
        Either ``{"times": <table of unique event times + frequencies>}`` or
        ``{"N-Threshold not met": True}`` if there are too few local events.
    """
    info("Computing unique event times")
    _require_time_column(df, time_col)
    _require_boolean_like_column(df, outcome_col)

    minimum_events = get_env_var(
        "COXPH_MINIMUM_EVENTS", COXPH_MINIMUM_EVENTS, as_type="int"
    )
    if (df[outcome_col] == 1).sum() <= minimum_events:
        warn(
            "Sub-task was not executed because the number of local events is too "
            f"small (n <= {minimum_events})"
        )
        return {"N-Threshold not met": True}

    times = df[df[outcome_col] == 1].groupby(time_col, as_index=False).count()
    times = times.sort_values(by=time_col)[[time_col, outcome_col]]
    times["freq"] = times[outcome_col]
    times = times.drop(columns=outcome_col)

    info("Returning results!")
    return {"times": times.to_dict()}


@federated
@dataframe(1)
def compute_summed_z(
    df: pd.DataFrame,
    outcome_col: str,
    expl_vars: list[str],
    categorical_specs: dict[str, dict] | None = None,
) -> dict:
    """Compute the sum of the explanatory variables over the local outcome events.

    Parameters
    ----------
    df : pd.DataFrame
        The data for the data station.
    outcome_col : str
        The column holding the (boolean-like) event indicator.
    expl_vars : list[str]
        The explanatory variables (covariates) to sum.
    categorical_specs : dict[str, dict], optional
        Mapping from categorical column name to
        ``{"levels": [...], "reference": <value>}``, as discovered by
        ``get_categorical_levels`` and resolved centrally. If provided, the
        listed columns are one-hot encoded (with the reference level dropped)
        before summing.

    Returns
    -------
    dict
        ``{"sum": {<covariate>: <summed value>, ...}}``.
    """
    info("Computing summed z statistics")
    _require_boolean_like_column(df, outcome_col)

    df, expl_vars = _apply_one_hot_encoding(df, list(expl_vars), categorical_specs or {})
    _require_numeric_columns(df, expl_vars)

    z_sum = df[df[outcome_col] == 1][expl_vars].dropna().sum().to_dict()

    info("Returning results!")
    return {"sum": z_sum}


@federated
@dataframe(1)
def perform_iteration(
    df: pd.DataFrame,
    time_col: str,
    expl_vars: list[str],
    beta: list[float],
    unique_time_events: list[float],
    categorical_specs: dict[str, dict] | None = None,
) -> dict:
    """Compute the local risk-set aggregates needed for one Newton-Raphson step.

    For every globally unique event time, this computes the local contribution
    to the risk-set sums ``S0 = sum(exp(beta.x))``, ``S1 = sum(x * exp(beta.x))``
    and ``S2 = sum(x.x' * exp(beta.x))``, where the local risk set is every local
    subject whose time is greater than or equal to that event time.

    Parameters
    ----------
    df : pd.DataFrame
        The data for the data station.
    time_col : str
        The column holding the (numeric) time-to-event/censoring.
    expl_vars : list[str]
        The explanatory variables (covariates) of the model.
    beta : list[float]
        The current estimate of the beta coefficients.
    unique_time_events : list[float]
        The globally aggregated list of unique event times.
    categorical_specs : dict[str, dict], optional
        Mapping from categorical column name to
        ``{"levels": [...], "reference": <value>}``. If provided, the listed
        columns are one-hot encoded (with the reference level dropped) before
        computation.

    Returns
    -------
    dict
        ``{"agg1": [...], "agg2": {...}, "agg3": [[...], ...], "risk_set_size":
        [...]}`` - the local S0, S1 and S2 risk-set sums per unique event time,
        plus the plain local head-count of each risk set (used centrally only
        as a privacy signal - a small *global* risk set at some time can leak
        a subject's covariates via S0/S1/S2).
    """
    info("Computing aggregates for the derivation of the partial likelihood")
    _require_time_column(df, time_col)

    df, expl_vars = _apply_one_hot_encoding(df, list(expl_vars), categorical_specs or {})
    _require_numeric_columns(df, expl_vars)

    beta = np.array(beta)
    num_unique_time_events = len(unique_time_events)
    num_explanatory_vars = len(expl_vars)

    agg1 = []
    agg2 = []
    agg3 = []
    risk_set_size = []

    for i in range(num_unique_time_events):
        R_i_df = df[df[time_col] >= unique_time_events[i]][expl_vars].dropna()
        risk_set_size.append(len(R_i_df))
        if not R_i_df.empty:
            R_i = R_i_df.to_numpy(dtype=float)
            ebz = np.exp(np.dot(R_i, beta))
            agg1.append(float(np.sum(ebz)))
            z_ebz = R_i * ebz[:, np.newaxis]
            agg2.append(pd.Series(z_ebz.sum(axis=0), index=expl_vars))

            summed = np.zeros((num_explanatory_vars, num_explanatory_vars))
            for j in range(len(R_i)):
                summed += np.outer(z_ebz[j], R_i[j])
            agg3.append(summed)
        else:
            agg1.append(0)
            agg2.append(pd.Series(np.zeros(num_explanatory_vars), index=expl_vars))
            agg3.append(np.zeros((num_explanatory_vars, num_explanatory_vars)))

    # JSON-serialize the results
    agg2 = pd.DataFrame(agg2).to_dict()
    agg3 = [array.tolist() for array in agg3]

    info("Returning results!")
    return {"agg1": agg1, "agg2": agg2, "agg3": agg3, "risk_set_size": risk_set_size}
