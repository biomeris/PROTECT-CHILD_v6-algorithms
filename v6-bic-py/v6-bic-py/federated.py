"""Local log-likelihood aggregation for BIC; never exports patient-level rows."""
from __future__ import annotations

import math
from typing import Any
import numpy as np
import pandas as pd
from vantage6.algorithm.decorator.action import federated
from vantage6.algorithm.decorator.data import dataframe


def _numeric(series: pd.Series, column: str) -> np.ndarray:
    try:
        return pd.to_numeric(series, errors="raise").to_numpy(dtype=float)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Column {column!r} must be numeric.") from exc


@federated
@dataframe(1)
def federated_function(
    df1: pd.DataFrame,
    input_mode: str = "contributions",
    log_likelihood_column: str = "log_likelihood",
    count_column: str = "n_observations",
    label_column: str = "y_true",
    probability_column: str = "p_positive",
    model_id_column: str | None = None,
) -> dict[str, Any]:
    """Return one sum of fitted-model log-likelihood and a corresponding n."""
    if df1 is None or df1.empty:
        return {"error": "No records in local dataset."}
    if input_mode not in ("contributions", "aggregated", "binary"):
        return {"error": "Unknown input_mode."}

    model_id: str | None = None
    if model_id_column is not None:
        if model_id_column not in df1.columns or df1[model_id_column].isna().any():
            return {"error": "Missing model_id_column or null fitted-model identifiers."}
        values = df1[model_id_column].astype(str).unique()
        if len(values) != 1 or not values[0].strip():
            return {"error": "Each node must use one nonempty fitted-model identifier."}
        model_id = values[0]

    try:
        if input_mode in ("contributions", "aggregated"):
            if log_likelihood_column not in df1:
                return {"error": f"Missing column {log_likelihood_column!r}."}
            vals = _numeric(df1[log_likelihood_column], log_likelihood_column)
            if input_mode == "contributions":
                vals = vals[~np.isnan(vals)]  # A row with missing LL is excluded.
                if not len(vals) or not np.isfinite(vals).all():
                    return {"error": "No valid finite per-observation log-likelihood values."}
                n = int(len(vals))
            else:
                if not np.isfinite(vals).all():
                    return {"error": "Aggregated log-likelihood values cannot be null or infinite."}
                if count_column not in df1:
                    return {"error": f"Missing aggregated count column {count_column!r}."}
                counts = _numeric(df1[count_column], count_column)
                if not np.isfinite(counts).all() or any(v < 1 or not float(v).is_integer() for v in counts):
                    return {"error": "Aggregated observation counts must be positive integers."}
                n = sum(int(v) for v in counts)
            ll = math.fsum(float(value) for value in vals)
        else:
            if label_column not in df1 or probability_column not in df1:
                return {"error": "Binary mode requires both label and probability columns."}
            numeric = pd.DataFrame({
                "label": pd.to_numeric(df1[label_column], errors="raise"),
                "p": pd.to_numeric(df1[probability_column], errors="raise"),
            }).dropna()
            y = numeric["label"].to_numpy(dtype=float)
            p = numeric["p"].to_numpy(dtype=float)
            if not len(y):
                return {"error": "No complete (label, probability) pairs."}
            if not np.isfinite(y).all() or not np.isfinite(p).all():
                return {"error": "Non-finite binary labels or probabilities."}
            if not np.isin(y, [0, 1]).all() or ((p < 0) | (p > 1)).any():
                return {"error": "Labels must be 0/1 and probabilities between 0 and 1."}
            if ((y == 1) & (p == 0)).any() or ((y == 0) & (p == 1)).any():
                return {"error": "Observed outcomes assigned zero probability yield an infinite criterion."}
            terms = [math.log(float(pi)) if yi == 1 else math.log1p(-float(pi))
                     for yi, pi in zip(y, p)]
            ll = math.fsum(terms)
            n = int(len(y))
    except (ValueError, TypeError, OverflowError) as exc:
        return {"error": str(exc)}
    if not math.isfinite(ll) or n < 1:
        return {"error": "Local log-likelihood or n is invalid."}

    return {
        "input_mode": input_mode,
        "log_likelihood": float(ll),
        "n_observations": int(n),
        "model_id": model_id,
    }
