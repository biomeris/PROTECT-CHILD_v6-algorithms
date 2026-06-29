"""
This file contains all federated algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the federated task
or directly to the user (if they requested federated results).
"""
"""
Federated functions executed on each node to compute Beta and M tables.
"""
import pandas as pd
from typing import Any

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.action import federated
from vantage6.algorithm.decorator.data import dataframe


def federated_impl(df1: pd.DataFrame, arg1: Any = None) -> pd.DataFrame:
    """Compute per-probe mean Beta values from preprocessed node data.

    Returns a DataFrame with columns: probe_id, beta_mean, m_mean, n_samples
    so the central function can compute a weighted global mean.
    """
    if df1 is None or df1.empty:
        warn("No preprocessed data received by federated function")
        return pd.DataFrame(columns=["probe_id", "beta_mean", "m_mean", "n_samples"])

    required = {"probe_id", "sample_label", "beta", "m_value"}
    if not required.issubset(set(df1.columns)):
        error(f"Preprocessed table missing required columns: {required}")
        raise ValueError("Preprocessed data missing required Beta/M columns")

    info("Computing node-level mean Beta and M values per CpG probe")
    n_samples = df1["sample_label"].nunique()

    summary = (
        df1.groupby("probe_id")
        .agg(beta_mean=("beta", "mean"), m_mean=("m_value", "mean"))
        .reset_index()
    )
    summary["n_samples"] = n_samples

    info(f"Node summary: {len(summary)} probes, {n_samples} samples")

    return summary


@federated
@dataframe(1)
def federated_function(df1: pd.DataFrame, arg1: Any = None) -> Any:
    """Wrapper decorated function that returns node-level probe means."""
    return federated_impl(df1, arg1)