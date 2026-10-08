"""
This file contains all federated algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the federated task
or directly to the user (if they requested federated results).
"""
"""
Federated functions executed on each node to compute per-cohort Beta and M summaries.
"""
import pandas as pd
from typing import Any

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.action import federated
from vantage6.algorithm.decorator.data import dataframe

NODE_RESULT_COLUMNS = ["cohort", "probe_id", "beta_mean", "m_mean", "n_samples"]


def federated_impl(
    df1: pd.DataFrame,
    arg1: Any = None,
    cohort_column: str = "cohort",
    min_samples: int = 2,
) -> pd.DataFrame:
    """Compute per-cohort, per-probe mean Beta and M values from preprocessed node data.

    Parameters
    ----------
    df1 : pd.DataFrame
        Long-format table with one row per (sample, probe) and the columns
        ``probe_id``, ``sample_label``, ``beta``, ``m_value`` and ``cohort_column``.
    arg1 : Any
        Unused; kept for backwards compatibility.
    cohort_column : str
        Name of the column in ``df1`` that holds the cohort of each sample.
    min_samples : int
        Cohorts with fewer samples than this on this node are dropped before
        anything is returned. Probes for which fewer than ``min_samples`` samples
        of a cohort have a value are dropped as well.

    Returns
    -------
    pd.DataFrame
        Columns: cohort, probe_id, beta_mean, m_mean, n_samples, so the central
        function can compute a sample-weighted global mean per cohort.
    """
    if not isinstance(min_samples, int) or isinstance(min_samples, bool) or min_samples < 1:
        raise ValueError(f"min_samples must be a positive integer, got {min_samples!r}")

    empty = pd.DataFrame(columns=NODE_RESULT_COLUMNS)

    if df1 is None or df1.empty:
        warn("No preprocessed data received by federated function")
        return empty

    if cohort_column not in df1.columns:
        error(f"Cohort column '{cohort_column}' not found in the node data")
        raise ValueError(
            f"Cohort column '{cohort_column}' not found in the node data. "
            f"Available columns: {sorted(map(str, df1.columns))}. "
            "Add a cohort column to the sample sheet or pass the correct name "
            "via the 'cohort_column' argument."
        )

    required = {"probe_id", "sample_label", "beta", "m_value"}
    missing = required - set(df1.columns)
    if missing:
        error(f"Preprocessed table missing required columns: {sorted(missing)}")
        raise ValueError("Preprocessed data missing required Beta/M columns")

    data = df1[["probe_id", "sample_label", "beta", "m_value", cohort_column]].rename(
        columns={cohort_column: "cohort"}
    )

    n_no_cohort = data.loc[data["cohort"].isna(), "sample_label"].nunique()
    if n_no_cohort:
        warn(f"Ignoring {n_no_cohort} sample(s) without a cohort label")
        data = data.dropna(subset=["cohort"])

    # Privacy threshold: drop cohorts with too few samples on this node
    cohort_sizes = data.groupby("cohort")["sample_label"].nunique()
    small = cohort_sizes[cohort_sizes < min_samples].index
    for cohort in small:
        info(
            f"Dropping cohort '{cohort}': fewer than min_samples={min_samples} "
            "samples on this node"
        )
    data = data[~data["cohort"].isin(small)]

    if data.empty:
        info("No cohort on this node meets the min_samples threshold; returning no results")
        return empty

    # Only samples with both values count towards the mean of a probe, so that
    # n_samples is the correct weight for the central aggregation
    data = data.dropna(subset=["beta", "m_value"])

    info("Computing node-level mean Beta and M values per cohort and CpG probe")
    summary = (
        data.groupby(["cohort", "probe_id"])
        .agg(
            beta_mean=("beta", "mean"),
            m_mean=("m_value", "mean"),
            n_samples=("sample_label", "nunique"),
        )
        .reset_index()
    )

    # A probe measured in too few samples of a cohort would expose those samples
    sparse = summary["n_samples"] < min_samples
    if sparse.any():
        for cohort, n_probes in summary[sparse].groupby("cohort").size().items():
            info(
                f"Dropping {n_probes} probe(s) of cohort '{cohort}': fewer than "
                f"min_samples={min_samples} samples with a value"
            )
        summary = summary[~sparse].reset_index(drop=True)

    info(
        f"Node summary: {summary['cohort'].nunique()} cohort(s), "
        f"{summary['probe_id'].nunique()} probes"
    )

    return summary[NODE_RESULT_COLUMNS]


@federated
@dataframe(1)
def federated_function(
    df1: pd.DataFrame,
    arg1: Any = None,
    cohort_column: str = "cohort",
    min_samples: int = 2,
) -> Any:
    """Return node-level per-cohort probe means as JSON-serialisable records."""
    summary = federated_impl(
        df1, arg1, cohort_column=cohort_column, min_samples=min_samples
    )
    return summary.to_dict(orient="records")
