"""
This file contains all federated algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the federated task
or directly to the user (if they requested federated results).
"""
import pandas as pd
from typing import Any, List
import numpy as np
import pyaging as pya

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.action import federated
from vantage6.algorithm.decorator.data import dataframe

NODE_RESULT_COLUMNS = ["cohort", "clock", "n_samples", "mean_age", "sd_age"]


def predict_sample_ages(df_long: pd.DataFrame, lista_relojes: List[str]) -> pd.DataFrame:
    """
    Predict the epigenetic age of every sample with each clock.

    Parameters:
    -----------
    df_long : pd.DataFrame
        Long-format beta values with columns probe_id, sample_label and beta
    lista_relojes : List[str]
        List of clock names to calculate (e.g., ['horvath2013', 'hannum', 'pcphenoage'])

    Returns:
    --------
    pd.DataFrame : One row per sample (index sample_label), one column per clock.
        A clock that fails to compute is all NaN.
    """
    # pyaging expects samples as rows, CpGs as columns
    matrix = df_long.pivot(index="sample_label", columns="probe_id", values="beta")
    matrix.columns.name = None
    matrix.index.name = None

    adata = pya.preprocess.df_to_adata(matrix, verbose=False)

    for reloj in lista_relojes:
        try:
            pya.pred.predict_age(adata, clock_names=[reloj], verbose=False)
            info(f"Successfully calculated {reloj}")
        except Exception as e:
            warn(f"Failed to calculate {reloj}: {str(e)}")
            adata.obs[reloj] = np.nan

    return adata.obs[list(lista_relojes)].astype(float)


def federated_impl(
    df1: pd.DataFrame,
    lista_relojes: List[str],
    cohort_column: str = "cohort",
    min_samples: int = 3,
) -> pd.DataFrame:
    """
    Calculate per-cohort epigenetic age statistics on this node.

    PRIVACY: Returns only the number of samples, mean and standard deviation of the
    predicted ages per (cohort, clock). Cohorts with fewer than ``min_samples``
    samples on this node are dropped before anything is computed.

    Parameters:
    -----------
    df1 : pd.DataFrame
        Long-format beta values with one row per (sample, probe) and the columns
        probe_id, sample_label, beta and ``cohort_column``
    lista_relojes : List[str]
        List of clock names to calculate (e.g., ['horvath2013', 'hannum', 'pcphenoage'])
    cohort_column : str
        Name of the column in ``df1`` that holds the cohort of each sample
    min_samples : int
        Minimum number of samples a cohort needs on this node to be included

    Returns:
    --------
    pd.DataFrame : Columns cohort, clock, n_samples, mean_age, sd_age (sample
        standard deviation, ddof=1), one row per (cohort, clock)
    """
    if not isinstance(min_samples, int) or isinstance(min_samples, bool) or min_samples < 2:
        raise ValueError(f"min_samples must be an integer of at least 2, got {min_samples!r}")

    empty = pd.DataFrame(columns=NODE_RESULT_COLUMNS)

    if df1 is None or df1.empty:
        error("Input dataframe is empty")
        return empty

    if not lista_relojes:
        warn("No clocks specified")
        return empty

    if cohort_column not in df1.columns:
        error(f"Cohort column '{cohort_column}' not found in the node data")
        raise ValueError(
            f"Cohort column '{cohort_column}' not found in the node data. "
            f"Available columns: {sorted(map(str, df1.columns))}. "
            "Add a cohort column to the sample sheet or pass the correct name "
            "via the 'cohort_column' argument."
        )

    required = {"probe_id", "sample_label", "beta"}
    missing = required - set(df1.columns)
    if missing:
        error(f"Input data missing required columns: {sorted(missing)}")
        raise ValueError(f"Input data missing required columns: {sorted(missing)}")

    data = df1[["probe_id", "sample_label", "beta", cohort_column]].rename(
        columns={cohort_column: "cohort"}
    )

    n_no_cohort = data.loc[data["cohort"].isna(), "sample_label"].nunique()
    if n_no_cohort:
        warn(f"Ignoring {n_no_cohort} sample(s) without a cohort label")
        data = data.dropna(subset=["cohort"])

    cohort_per_sample = data.groupby("sample_label")["cohort"].nunique()
    if (cohort_per_sample > 1).any():
        raise ValueError("Some samples are assigned to more than one cohort")

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

    info(f"Calculating clocks: {lista_relojes} for {data['sample_label'].nunique()} samples")
    ages = predict_sample_ages(data, lista_relojes)
    ages["cohort"] = data.groupby("sample_label")["cohort"].first().reindex(ages.index)

    rows = []
    for cohort, cohort_ages in ages.groupby("cohort"):
        for clk in lista_relojes:
            valid = cohort_ages[clk].dropna()
            if len(valid) < min_samples:
                info(
                    f"Dropping clock '{clk}' for cohort '{cohort}': fewer than "
                    f"min_samples={min_samples} samples with a predicted age"
                )
                continue
            rows.append({
                "cohort": cohort,
                "clock": clk,
                "n_samples": int(len(valid)),
                "mean_age": float(valid.mean()),
                "sd_age": float(valid.std(ddof=1)),
            })

    summary = pd.DataFrame(rows, columns=NODE_RESULT_COLUMNS)
    info(f"Node summary: {summary['cohort'].nunique()} cohort(s), {len(summary)} (cohort, clock) results")
    return summary


@federated
@dataframe(1)
def federated_function(
    df1: pd.DataFrame,
    lista_relojes: List[str],
    cohort_column: str = "cohort",
    min_samples: int = 3,
) -> Any:
    """
    Calculate per-cohort epigenetic age statistics on this node.

    Returns a list of records with cohort, clock, n_samples, mean_age and sd_age.
    See ``federated_impl`` for details.
    """
    summary = federated_impl(
        df1, lista_relojes, cohort_column=cohort_column, min_samples=min_samples
    )
    return summary.to_dict(orient="records")
