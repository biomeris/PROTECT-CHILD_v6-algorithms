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

from .clock_bundle import clock_cpgs
from .epicv2 import adapt_long, default_legacy_map, to_clock_ids

# Clocks calculated when the user does not choose any (pyaging clock names)
DEFAULT_CLOCKS = ["horvath2013", "hannum", "pcphenoage", "pedbe"]

NODE_RESULT_COLUMNS = [
    "cohort", "clock", "n_samples", "mean_age", "sd_age", "coverage", "coverage_ok",
    "n_with_age", "mean_chronological_age", "mean_age_acceleration", "sd_age_acceleration",
]


def keep_clock_cpgs(df_long: pd.DataFrame, lista_relojes: List[str], legacy_map) -> pd.DataFrame:
    """Keep only the rows of probes that one of the clocks uses.

    An EPIC v2 table has ~870k probes per sample while the clocks use a few thousand
    CpGs; converting and pivoting only those saves most of the memory and time. If a
    clock's CpG list cannot be read, nothing is removed (that clock will then also
    fail to compute and be reported as failed).
    """
    needed: set[str] = set()
    for reloj in lista_relojes:
        try:
            needed |= clock_cpgs(reloj)
        except Exception as e:
            warn(f"Could not read the CpGs of clock '{reloj}' ({e}); using all probes")
            return df_long
    probe_ids = pd.Index(df_long["probe_id"].unique())
    keep = probe_ids[to_clock_ids(probe_ids, legacy_map).isin(needed).to_numpy()]
    info(f"Keeping {len(keep):,} of {len(probe_ids):,} probes: the CpGs of the requested clocks")
    return df_long[df_long["probe_id"].isin(keep)]


def predict_sample_ages(df_long: pd.DataFrame, lista_relojes: List[str]) -> pd.DataFrame:
    """
    Predict the epigenetic age of every sample with each clock.

    EPIC v2 probe identifiers ('cg00000029_TC21') are converted to the clock CpG
    identifiers ('cg00000029') and replicate probes are averaged first. CpGs renamed
    in EPIC v2 are also mapped to their 450K / EPIC v1 identifier, with the mapping
    packaged with the algorithm (or the manifest at $MANIFEST_PATH, if set).

    Parameters:
    -----------
    df_long : pd.DataFrame
        Long-format beta values with columns probe_id, sample_label and beta
    lista_relojes : List[str]
        List of clock names to calculate (e.g., ['horvath2013', 'hannum', 'pcphenoage'])

    Returns:
    --------
    pd.DataFrame : One row per sample (index sample_label), one column per clock.
        A clock that fails to compute is all NaN. ``.attrs["clock_coverage"]`` holds,
        per clock, the share of its CpGs present in the data (None if it failed), and
        ``.attrs["legacy_loci_mapping"]`` whether the rename mapping was used.
    """
    legacy_map, source = default_legacy_map()
    if legacy_map is None:
        info(f"Renamed EPIC v2 CpGs: {source}; only EPIC v2 suffixes are stripped")
    else:
        info(f"Renamed EPIC v2 CpGs: mapping {len(legacy_map)} to 450K/EPIC v1 identifiers ({source})")
    data = keep_clock_cpgs(df_long[["probe_id", "sample_label", "beta"]], lista_relojes, legacy_map)
    data = adapt_long(data, value_columns=("beta",), legacy_map=legacy_map)

    # pyaging expects samples as rows, CpGs as columns
    matrix = data.pivot(index="sample_label", columns="probe_id", values="beta")
    matrix.columns.name = None
    matrix.index.name = None

    adata = pya.preprocess.df_to_adata(matrix, verbose=False)

    coverage = {}
    for reloj in lista_relojes:
        try:
            pya.pred.predict_age(adata, clock_names=[reloj], verbose=False)
            coverage[reloj] = 1 - adata.uns[f"{reloj}_percent_na"] / 100
            info(f"Successfully calculated {reloj} ({coverage[reloj]:.1%} of its CpGs present)")
        except Exception as e:
            warn(f"Failed to calculate {reloj}: {str(e)}")
            adata.obs[reloj] = np.nan
            coverage[reloj] = None

    ages = adata.obs[list(lista_relojes)].astype(float)
    ages.attrs["clock_coverage"] = coverage
    ages.attrs["legacy_loci_mapping"] = legacy_map is not None
    return ages


def federated_impl(
    df1: pd.DataFrame,
    lista_relojes: List[str] | None = None,
    cohort_column: str = "cohort",
    min_samples: int = 3,
    coverage_threshold: float = 0.9,
    age_column: str | None = "age",
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
    lista_relojes : List[str] | None
        pyaging clock names to calculate (e.g., ['horvath2013', 'hannum', 'pedbe']).
        If None or empty, DEFAULT_CLOCKS are calculated.
    cohort_column : str
        Name of the column in ``df1`` that holds the cohort of each sample
    min_samples : int
        Minimum number of samples a cohort needs on this node to be included
    coverage_threshold : float
        Minimum coverage (share of a clock's CpGs present in this node's data, 0-1)
        for a result to be flagged ``coverage_ok``. pyaging fills missing CpGs with
        reference values or 0, which can bias the age. It only sets the flag; no
        result is removed.
    age_column : str | None
        Column with each sample's chronological age (e.g. 'age' from the IDAT
        extraction's sample_columns). If present, the result also compares the
        predicted with the chronological age. None or a missing column skips this.

    Returns:
    --------
    pd.DataFrame : Columns cohort, clock, n_samples, mean_age, sd_age (sample
        standard deviation, ddof=1), coverage and coverage_ok, one row per
        (cohort, clock), plus n_with_age (samples with a predicted and a
        chronological age), mean_chronological_age, mean_age_acceleration and
        sd_age_acceleration (acceleration = predicted minus chronological age).
        The age fields are empty when fewer than ``min_samples`` samples have an
        age; the row is kept. ``.attrs["clock_coverage"]`` holds the coverage of each
        clock on this node (None if the clock failed to compute; failed clocks have
        no rows).
    """
    if not isinstance(min_samples, int) or isinstance(min_samples, bool) or min_samples < 2:
        raise ValueError(f"min_samples must be an integer of at least 2, got {min_samples!r}")
    if not 0 <= coverage_threshold <= 1:
        raise ValueError(
            f"coverage_threshold must be between 0 and 1, got {coverage_threshold!r}"
        )

    empty = pd.DataFrame(columns=NODE_RESULT_COLUMNS)

    if df1 is None or df1.empty:
        error("Input dataframe is empty")
        return empty

    if not lista_relojes:
        lista_relojes = list(DEFAULT_CLOCKS)
        info(f"No clocks specified; using the default clocks: {lista_relojes}")

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

    chronological = None
    if age_column and age_column in df1.columns:
        chronological = pd.to_numeric(
            df1.groupby("sample_label")[age_column].first(), errors="coerce"
        )
        info(f"Comparing with chronological age ('{age_column}'): "
             f"{int(chronological.notna().sum())} sample(s) have an age")
    elif age_column:
        info(f"No '{age_column}' column; the chronological age comparison is skipped")

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
    coverage = ages.attrs["clock_coverage"]
    # Clocks that failed to compute have no coverage and produce no rows
    clocks_used = [clk for clk in lista_relojes if coverage[clk] is not None]
    for clk in clocks_used:
        if coverage[clk] < coverage_threshold:
            warn(
                f"Clock '{clk}': only {coverage[clk]:.1%} of its CpGs are present on this "
                f"node (below {coverage_threshold:.0%}); results have coverage_ok=False"
            )
    ages["cohort"] = data.groupby("sample_label")["cohort"].first().reindex(ages.index)

    rows = []
    for cohort, cohort_ages in ages.groupby("cohort"):
        for clk in clocks_used:
            valid = cohort_ages[clk].dropna()
            if len(valid) < min_samples:
                info(
                    f"Dropping clock '{clk}' for cohort '{cohort}': fewer than "
                    f"min_samples={min_samples} samples with a predicted age"
                )
                continue
            row = {
                "cohort": cohort,
                "clock": clk,
                "n_samples": int(len(valid)),
                "mean_age": float(valid.mean()),
                "sd_age": float(valid.std(ddof=1)),
                "coverage": float(coverage[clk]),
                "coverage_ok": bool(coverage[clk] >= coverage_threshold),
                "n_with_age": 0,
                "mean_chronological_age": None,
                "mean_age_acceleration": None,
                "sd_age_acceleration": None,
            }
            if chronological is not None:
                real = chronological.reindex(valid.index).dropna()
                row["n_with_age"] = int(len(real))
                # Same privacy threshold as the ages: too few samples with an age -> empty
                if len(real) >= min_samples:
                    acceleration = valid.loc[real.index] - real
                    row.update({
                        "mean_chronological_age": float(real.mean()),
                        "mean_age_acceleration": float(acceleration.mean()),
                        "sd_age_acceleration": float(acceleration.std(ddof=1)),
                    })
            rows.append(row)

    summary = pd.DataFrame(rows, columns=NODE_RESULT_COLUMNS)
    summary.attrs["clock_coverage"] = coverage
    summary.attrs["legacy_loci_mapping"] = ages.attrs["legacy_loci_mapping"]
    info(f"Node summary: {summary['cohort'].nunique()} cohort(s), {len(summary)} (cohort, clock) results")
    return summary


@federated
@dataframe(1)
def federated_function(
    df1: pd.DataFrame,
    lista_relojes: List[str] | None = None,
    cohort_column: str = "cohort",
    min_samples: int = 3,
    coverage_threshold: float = 0.9,
    age_column: str | None = "age",
) -> Any:
    """
    Calculate per-cohort epigenetic age statistics on this node.

    Returns ``{"results": [records with cohort, clock, n_samples, mean_age,
    sd_age, coverage, coverage_ok, n_with_age, mean_chronological_age,
    mean_age_acceleration, sd_age_acceleration], "clock_coverage": {clock: share of its CpGs
    present, or None if the clock failed}, "legacy_loci_mapping": whether renamed
    EPIC v2 CpGs were mapped}``.
    See ``federated_impl`` for details.
    """
    summary = federated_impl(
        df1, lista_relojes, cohort_column=cohort_column, min_samples=min_samples,
        coverage_threshold=coverage_threshold, age_column=age_column,
    )
    return {
        "results": summary.to_dict(orient="records"),
        "clock_coverage": summary.attrs.get("clock_coverage", {}),
        "legacy_loci_mapping": summary.attrs.get("legacy_loci_mapping", False),
    }
