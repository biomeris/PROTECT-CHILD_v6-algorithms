"""
This file contains all federated algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the federated task
or directly to the user (if they requested federated results).
"""
"""
Federated functions executed on each node to compute per-cohort Beta and M summaries.

A node never returns the summaries of all probes: an EPIC v2 table has about 870k
probes, which makes a result of several hundred MB, more than vantage6 can store.
`federated_function` returns the summaries of a given list of probes and
`federated_top_probes` returns only the IDs of the node's candidate probes.
"""
import pandas as pd
from typing import Any

import base64
import os

import numpy as np

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.common.globals import ContainerEnvNames
from vantage6.algorithm.decorator.action import federated
from vantage6.algorithm.decorator.data import dataframe

NODE_RESULT_COLUMNS = ["cohort", "probe_id", "beta_mean", "m_mean", "n_samples", "beta_sd", "m_sd"]
# The SD of fewer samples is not returned: with the mean, the SD of 2 samples
# gives both of their values
SD_MIN_SAMPLES = 3
DEFAULT_DISTRIBUTION_BINS = 50
CANDIDATE_COLUMNS = ["probe_id", "delta_beta"]
# Largest number of probes a user may request or rank (see central.py)
MAX_RESULT_PROBES = 10_000
# The central function asks the nodes for at most this many candidate probes at once
CANDIDATE_FACTOR = 2
MAX_NODE_PROBES = CANDIDATE_FACTOR * MAX_RESULT_PROBES


def _organization_id():
    """This node's organization (set by vantage6), or None if unknown."""
    value = os.environ.get(ContainerEnvNames.ORGANIZATION_ID.value)
    return int(value) if value not in (None, "") else None


def _validate_min_samples(min_samples: int) -> None:
    if not isinstance(min_samples, int) or isinstance(min_samples, bool) or min_samples < 1:
        raise ValueError(f"min_samples must be a positive integer, got {min_samples!r}")


def _validate_probes(probes: list[str] | None, limit: int) -> list[str]:
    """Return the requested probe IDs (stripped, without duplicates), within `limit`."""
    if probes is None:
        raise ValueError("A list of probes is required")
    if isinstance(probes, str) or not isinstance(probes, (list, tuple)):
        raise ValueError(f"probes must be a list of probe IDs, got {type(probes).__name__}")
    requested = list(dict.fromkeys(str(probe).strip() for probe in probes if str(probe).strip()))
    if not requested:
        raise ValueError("probes is empty: give at least one probe ID")
    if len(requested) > limit:
        raise ValueError(
            f"probes has {len(requested):,} entries; the maximum is {limit:,}. "
            "Split the list over several tasks."
        )
    return requested


def _validate_top_n(top_n: int) -> None:
    if not isinstance(top_n, int) or isinstance(top_n, bool) or not 1 <= top_n <= MAX_RESULT_PROBES:
        raise ValueError(
            f"top_n must be an integer between 1 and {MAX_RESULT_PROBES:,}, got {top_n!r}. "
            "For more probes, request them by name with the 'probes' argument, "
            f"at most {MAX_RESULT_PROBES:,} per task."
        )


def matching_probe_ids(probe_ids: pd.Series, requested: list[str]) -> pd.Index:
    """Return the probe IDs in the data that match the requested IDs.

    A requested ID matches a probe ID exactly, or matches its CpG name: 'cg00000029'
    selects the EPIC v2 replicates 'cg00000029_TC21' and 'cg00000029_BC11'. This lets
    probe lists from 450K/EPIC studies be used on EPIC v2 data.
    """
    unique_ids = pd.Index(probe_ids.unique()).astype(str)
    wanted = set(requested)
    cpg_names = unique_ids.str.split("_", n=1).str[0]
    return unique_ids[unique_ids.isin(wanted) | cpg_names.isin(wanted)]


def _validate_cohorts(cohorts) -> list[str] | None:
    """Cohorts to compare (None or empty: all). The web interface sends [] for blank."""
    if not cohorts:
        return None
    if not isinstance(cohorts, (list, tuple)) or not all(isinstance(c, (str, int)) for c in cohorts):
        raise ValueError(f"cohorts must be a list of cohort labels, got {cohorts!r}")
    return [str(c) for c in dict.fromkeys(cohorts)]


def _with_cohorts(data: pd.DataFrame, cohort_column: str, cohorts: list[str] | None) -> pd.DataFrame:
    """Only the samples of the chosen cohorts (all if None)."""
    if not cohorts:
        return data
    return data[data[cohort_column].astype(str).isin(cohorts)]


def federated_impl(
    df1: pd.DataFrame,
    cohort_column: str = "cohort",
    min_samples: int = 2,
    probes: list[str] | None = None,
    cohorts: list[str] | None = None,
) -> pd.DataFrame:
    """Compute per-cohort, per-probe mean Beta and M values from preprocessed node data.

    Parameters
    ----------
    df1 : pd.DataFrame
        Long-format table with one row per (sample, probe) and the columns
        ``probe_id``, ``sample_label``, ``beta``, ``m_value`` and ``cohort_column``.
    cohort_column : str
        Name of the column in ``df1`` that holds the cohort of each sample.
    min_samples : int
        Cohorts with fewer samples than this on this node are dropped before
        anything is returned. Probes for which fewer than ``min_samples`` samples
        of a cohort have a value are dropped as well.
    probes : list[str] | None
        Only summarise these probes (see ``matching_probe_ids``). None summarises
        every probe; the node-facing functions never return that to HQ.
    cohorts : list[str] | None
        Only these cohorts (None: all).

    Returns
    -------
    pd.DataFrame
        Columns: cohort, probe_id, beta_mean, m_mean, n_samples, beta_sd, m_sd
        (sample SD; empty when fewer than SD_MIN_SAMPLES samples have a value), so
        the central function can pool the means and SDs exactly.
    """
    _validate_min_samples(min_samples)

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

    data = _with_cohorts(df1[["probe_id", "sample_label", "beta", "m_value", cohort_column]],
                         cohort_column, cohorts)
    if probes is not None:
        selected = matching_probe_ids(data["probe_id"], probes)
        info(f"{len(selected):,} probe(s) on this node match the {len(probes):,} requested")
        data = data[data["probe_id"].isin(selected)]
        if data.empty:
            return empty
    data = data.rename(columns={cohort_column: "cohort"})

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
        data.groupby(["cohort", "probe_id"], observed=True)
        .agg(
            beta_mean=("beta", "mean"),
            m_mean=("m_value", "mean"),
            n_samples=("sample_label", "nunique"),
            beta_sd=("beta", "std"),
            m_sd=("m_value", "std"),
        )
        .reset_index()
    )
    # Mean and SD of 2 samples give both values: no SD below SD_MIN_SAMPLES
    few = summary["n_samples"] < SD_MIN_SAMPLES
    summary.loc[few, ["beta_sd", "m_sd"]] = float("nan")

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


def cohort_sizes(df1: pd.DataFrame, cohort_column: str = "cohort", min_samples: int = 2,
                 cohorts: list[str] | None = None) -> dict[str, int]:
    """Number of samples per cohort on this node, for the cohorts that pass min_samples.

    The central function divides a probe's sample count by this to report coverage.
    """
    if df1 is None or df1.empty or cohort_column not in df1.columns:
        return {}
    data = _with_cohorts(df1.dropna(subset=[cohort_column]), cohort_column, cohorts)
    sizes = data.groupby(data[cohort_column].astype(str))["sample_label"].nunique()
    return {str(cohort): int(n) for cohort, n in sizes.items() if n >= min_samples}


def participation(df1: pd.DataFrame, cohort_column: str = "cohort", min_samples: int = 2,
                  cohorts: list[str] | None = None) -> list[dict]:
    """Per cohort at this node: included (with its sample count) or left out (reason only).

    A cohort below min_samples is reported without its count; a chosen cohort
    the node does not have is reported as not present.
    """
    if df1 is None or df1.empty or cohort_column not in df1.columns:
        return []
    data = _with_cohorts(df1.dropna(subset=[cohort_column]), cohort_column, cohorts)
    sizes = data.groupby(data[cohort_column].astype(str))["sample_label"].nunique()
    rows = []
    for cohort, n in sizes.items():
        if n >= min_samples:
            rows.append({"cohort": str(cohort), "n_samples": int(n), "status": "included",
                         "sd_reported": bool(n >= SD_MIN_SAMPLES)})
        else:
            rows.append({"cohort": str(cohort), "n_samples": None,
                         "status": f"left out: fewer than {min_samples} samples", "sd_reported": False})
    for cohort in cohorts or []:
        if cohort not in sizes.index.astype(str):
            rows.append({"cohort": cohort, "n_samples": None, "status": "not present",
                         "sd_reported": False})
    return rows


def beta_histogram(df1: pd.DataFrame, cohort_column: str = "cohort", min_samples: int = 2,
                   cohorts: list[str] | None = None, bins: int = DEFAULT_DISTRIBUTION_BINS) -> dict:
    """Counts of all Beta values (every probe and sample) per cohort in `bins`
    equal bins over [0, 1], for the cohorts that pass min_samples. No probe or
    sample identity is kept, only counts per bin."""
    if not bins or df1 is None or df1.empty or cohort_column not in df1.columns:
        return {}
    data = _with_cohorts(df1.dropna(subset=[cohort_column, "beta"]), cohort_column, cohorts)
    sizes = data.groupby(data[cohort_column].astype(str))["sample_label"].nunique()
    edges = np.linspace(0.0, 1.0, int(bins) + 1)
    out = {}
    for cohort, group in data.groupby(data[cohort_column].astype(str)):
        if sizes[cohort] >= min_samples:
            counts, _ = np.histogram(np.clip(group["beta"].to_numpy(dtype=float), 0, 1), bins=edges)
            out[str(cohort)] = counts.astype(int).tolist()
    return out


def between_cohort_delta(summary: pd.DataFrame, mean_column: str) -> pd.Series:
    """Per probe, the largest minus the smallest cohort mean Beta (Δβ).

    Probes measured in fewer than two cohorts have no Δβ (NaN).
    """
    if summary.empty:
        return pd.Series(dtype=float, name="delta_beta")
    means = summary.pivot(index="probe_id", columns="cohort", values=mean_column)
    delta = means.max(axis=1) - means.min(axis=1)
    delta[means.notna().sum(axis=1) < 2] = float("nan")
    return delta.rename("delta_beta")


def top_probes_impl(
    df1: pd.DataFrame,
    cohort_column: str = "cohort",
    min_samples: int = 2,
    top_n: int = 50,
    cohorts: list[str] | None = None,
) -> pd.DataFrame:
    """The node's `top_n` probes with the largest difference between its cohorts
    (only the chosen `cohorts`, if given).

    Returns only probe IDs and their Δβ on this node (columns probe_id, delta_beta),
    computed from the cohorts that pass `min_samples`. A node with fewer than two
    such cohorts has no candidates.
    """
    _validate_top_n(top_n)
    summary = federated_impl(df1, cohort_column=cohort_column, min_samples=min_samples,
                             cohorts=cohorts)
    delta = between_cohort_delta(summary, "beta_mean").dropna()
    if delta.empty:
        info("Fewer than two cohorts pass min_samples on this node; no candidate probes")
        return pd.DataFrame(columns=CANDIDATE_COLUMNS)
    top = delta.sort_values(ascending=False, kind="stable").head(top_n)
    info(f"Proposing the node's top {len(top):,} probes by difference between cohorts")
    return top.reset_index()[CANDIDATE_COLUMNS]


@federated
@dataframe(1)
def federated_function(
    df1: pd.DataFrame,
    cohort_column: str = "cohort",
    min_samples: int = 2,
    probes: list[str] | None = None,
    cohorts: list[str] | None = None,
    distribution_bins: int = 0,
) -> Any:
    """Return node-level per-cohort means and SDs of the given probes, the node's
    cohort sizes and participation, and optionally the Beta distribution per cohort.

    Result: {"summary": records with NODE_RESULT_COLUMNS,
             "cohort_sizes": {cohort: number of samples},
             "participation": [{cohort, n_samples, status, sd_reported}],
             "organization_id": this node's organization (None if unknown),
             "beta_histogram": {cohort: counts per bin} (if distribution_bins)}
    """
    requested = _validate_probes(probes, MAX_NODE_PROBES)
    cohorts = _validate_cohorts(cohorts)
    summary = federated_impl(
        df1, cohort_column=cohort_column, min_samples=min_samples, probes=requested,
        cohorts=cohorts,
    )
    return {
        "summary": summary.to_dict(orient="records"),
        "cohort_sizes": cohort_sizes(df1, cohort_column, min_samples, cohorts),
        "participation": participation(df1, cohort_column, min_samples, cohorts),
        "organization_id": _organization_id(),
        "beta_histogram": beta_histogram(df1, cohort_column, min_samples, cohorts, distribution_bins),
    }


def _encode(values) -> dict:
    array = np.ascontiguousarray(np.asarray(values, dtype="<f8"))
    return {"n": int(array.size), "data": base64.b64encode(array.tobytes()).decode("ascii")}


def decode(encoded: dict) -> np.ndarray:
    return np.frombuffer(base64.b64decode(encoded["data"]), dtype="<f8")


@federated
@dataframe(1)
def federated_probe_means(
    df1: pd.DataFrame,
    cohort_column: str = "cohort",
    min_samples: int = 2,
    cohorts: list[str] | None = None,
) -> Any:
    """Mean Beta and number of samples of every probe per cohort (cohorts that pass
    min_samples), for an exact ranking of the probes over all hospitals. A cohort
    may be at this hospital only, so the ranking needs the means, not this node's
    own top probes. The same kind of aggregate as the means returned for the
    selected probes, for every probe; arrays are binary (base64) to keep it small.
    """
    summary = federated_impl(df1, cohort_column=cohort_column, min_samples=min_samples,
                             cohorts=_validate_cohorts(cohorts))
    out = {}
    for cohort, group in summary.groupby("cohort"):
        out[str(cohort)] = {
            "probes": group["probe_id"].astype(str).tolist(),
            "mean": _encode(group["beta_mean"]),
            "n": _encode(group["n_samples"]),
        }
    info(f"Probe means of {len(out)} cohort(s) for the exact ranking")
    return {"cohorts": out}


@federated
@dataframe(1)
def federated_top_probes(
    df1: pd.DataFrame,
    cohort_column: str = "cohort",
    min_samples: int = 2,
    top_n: int = 50,
    cohorts: list[str] | None = None,
) -> Any:
    """Return the IDs (and node Δβ) of this node's candidate probes for a top-N query."""
    candidates = top_probes_impl(
        df1, cohort_column=cohort_column, min_samples=min_samples, top_n=top_n,
        cohorts=_validate_cohorts(cohorts),
    )
    return candidates.to_dict(orient="records")
