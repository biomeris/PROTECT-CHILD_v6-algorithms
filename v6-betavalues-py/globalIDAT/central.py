"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""
"""
Central functions that orchestrate the federated Beta/M computation.

The result is limited to at most MAX_RESULT_PROBES probes: either the probes the
user asks for (`probes`), or the `top_n` probes with the largest difference in mean
Beta between cohorts. The full table (about 870k EPIC v2 probes per cohort) is too
large to store in vantage6 or to show in the UI.
"""
from typing import Any

import pandas as pd
from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.client import AlgorithmClient

import numpy as np

from .federated import (
    CANDIDATE_COLUMNS,
    DEFAULT_DISTRIBUTION_BINS,
    SD_MIN_SAMPLES,
    _validate_cohorts,
    CANDIDATE_FACTOR,
    MAX_RESULT_PROBES,
    NODE_RESULT_COLUMNS,
    _validate_min_samples,
    _validate_probes,
    _validate_top_n,
    between_cohort_delta,
    decode,
)

GLOBAL_RESULT_COLUMNS = [
    "cohort",
    "probe_id",
    "beta_mean_global",
    "m_mean_global",
    "n_samples_total",
    "beta_sd_global",
    "m_sd_global",
    "n_samples_sd",
]
CENTRAL_RESULT_COLUMNS = GLOBAL_RESULT_COLUMNS + ["n_samples_cohort", "coverage", "delta_beta"]
PARTICIPATION_COLUMNS = ["organization_id", "cohort", "n_samples", "status", "sd_reported"]
DISTRIBUTION_COLUMNS = ["cohort", "bin_start", "bin_end", "count", "fraction"]


def aggregate_cohort_results(results: list) -> pd.DataFrame:
    """Combine node-level summaries into a sample-weighted global mean per cohort.

    Each node result holds rows with cohort, probe_id, beta_mean, m_mean and
    n_samples (as records, a DataFrame, or the "summary" of a federated_function
    result). Within each (cohort, probe_id) the global mean is
    sum(mean_i * n_i) / sum(n_i) over the nodes that returned that cohort.
    """
    node_dfs = []
    for r in results:
        if isinstance(r, dict):
            r = r.get("summary") or []
        if isinstance(r, pd.DataFrame):
            node_dfs.append(r)
        elif isinstance(r, list):
            node_dfs.append(pd.DataFrame(r, columns=NODE_RESULT_COLUMNS))

    combined = (
        pd.concat(node_dfs, ignore_index=True)
        if node_dfs
        else pd.DataFrame(columns=NODE_RESULT_COLUMNS)
    )
    if combined.empty:
        return pd.DataFrame(columns=GLOBAL_RESULT_COLUMNS)

    combined["beta_weighted"] = combined["beta_mean"] * combined["n_samples"]
    combined["m_weighted"] = combined["m_mean"] * combined["n_samples"]

    global_summary = (
        combined.groupby(["cohort", "probe_id"])
        .agg(
            beta_sum=("beta_weighted", "sum"),
            m_sum=("m_weighted", "sum"),
            n_samples_total=("n_samples", "sum"),
        )
        .reset_index()
    )
    global_summary["beta_mean_global"] = global_summary["beta_sum"] / global_summary["n_samples_total"]
    global_summary["m_mean_global"] = global_summary["m_sum"] / global_summary["n_samples_total"]

    sd = pooled_sd(combined)
    global_summary = global_summary.merge(sd, on=["cohort", "probe_id"], how="left")
    global_summary["n_samples_sd"] = global_summary["n_samples_sd"].fillna(0).astype("int64")
    return global_summary[GLOBAL_RESULT_COLUMNS]


def pooled_sd(combined: pd.DataFrame) -> pd.DataFrame:
    """Exact SD of all samples per (cohort, probe), from each node's n, mean and SD.

    Only nodes that returned an SD (at least SD_MIN_SAMPLES samples) are pooled;
    n_samples_sd says how many samples are behind it (it can be fewer than behind
    the mean). Total sum of squares = sum((n_i - 1) sd_i^2 + n_i (mean_i - mean)^2).
    """
    columns = ["cohort", "probe_id", "beta_sd_global", "m_sd_global", "n_samples_sd"]
    if "beta_sd" not in combined.columns:
        return pd.DataFrame(columns=columns)
    rows = combined.dropna(subset=["beta_sd", "m_sd"]).copy()
    if rows.empty:
        return pd.DataFrame(columns=columns)
    out = []
    for value in ("beta", "m"):
        rows[f"{value}_w"] = rows[f"{value}_mean"] * rows["n_samples"]
        grouped = rows.groupby(["cohort", "probe_id"])
        n = grouped["n_samples"].sum()
        mean = grouped[f"{value}_w"].sum() / n
        rows = rows.join(mean.rename(f"{value}_pooled_mean"), on=["cohort", "probe_id"])
        rows[f"{value}_ss"] = ((rows["n_samples"] - 1) * rows[f"{value}_sd"] ** 2
                               + rows["n_samples"] * (rows[f"{value}_mean"] - rows[f"{value}_pooled_mean"]) ** 2)
        ss = rows.groupby(["cohort", "probe_id"])[f"{value}_ss"].sum()
        out.append(np.sqrt(ss / (n - 1)).rename(f"{value}_sd_global"))
        if value == "beta":
            out.append(n.rename("n_samples_sd"))
    return pd.concat(out, axis=1).reset_index()[columns]


def cohort_totals(results: list) -> pd.Series:  # noqa: D401
    """Total number of samples per cohort over the nodes (from their cohort_sizes)."""
    totals: dict[str, int] = {}
    for r in results:
        if isinstance(r, dict):
            for cohort, n in (r.get("cohort_sizes") or {}).items():
                totals[cohort] = totals.get(cohort, 0) + int(n)
    return pd.Series(totals, dtype="int64", name="n_samples_cohort")


def with_delta_beta(global_summary: pd.DataFrame, totals: pd.Series) -> pd.DataFrame:
    """Add coverage and the probe's Δβ to every row, sorted by Δβ.

    - n_samples_cohort: samples of the cohort over all nodes (after min_samples)
    - coverage: n_samples_total / n_samples_cohort, the share of the cohort's samples
      behind the means; a probe that QC removed in some samples has coverage < 1.
      Nothing is dropped for low coverage: users can filter on it.
    - delta_beta: largest minus smallest cohort mean Beta of the probe (largest first;
      probes in a single cohort, without Δβ, last)
    """
    if global_summary.empty:
        return pd.DataFrame(columns=CENTRAL_RESULT_COLUMNS)
    result = global_summary.copy()
    # every cohort in a summary comes from a node that also reported its size
    result["n_samples_cohort"] = result["cohort"].map(totals).astype("int64")
    result["coverage"] = result["n_samples_total"] / result["n_samples_cohort"]
    delta = between_cohort_delta(global_summary, "beta_mean_global")
    result = result.merge(delta.reset_index(), on="probe_id", how="left")
    result = result.sort_values(
        ["delta_beta", "probe_id", "cohort"], ascending=[False, True, True], na_position="last"
    )
    return result[CENTRAL_RESULT_COLUMNS].reset_index(drop=True)


def select_top_probes(result: pd.DataFrame, top_n: int) -> pd.DataFrame:
    """Keep the rows of the `top_n` probes with the largest Δβ (`result` is sorted)."""
    top = result.dropna(subset=["delta_beta"])["probe_id"].drop_duplicates().head(top_n)
    return result[result["probe_id"].isin(top)].reset_index(drop=True)


def rank_exactly(results: list, top_n: int) -> list[str]:
    """The `top_n` probes with the largest difference between the pooled cohort means
    (largest minus smallest), from every hospital's per-cohort probe means."""
    totals: dict[tuple[str, str], list[float]] = {}
    for r in results:
        for cohort, block in ((r or {}).get("cohorts") or {}).items():
            means, counts = decode(block["mean"]), decode(block["n"])
            for probe, mean, n in zip(block["probes"], means, counts):
                entry = totals.setdefault((cohort, probe), [0.0, 0.0])
                entry[0] += mean * n
                entry[1] += n
    if not totals:
        return []
    frame = pd.DataFrame(
        [(cohort, probe, s / n) for (cohort, probe), (s, n) in totals.items() if n > 0],
        columns=["cohort", "probe_id", "beta_mean"],
    )
    delta = between_cohort_delta(frame, "beta_mean").dropna()
    return delta.sort_values(ascending=False, kind="stable").head(top_n).index.tolist()


def rank_candidates(results: list, limit: int) -> list[str]:
    """Union of the nodes' candidate probes, best node Δβ first, at most `limit`."""
    frames = [pd.DataFrame(r, columns=CANDIDATE_COLUMNS) for r in results if r]
    if not frames:
        return []
    candidates = pd.concat(frames, ignore_index=True)
    best = candidates.groupby("probe_id")["delta_beta"].max()
    return best.sort_values(ascending=False, kind="stable").head(limit).index.tolist()


# Output: the number of samples with a value for a probe is reported under a clearer
# name, and values are rounded (Beta values lie in [0, 1]; 4 decimals are ample)
OUTPUT_NAMES = {"n_samples_total": "n_samples_with_value"}
OUTPUT_DECIMALS = 4
OUTPUT_COLUMNS = [OUTPUT_NAMES.get(column, column) for column in CENTRAL_RESULT_COLUMNS]


def select_organizations(client: AlgorithmClient, organizations_to_include: list[int] | None) -> list[int]:
    """Organization ids of the collaboration, or only those in `organizations_to_include`."""
    org_ids = [organization.get("id") for organization in client.organization.list()]
    if not organizations_to_include:
        return org_ids
    wanted = [int(org_id) for org_id in organizations_to_include]
    unknown = sorted(set(wanted) - set(org_ids))
    if unknown:
        raise ValueError(f"organizations_to_include: {unknown} are not in this collaboration ({org_ids})")
    return [org_id for org_id in org_ids if org_id in wanted]


def _run_subtask(client: AlgorithmClient, org_ids: list[int], method: str, arguments: dict,
                 name: str, description: str) -> list:
    task = client.task.create(
        method=method,
        arguments=arguments,
        organizations=org_ids,
        name=name,
        description=description,
    )
    # HQ answers with a message instead of a task when it refuses one (e.g. the
    # dataframe is missing at a node); waiting for it would never end
    if not isinstance(task, dict) or task.get("id") is None:
        message = task.get("msg") if isinstance(task, dict) else task
        raise RuntimeError(f"HQ did not create the subtask '{name}': {message}")
    info(f"Waiting for results from subtask {task.get('id')} ({method})")
    results = client.wait_for_results(task_id=task.get("id"))
    info(f"Received results from {len(results)} organisations")
    return results


@central
@algorithm_client
def central_function(
    client: AlgorithmClient,
    cohort_column: str = "cohort",
    min_samples: int = 2,
    probes: list[str] | None = None,
    top_n: int = 50,
    organizations_to_include: list[int] | None = None,
    cohorts: list[str] | None = None,
    distribution_bins: int = DEFAULT_DISTRIBUTION_BINS,
) -> Any:
    """Global per-cohort mean and SD of Beta and M values for selected probes.

    Parameters
    ----------
    client : AlgorithmClient
        Client provided by vantage6.
    cohort_column : str
        Name of the column in each node's data that holds the cohort of each sample.
    min_samples : int
        Minimum number of samples a cohort needs on a node to be included in that
        node's result.
    probes : list[str] | None
        Probes to report (at most MAX_RESULT_PROBES). A CpG name such as 'cg00000029'
        also selects its EPIC v2 replicates ('cg00000029_TC21'). When given, `top_n`
        is not used.
    top_n : int
        Without `probes`: report the `top_n` probes (at most MAX_RESULT_PROBES) with
        the largest difference in mean Beta between cohorts (Δβ).
    organizations_to_include : list[int] | None
        Organizations (hospitals) to use; all of the collaboration if not given.
    cohorts : list[str] | None
        Cohorts to compare (at least two for the top probes); empty: all. The top
        probes are ranked by the difference between these cohorts only.
    distribution_bins : int
        Number of equal bins over [0, 1] for the distribution of all Beta values
        per cohort (every probe and sample, counts only); 0: not computed.

    Returns
    -------
    dict
        ``results``: one record per (cohort, probe_id) with beta_mean_global,
        m_mean_global, beta_sd_global, m_sd_global (pooled over the hospitals
        where the cohort has at least SD_MIN_SAMPLES samples; n_samples_sd),
        n_samples_with_value, n_samples_cohort, coverage and the probe's
        delta_beta, sorted by delta_beta; ``participation``: per hospital and
        cohort, included (with its sample count) or left out (reason);
        ``beta_distribution``: counts per bin and cohort; ``input``: the settings.
        Values rounded to OUTPUT_DECIMALS decimals.
    """
    _validate_min_samples(min_samples)
    # The web interface sends a blank optional list as [] instead of leaving it out
    if not probes:
        probes = None
    requested = _validate_probes(probes, MAX_RESULT_PROBES) if probes is not None else None
    if requested is None:
        _validate_top_n(top_n)
    cohorts = _validate_cohorts(cohorts)
    if requested is None and cohorts is not None and len(cohorts) < 2:
        raise ValueError("Give at least two cohorts to compare (or none for all cohorts)")
    distribution_bins = int(distribution_bins or 0)
    if not 0 <= distribution_bins <= 1000:
        raise ValueError("distribution_bins must be between 0 and 1000")

    org_ids = select_organizations(client, organizations_to_include)
    common = {"cohort_column": cohort_column, "min_samples": min_samples, "cohorts": cohorts}

    if requested is None:
        # Round 1: every node proposes its own top probes (IDs only); the global top
        # is then chosen from the best of these candidates.
        info(f"Selecting the top {top_n:,} probes by difference between cohorts")
        requested = []
        if cohorts is None:
            candidate_results = _run_subtask(
                client, org_ids, "federated_top_probes", {**common, "top_n": top_n},
                name="Beta/M candidate probes",
                description="Each node proposes its probes with the largest difference between cohorts",
            )
            requested = rank_candidates(candidate_results, CANDIDATE_FACTOR * top_n)
            info(f"{len(requested):,} candidate probes from {len(candidate_results)} node(s)")
        if cohorts is not None or not requested:
            # Chosen cohorts may sit at different hospitals, where no hospital can
            # rank them alone: rank exactly on the pooled means of every probe
            info("Ranking every probe on the pooled cohort means")
            mean_results = _run_subtask(
                client, org_ids, "federated_probe_means", common,
                name="Beta/M probe means for ranking",
                description="Each node returns its per-cohort mean of every probe",
            )
            requested = rank_exactly(mean_results, top_n)
            info(f"{len(requested):,} probes selected by the exact ranking")
        if not requested:
            warn("No node has two or more cohorts that pass min_samples; no results to return")
            return _output(pd.DataFrame(columns=CENTRAL_RESULT_COLUMNS), [], cohorts, top_n,
                           probes, distribution_bins)

    # Round 2: exact per-cohort summaries of the selected probes from every node
    node_results = _run_subtask(
        client, org_ids, "federated_function",
        {**common, "probes": requested, "distribution_bins": distribution_bins},
        name="IDAT Beta/M computation",
        description=f"Per-cohort Beta and M means of {len(requested):,} probes",
    )
    result = with_delta_beta(aggregate_cohort_results(node_results), cohort_totals(node_results))

    if probes is None:
        result = select_top_probes(result, top_n)
    else:
        found = set(result["probe_id"])
        found_cpgs = {probe.split("_", 1)[0] for probe in found}
        n_missing = sum(1 for p in requested if p not in found and p not in found_cpgs)
        if n_missing:
            warn(f"{n_missing:,} of {len(requested):,} requested probes are not in any node's result")

    if result.empty:
        warn("No probe has results that pass min_samples; no results to return")
    else:
        info(
            f"Global summary: {result['cohort'].nunique()} cohort(s), "
            f"{result['probe_id'].nunique():,} probes, {len(result):,} rows"
        )
    return _output(result, node_results, cohorts, top_n, probes, distribution_bins)


def _output(result: pd.DataFrame, node_results: list, cohorts, top_n, probes,
            distribution_bins) -> dict:
    """The result: probe rows, participation per hospital and cohort, Beta distribution."""
    participation_rows = [
        {"organization_id": r.get("organization_id"), **row}
        for r in node_results if isinstance(r, dict)
        for row in r.get("participation", [])
    ]
    histograms: dict[str, np.ndarray] = {}
    for r in node_results:
        if isinstance(r, dict):
            for cohort, counts in (r.get("beta_histogram") or {}).items():
                histograms[cohort] = histograms.get(cohort, 0) + np.asarray(counts, dtype=np.int64)
    distribution = []
    if distribution_bins and histograms:
        edges = np.linspace(0.0, 1.0, distribution_bins + 1)
        for cohort in sorted(histograms):
            counts = histograms[cohort]
            total = counts.sum()
            for k, count in enumerate(counts):
                distribution.append({
                    "cohort": cohort, "bin_start": round(float(edges[k]), 4),
                    "bin_end": round(float(edges[k + 1]), 4), "count": int(count),
                    "fraction": round(float(count / total), 6) if total else 0.0,
                })
    return {
        "results": result.rename(columns=OUTPUT_NAMES).round(OUTPUT_DECIMALS).to_dict(orient="records"),
        "participation": participation_rows,
        "beta_distribution": distribution,
        "input": [{
            "cohorts": ", ".join(cohorts) if cohorts else "all",
            "probes": f"{len(probes)} given" if probes else f"top {top_n} by difference between cohorts",
            "sd_min_samples": SD_MIN_SAMPLES,
            "distribution_bins": distribution_bins,
        }],
    }
