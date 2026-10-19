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

from .federated import (
    CANDIDATE_COLUMNS,
    CANDIDATE_FACTOR,
    MAX_RESULT_PROBES,
    NODE_RESULT_COLUMNS,
    _validate_min_samples,
    _validate_probes,
    _validate_top_n,
    between_cohort_delta,
)

GLOBAL_RESULT_COLUMNS = [
    "cohort",
    "probe_id",
    "beta_mean_global",
    "m_mean_global",
    "n_samples_total",
]
CENTRAL_RESULT_COLUMNS = GLOBAL_RESULT_COLUMNS + ["n_samples_cohort", "coverage", "delta_beta"]


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

    return global_summary[GLOBAL_RESULT_COLUMNS]


def cohort_totals(results: list) -> pd.Series:
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


def rank_candidates(results: list, limit: int) -> list[str]:
    """Union of the nodes' candidate probes, best node Δβ first, at most `limit`."""
    frames = [pd.DataFrame(r, columns=CANDIDATE_COLUMNS) for r in results if r]
    if not frames:
        return []
    candidates = pd.concat(frames, ignore_index=True)
    best = candidates.groupby("probe_id")["delta_beta"].max()
    return best.sort_values(ascending=False, kind="stable").head(limit).index.tolist()


def _run_subtask(client: AlgorithmClient, org_ids: list[int], method: str, arguments: dict,
                 name: str, description: str) -> list:
    task = client.task.create(
        method=method,
        arguments=arguments,
        organizations=org_ids,
        name=name,
        description=description,
    )
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
    top_n: int = 1000,
) -> Any:
    """Global per-cohort mean Beta and M values for selected probes.

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

    Returns
    -------
    list[dict]
        One record per (cohort, probe_id) with beta_mean_global, m_mean_global,
        n_samples_total, n_samples_cohort, coverage and the probe's delta_beta,
        sorted by delta_beta.
    """
    _validate_min_samples(min_samples)
    requested = _validate_probes(probes, MAX_RESULT_PROBES) if probes is not None else None
    if requested is None:
        _validate_top_n(top_n)

    organizations = client.organization.list()
    org_ids = [organization.get("id") for organization in organizations]
    common = {"cohort_column": cohort_column, "min_samples": min_samples}

    if requested is None:
        # Round 1: every node proposes its own top probes (IDs only); the global top
        # is then chosen from the best of these candidates.
        info(f"Selecting the top {top_n:,} probes by difference between cohorts")
        candidate_results = _run_subtask(
            client, org_ids, "federated_top_probes", {**common, "top_n": top_n},
            name="Beta/M candidate probes",
            description="Each node proposes its probes with the largest difference between cohorts",
        )
        requested = rank_candidates(candidate_results, CANDIDATE_FACTOR * top_n)
        info(f"{len(requested):,} candidate probes from {len(candidate_results)} node(s)")
        if not requested:
            warn("No node has two or more cohorts that pass min_samples; no results to return")
            return []

    # Round 2: exact per-cohort summaries of the selected probes from every node
    node_results = _run_subtask(
        client, org_ids, "federated_function", {**common, "probes": requested},
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
    return result.to_dict(orient="records")
