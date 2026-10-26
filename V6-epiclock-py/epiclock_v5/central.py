"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""
from typing import Any, List
import pandas as pd
import numpy as np

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.client import AlgorithmClient

from .clock_bundle import load_bundle
from .federated import DEFAULT_CLOCKS, NODE_RESULT_COLUMNS

GLOBAL_RESULT_COLUMNS = [
    "cohort",
    "clock",
    "mean_age_global",
    "sd_age_global",
    "n_samples_total",
    "n_nodes",
    "coverage_min",
    "coverage_max",
    "coverage_ok",
    "n_with_age_total",
    "mean_chronological_age_global",
    "mean_age_acceleration_global",
    "sd_age_acceleration_global",
]


def pooled_mean_sd(n: np.ndarray, means: np.ndarray, sds: np.ndarray) -> tuple[float, float | None]:
    """Mean and sample standard deviation of all samples, from per-node n, mean and SD."""
    total_n = n.sum()
    mean = float((n * means).sum() / total_n)
    if total_n < 2:
        return mean, None
    sum_squares = ((n - 1) * np.nan_to_num(sds) ** 2 + n * (means - mean) ** 2).sum()
    return mean, float(np.sqrt(sum_squares / (total_n - 1)))


def pooled_age_comparison(group: pd.DataFrame) -> dict:
    """Pool the chronological age and age acceleration of one (cohort, clock) over
    the nodes that reported them (weighted by n_with_age)."""
    fields = {
        "n_with_age_total": 0,
        "mean_chronological_age_global": None,
        "mean_age_acceleration_global": None,
        "sd_age_acceleration_global": None,
    }
    reported = group[pd.to_numeric(group["mean_age_acceleration"], errors="coerce").notna()]
    if reported.empty:
        return fields
    n = reported["n_with_age"].to_numpy(dtype=float)
    real_mean, _ = pooled_mean_sd(
        n, reported["mean_chronological_age"].to_numpy(dtype=float), np.zeros(len(n))
    )
    accel_mean, accel_sd = pooled_mean_sd(
        n, reported["mean_age_acceleration"].to_numpy(dtype=float),
        reported["sd_age_acceleration"].to_numpy(dtype=float),
    )
    fields.update({
        "n_with_age_total": int(n.sum()),
        "mean_chronological_age_global": real_mean,
        "mean_age_acceleration_global": accel_mean,
        "sd_age_acceleration_global": accel_sd,
    })
    return fields


def aggregate_cohort_results(results: list) -> pd.DataFrame:
    """
    Combine node-level statistics into global statistics per (cohort, clock).

    For the nodes i that returned a (cohort, clock):
        n_samples_total = sum(n_i)
        mean_age_global = sum(n_i * mean_i) / n_samples_total
        sd_age_global   = sqrt(sum((n_i - 1) * sd_i^2 + n_i * (mean_i - mean_age_global)^2)
                               / (n_samples_total - 1))
    which equals the mean and sample standard deviation of all the cohort's samples.
    The age acceleration (predicted minus chronological age) is pooled the same way,
    weighted by n_with_age, over the nodes that reported it.

    Parameters:
    -----------
    results : list
        One entry per node: ``{"results": records, ...}`` as returned by
        federated_function, or directly a list of records / DataFrame with columns
        cohort, clock, n_samples, mean_age and sd_age

    Returns:
    --------
    pd.DataFrame : Columns cohort, clock, mean_age_global, sd_age_global,
        n_samples_total, n_nodes, coverage_min, coverage_max (clock coverage over
        the nodes that contributed), coverage_ok (True only if every
        contributing node meets the coverage threshold), and n_with_age_total,
        mean_chronological_age_global, mean_age_acceleration_global and
        sd_age_acceleration_global (empty if no node reported ages)
    """
    node_dfs = []
    for r in results:
        if isinstance(r, dict):
            r = r.get("results", [])
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

    rows = []
    for (cohort, clk), group in combined.groupby(["cohort", "clock"]):
        n = group["n_samples"].to_numpy(dtype=float)
        means = group["mean_age"].to_numpy(dtype=float)
        sds = group["sd_age"].to_numpy(dtype=float)

        total_n = n.sum()
        mean_global, sd_global = pooled_mean_sd(n, means, sds)

        rows.append({
            "cohort": cohort,
            "clock": clk,
            "mean_age_global": mean_global,
            "sd_age_global": sd_global,
            "n_samples_total": int(total_n),
            "n_nodes": int(len(group)),
            "coverage_min": float(group["coverage"].min()),
            "coverage_max": float(group["coverage"].max()),
            "coverage_ok": bool(group["coverage_ok"].astype(bool).all()),
            **pooled_age_comparison(group),
        })

    return pd.DataFrame(rows, columns=GLOBAL_RESULT_COLUMNS)


def check_requested_clocks(lista_relojes: List[str], bundle: dict | None) -> None:
    """Raise a clear error for clocks that are not bundled in this image.

    Without a bundle (local development, online pyaging) any clock is accepted.
    """
    if bundle is None:
        return
    available = bundle["clocks"]
    unknown = [c for c in lista_relojes if c not in available]
    if unknown:
        raise ValueError(
            f"Clock(s) {unknown} are not available in this algorithm image. "
            f"{len(available)} clocks are available: {', '.join(sorted(available))}"
        )


def summarise_clock_coverage(
    results: list, lista_relojes: List[str], coverage_threshold: float,
    bundle: dict | None = None,
) -> list[dict]:
    """Per clock: coverage over the nodes, on how many nodes it meets the coverage
    threshold and on how many it failed to compute (those nodes have no results).
    With a clock bundle, also the clock's non-CpG inputs (``extra_features``, which
    the algorithm does not provide; pyaging fills them with reference values or 0),
    tissue and population from pyaging's catalogue."""
    rows = []
    for clk in lista_relojes:
        values = [
            r["clock_coverage"].get(clk)
            for r in results
            if isinstance(r, dict) and clk in r.get("clock_coverage", {})
        ]
        measured = [v for v in values if v is not None]
        rows.append({
            "clock": clk,
            "coverage_min": min(measured) if measured else None,
            "coverage_max": max(measured) if measured else None,
            "n_nodes_reported": len(values),
            "n_nodes_coverage_ok": sum(v >= coverage_threshold for v in measured),
            "n_nodes_failed": sum(v is None for v in values),
        })
        if bundle is not None and clk in bundle["clocks"]:
            entry = bundle["clocks"][clk]
            rows[-1].update({
                "extra_features": entry.get("extra_features", []),
                "tissue": entry.get("tissue"),
                "population": entry.get("population"),
            })
    return rows


@central
@algorithm_client
def central_function(
    client: AlgorithmClient,
    lista_relojes: List[str] | None = None,
    cohort_column: str = "cohort",
    min_samples: int = 3,
    coverage_threshold: float = 0.9,
    age_column: str | None = "age",
) -> Any:
    """
    Central aggregation of epigenetic age predictions per cohort.

    Implements PRIVACY-PRESERVING AGGREGATION: nodes return only the number of
    samples, mean and standard deviation per (cohort, clock), never individual
    patient data. A cohort may span several organizations; the central node pools
    each cohort across organizations, weighting by the number of samples.

    Parameters:
    -----------
    lista_relojes : List[str], optional
        pyaging clock names to calculate (e.g., ['horvath2013', 'hannum', 'pedbe']).
        If not given or empty, DEFAULT_CLOCKS (defined in federated.py) are
        calculated.
    cohort_column : str, optional
        Name of the column in each node's data that holds the cohort of each sample
        (default: "cohort")
    min_samples : int, optional
        Minimum number of samples a cohort needs on a node to be included in that
        node's result (default: 3)
    coverage_threshold : float, optional
        Minimum coverage (share of a clock's CpGs present in a node's data, 0-1) for
        results to be flagged ``coverage_ok`` (default: 0.9). EPIC v2 lacks some clock
        CpGs and pyaging fills them with reference values or 0, which can bias the
        age. It only sets the flag; no clock or result is removed.
    age_column : str, optional
        Column in each node's data with the chronological age of each sample
        (default: "age", added by the IDAT extraction's sample_columns). Nodes that
        have it also report the age acceleration (predicted minus chronological
        age); None skips the comparison.

    Returns:
    --------
    dict : ``{"results": [one record per (cohort, clock) with mean_age_global,
        sd_age_global, n_samples_total, n_nodes, coverage_min, coverage_max,
        coverage_ok, n_with_age_total, mean_chronological_age_global,
        mean_age_acceleration_global and sd_age_acceleration_global], "clock_coverage": [one record per clock with coverage_min,
        coverage_max, n_nodes_reported, n_nodes_coverage_ok and n_nodes_failed],
        "n_nodes_legacy_loci_mapping": nodes that mapped renamed EPIC v2 CpGs (packaged
        mapping or $MANIFEST_PATH)}``
    """
    info("Retrieving organizations in collaboration")
    organizations = client.organization.list()
    org_ids = [org.get("id") for org in organizations]
    info(f"Found {len(org_ids)} organizations")

    if not lista_relojes:
        lista_relojes = list(DEFAULT_CLOCKS)
        info("No clocks specified; using the default clocks")
    bundle = load_bundle()
    check_requested_clocks(lista_relojes, bundle)
    info(f"Creating subtask to calculate clocks: {lista_relojes}")
    task = client.task.create(
        organizations=org_ids,
        method="federated_function",
        name="Epigenetic clock calculation",
        description="Calculate epigenetic age per cohort using multiple clocks (aggregated statistics only)",
        arguments={
            "lista_relojes": lista_relojes,
            "cohort_column": cohort_column,
            "min_samples": min_samples,
            "coverage_threshold": coverage_threshold,
            "age_column": age_column,
        },
    )

    info(f"Waiting for aggregated results from {len(org_ids)} organizations")
    results = client.wait_for_results(task_id=task.get("id"))
    info(f"Received aggregated results from {len(results)} organizations")

    global_summary = aggregate_cohort_results(results)
    if global_summary.empty:
        warn("No cohort met the min_samples threshold on any node; no results to return")
    else:
        info(
            f"Global summary: {global_summary['cohort'].nunique()} cohort(s), "
            f"{global_summary['clock'].nunique()} clock(s)"
        )
    coverage = summarise_clock_coverage(results, lista_relojes, coverage_threshold, bundle)
    for row in coverage:
        if row.get("extra_features"):
            warn(f"Clock {row['clock']} also needs {row['extra_features']}, which this algorithm "
                 "does not provide; pyaging fills them with reference values or 0")
        if row["n_nodes_failed"]:
            warn(f"Clock {row['clock']} failed to compute on {row['n_nodes_failed']} node(s)")
        n_below = row["n_nodes_reported"] - row["n_nodes_failed"] - row["n_nodes_coverage_ok"]
        if n_below:
            warn(f"Clock {row['clock']}: coverage below {coverage_threshold:.0%} on "
                 f"{n_below} node(s); those results have coverage_ok=False")
    n_mapped = sum(bool(r.get("legacy_loci_mapping")) for r in results if isinstance(r, dict))
    if n_mapped < len(results):
        info(f"{len(results) - n_mapped} node(s) did not map renamed EPIC v2 CpGs (mapping disabled or missing)")
    return {
        "results": global_summary.to_dict(orient="records"),
        "clock_coverage": coverage,
        "n_nodes_legacy_loci_mapping": n_mapped,
    }
