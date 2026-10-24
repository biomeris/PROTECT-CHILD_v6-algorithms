"""
Central functions for the approximate federated Spearman correlation algorithm.

The algorithm uses three federated rounds:
1. local min/max/count summaries;
2. local histograms on globally defined bin edges;
3. local sufficient statistics computed on approximate global midranks.
"""
from itertools import combinations
from typing import Any, Dict, List, Optional

import numpy as np
from scipy import stats

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.client import AlgorithmClient


def _make_edges(min_val: float, max_val: float, n_bins: int) -> List[float]:
    if not np.isfinite(min_val) or not np.isfinite(max_val):
        raise ValueError("Non-finite min/max encountered while creating bin edges.")
    if max_val == min_val:
        return [float(min_val - 0.5), float(max_val + 0.5)]
    return np.linspace(min_val, max_val, n_bins + 1).astype(float).tolist()


def _pair_key(col1: str, col2: str) -> str:
    return f"{col1}|||{col2}"


@central
@algorithm_client
def central_function(
    client: AlgorithmClient,
    columns: Optional[List[str]] = None,
    n_bins: int = 50,
    approx_pvalues: bool = True,
    organizations_to_include: Optional[List[int]] = None,
) -> Any:
    """Compute an approximate federated Spearman rank correlation matrix."""
    info("Starting central federated Spearman correlation")

    organizations = client.organization.list()
    available_org_ids = [org.get("id") for org in organizations]
    if not available_org_ids:
        error("No organizations found in the collaboration.")
        return {"error": "No organizations found in the collaboration."}

    if organizations_to_include is None:
        org_ids = available_org_ids
    else:
        org_ids = [org_id for org_id in organizations_to_include if org_id in available_org_ids]
    if not org_ids:
        error("No valid organizations selected.")
        return {"error": "No valid organizations selected."}

    n_bins = max(2, int(n_bins))

    info("Round 1/3 - collecting local summaries")
    summary_task = client.task.create(
        method="federated_function",
        arguments={"mode": "summary", "columns": columns},
        organizations=org_ids,
        name="Federated Spearman summaries",
        description="Collect local min/max/count per feature",
    )
    summary_results = client.wait_for_results(task_id=summary_task.get("id"))
    if not summary_results:
        return {"error": "No results received in summary round."}

    expected_columns = None
    global_summary: Dict[str, Dict[str, Any]] = {}

    for idx, res in enumerate(summary_results):
        if res is None:
            warn(f"Empty summary result from node {idx}. Skipping.")
            continue
        if "error" in res:
            warn(f"Node {idx} returned summary error: {res['error']}")
            continue

        local_columns = res.get("columns")
        local_summary = res.get("summary")
        if local_columns is None or local_summary is None:
            continue

        if expected_columns is None:
            expected_columns = list(local_columns)
            global_summary = {
                col: {"count": 0, "min": None, "max": None}
                for col in expected_columns
            }
        elif list(local_columns) != expected_columns:
            return {"error": "Column mismatch between nodes."}

        for col in expected_columns:
            entry = local_summary[col]
            local_count = int(entry["count"])
            if local_count == 0:
                continue

            local_min = float(entry["min"])
            local_max = float(entry["max"])
            global_summary[col]["count"] += local_count

            if global_summary[col]["min"] is None or local_min < global_summary[col]["min"]:
                global_summary[col]["min"] = local_min
            if global_summary[col]["max"] is None or local_max > global_summary[col]["max"]:
                global_summary[col]["max"] = local_max

    if expected_columns is None:
        return {"error": "No valid summary results to aggregate."}

    valid_columns = [col for col in expected_columns if global_summary[col]["count"] > 0]
    if len(valid_columns) < 2:
        return {"error": "At least two valid numeric columns are required."}

    global_edges = {
        col: _make_edges(
            float(global_summary[col]["min"]),
            float(global_summary[col]["max"]),
            n_bins,
        )
        for col in valid_columns
    }

    info("Round 2/3 - collecting local histograms")
    hist_task = client.task.create(
        method="federated_function",
        arguments={
            "mode": "histogram",
            "columns": valid_columns,
            "bin_edges": global_edges,
        },
        organizations=org_ids,
        name="Federated Spearman histograms",
        description="Collect local histograms per feature",
    )
    hist_results = client.wait_for_results(task_id=hist_task.get("id"))
    if not hist_results:
        return {"error": "No results received in histogram round."}

    global_hist_counts = {
        col: np.zeros(len(global_edges[col]) - 1, dtype=float)
        for col in valid_columns
    }

    for idx, res in enumerate(hist_results):
        if res is None:
            continue
        if "error" in res:
            warn(f"Node {idx} returned histogram error: {res['error']}")
            continue

        local_columns = res.get("columns")
        local_hist = res.get("histograms")
        if local_columns is None or local_hist is None:
            continue
        if list(local_columns) != valid_columns:
            return {"error": "Column mismatch in histogram round."}

        for col in valid_columns:
            counts = np.asarray(local_hist[col], dtype=float)
            if counts.shape[0] != len(global_edges[col]) - 1:
                return {"error": f"Histogram length mismatch for column '{col}'."}
            global_hist_counts[col] += counts

    global_rank_maps: Dict[str, Dict[str, Any]] = {}
    constant_columns = set()

    for col in valid_columns:
        counts = global_hist_counts[col]
        cum_before = np.concatenate(([0.0], np.cumsum(counts[:-1])))
        midranks = cum_before + (counts + 1.0) / 2.0
        if np.count_nonzero(counts) <= 1:
            constant_columns.add(col)

        global_rank_maps[col] = {
            "edges": global_edges[col],
            "midranks": midranks.astype(float).tolist(),
            "counts": counts.astype(float).tolist(),
        }

    info("Round 3/3 - collecting pairwise rank statistics")
    pair_task = client.task.create(
        method="federated_function",
        arguments={
            "mode": "pair_stats",
            "columns": valid_columns,
            "rank_maps": global_rank_maps,
        },
        organizations=org_ids,
        name="Federated Spearman pair statistics",
        description="Collect pairwise sufficient statistics on approximate global ranks",
    )
    pair_results = client.wait_for_results(task_id=pair_task.get("id"))
    if not pair_results:
        return {"error": "No results received in pair statistics round."}

    aggregated_pairs: Dict[str, Dict[str, float]] = {}
    for col1, col2 in combinations(valid_columns, 2):
        aggregated_pairs[_pair_key(col1, col2)] = {
            "n": 0.0,
            "sum_x": 0.0,
            "sum_y": 0.0,
            "sum_x2": 0.0,
            "sum_y2": 0.0,
            "sum_xy": 0.0,
        }

    for idx, res in enumerate(pair_results):
        if res is None:
            continue
        if "error" in res:
            warn(f"Node {idx} returned pair-stats error: {res['error']}")
            continue

        local_columns = res.get("columns")
        local_pairs = res.get("pair_stats")
        if local_columns is None or local_pairs is None:
            continue
        if list(local_columns) != valid_columns:
            return {"error": "Column mismatch in pair statistics round."}

        for key, entry in local_pairs.items():
            if key not in aggregated_pairs:
                continue
            for stat_name in aggregated_pairs[key]:
                aggregated_pairs[key][stat_name] += float(entry[stat_name])

    p = len(valid_columns)
    rho_matrix = np.full((p, p), np.nan, dtype=float)
    n_complete_matrix = np.zeros((p, p), dtype=int)
    pvalue_matrix = np.full((p, p), np.nan, dtype=float)

    for i, col in enumerate(valid_columns):
        n_complete_matrix[i, i] = int(global_summary[col]["count"])
        rho_matrix[i, i] = np.nan if col in constant_columns else 1.0

    pair_results_long = []
    for i, col1 in enumerate(valid_columns):
        for j in range(i + 1, p):
            col2 = valid_columns[j]
            s = aggregated_pairs[_pair_key(col1, col2)]
            n = int(s["n"])
            n_complete_matrix[i, j] = n_complete_matrix[j, i] = n

            if n < 2:
                rho = np.nan
                approx_p = np.nan
            else:
                num = n * s["sum_xy"] - s["sum_x"] * s["sum_y"]
                den_x = n * s["sum_x2"] - s["sum_x"] ** 2
                den_y = n * s["sum_y2"] - s["sum_y"] ** 2
                den = np.sqrt(max(den_x, 0.0) * max(den_y, 0.0))

                if den == 0.0:
                    rho = np.nan
                    approx_p = np.nan
                else:
                    rho = float(np.clip(num / den, -1.0, 1.0))
                    if approx_pvalues and n > 2 and abs(rho) < 1.0:
                        t_stat = rho * np.sqrt((n - 2) / (1.0 - rho ** 2))
                        approx_p = float(
                            2.0 * stats.t.sf(abs(t_stat), df=n - 2)
                        )
                    elif approx_pvalues and n > 2 and abs(rho) == 1.0:
                        approx_p = 0.0
                    else:
                        approx_p = np.nan

            rho_matrix[i, j] = rho_matrix[j, i] = rho
            pvalue_matrix[i, j] = pvalue_matrix[j, i] = approx_p

            pair_results_long.append({
                "column_x": col1,
                "column_y": col2,
                "n_complete": n,
                "spearman_rho_approx": rho,
                "approx_p_value": approx_p,
            })

    result = {
        "test": "federated_spearman_approx",
        "columns": valid_columns,
        "n_bins": int(n_bins),
        "approximate_ranking": True,
        "correlation_matrix": rho_matrix.tolist(),
        "n_complete_matrix": n_complete_matrix.tolist(),
        "approx_pvalue_matrix": (
            pvalue_matrix.tolist() if approx_pvalues else None
        ),
        "pairs": pair_results_long,
        "global_summary": global_summary,
    }

    info("Central federated Spearman correlation finished")
    return result
