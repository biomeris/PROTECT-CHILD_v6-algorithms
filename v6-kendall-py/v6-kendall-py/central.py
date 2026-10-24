from itertools import combinations
from typing import Any, Dict, List, Optional

import numpy as np

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.client import AlgorithmClient


def _pair_key(col1: str, col2: str) -> str:
    return f"{col1}|||{col2}"


def _make_edges(min_val: float, max_val: float, n_bins: int) -> List[float]:
    if not np.isfinite(min_val) or not np.isfinite(max_val):
        raise ValueError("Non-finite min/max encountered while creating bin edges.")
    if max_val == min_val:
        return [float(min_val - 0.5), float(max_val + 0.5)]
    return np.linspace(min_val, max_val, n_bins + 1).astype(float).tolist()


def _kendall_tau_b_from_table(table: np.ndarray) -> Dict[str, Any]:
    table = np.asarray(table, dtype=float)
    n = float(table.sum())

    if n < 2:
        return {
            "n_complete": int(n),
            "tau_b_approx": np.nan,
            "concordant_pairs": 0.0,
            "discordant_pairs": 0.0,
            "ties_x": 0.0,
            "ties_y": 0.0,
        }

    rows, cols = table.shape
    row_sums = table.sum(axis=1)
    col_sums = table.sum(axis=0)

    n0 = n * (n - 1.0) / 2.0
    n1 = np.sum(row_sums * (row_sums - 1.0) / 2.0)
    n2 = np.sum(col_sums * (col_sums - 1.0) / 2.0)

    concordant = 0.0
    discordant = 0.0

    for i in range(rows):
        for j in range(cols):
            nij = table[i, j]
            if nij == 0:
                continue
            if i + 1 < rows and j + 1 < cols:
                concordant += nij * table[i + 1 :, j + 1 :].sum()
            if i + 1 < rows and j > 0:
                discordant += nij * table[i + 1 :, :j].sum()

    denom = np.sqrt(max(n0 - n1, 0.0) * max(n0 - n2, 0.0))
    tau_b = np.nan if denom == 0 else (concordant - discordant) / denom

    return {
        "n_complete": int(n),
        "tau_b_approx": float(tau_b) if np.isfinite(tau_b) else np.nan,
        "concordant_pairs": float(concordant),
        "discordant_pairs": float(discordant),
        "ties_x": float(n1),
        "ties_y": float(n2),
    }


@central
@algorithm_client
def central_function(
    client: AlgorithmClient,
    columns: Optional[List[str]] = None,
    n_bins: int = 20,
    organizations_to_include: Optional[List[int]] = None,
) -> Any:
    info("Starting central federated Kendall tau-b")

    organizations = client.organization.list()
    available_org_ids = [organization.get("id") for organization in organizations]
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

    info("Round 1/2 - collecting local summaries")
    task = client.task.create(
        method="federated_function",
        arguments={"mode": "summary", "columns": columns},
        organizations=org_ids,
        name="Federated Kendall summaries",
        description="Collect local min/max/count per feature",
    )
    summary_results = client.wait_for_results(task_id=task.get("id"))
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

    global_edges: Dict[str, List[float]] = {}
    constant_columns = set()
    for col in valid_columns:
        min_val = float(global_summary[col]["min"])
        max_val = float(global_summary[col]["max"])
        if min_val == max_val:
            constant_columns.add(col)
        global_edges[col] = _make_edges(min_val, max_val, n_bins)

    info("Round 2/2 - collecting local contingency tables")
    task = client.task.create(
        method="federated_function",
        arguments={
            "mode": "contingency",
            "columns": valid_columns,
            "bin_edges": global_edges,
        },
        organizations=org_ids,
        name="Federated Kendall contingency tables",
        description="Collect local 2D contingency tables for Kendall tau-b",
    )
    contingency_results = client.wait_for_results(task_id=task.get("id"))
    if not contingency_results:
        return {"error": "No results received in contingency round."}

    aggregated_tables: Dict[str, np.ndarray] = {}
    for col1, col2 in combinations(valid_columns, 2):
        aggregated_tables[_pair_key(col1, col2)] = np.zeros(
            (len(global_edges[col1]) - 1, len(global_edges[col2]) - 1),
            dtype=float,
        )

    for idx, res in enumerate(contingency_results):
        if res is None:
            continue
        if "error" in res:
            warn(f"Node {idx} returned contingency error: {res['error']}")
            continue

        local_columns = res.get("columns")
        local_tables = res.get("contingency_tables")
        if local_columns is None or local_tables is None:
            continue
        if list(local_columns) != valid_columns:
            return {"error": "Column mismatch in contingency round."}

        for key, table in local_tables.items():
            if key in aggregated_tables:
                aggregated_tables[key] += np.asarray(table, dtype=float)

    p = len(valid_columns)
    tau_matrix = np.full((p, p), np.nan, dtype=float)
    n_complete_matrix = np.zeros((p, p), dtype=int)

    for i, col in enumerate(valid_columns):
        tau_matrix[i, i] = np.nan if col in constant_columns else 1.0
        n_complete_matrix[i, i] = int(global_summary[col]["count"])

    pair_results = []
    for i, col1 in enumerate(valid_columns):
        for j in range(i + 1, p):
            col2 = valid_columns[j]
            stats_ = _kendall_tau_b_from_table(
                aggregated_tables[_pair_key(col1, col2)]
            )
            tau = stats_["tau_b_approx"]
            n_complete = stats_["n_complete"]

            tau_matrix[i, j] = tau_matrix[j, i] = tau
            n_complete_matrix[i, j] = n_complete_matrix[j, i] = n_complete

            pair_results.append({
                "column_x": col1,
                "column_y": col2,
                **stats_,
            })

    result = {
        "test": "federated_kendall_tau_b_approx",
        "columns": valid_columns,
        "n_bins": int(n_bins),
        "approximate": True,
        "tau_matrix": tau_matrix.tolist(),
        "n_complete_matrix": n_complete_matrix.tolist(),
        "pairs": pair_results,
        "global_summary": global_summary,
    }
    info("Central federated Kendall tau-b finished")
    return result
