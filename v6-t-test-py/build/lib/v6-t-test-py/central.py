from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np
from scipy.stats import t

from vantage6.algorithm.client import AlgorithmClient
from vantage6.algorithm.tools.decorators import algorithm_client
from vantage6.algorithm.tools.exceptions import UserInputError
from vantage6.algorithm.tools.util import info


def _combine_stats(stats_list: List[Dict[str, float]]) -> Optional[Dict[str, float]]:
    """
    Combina statistiche per-sito (media, varianza campionaria, count) in un
    riassunto globale per un gruppo.

    Ogni elemento di stats_list è:
        {"average": float, "variance": float, "count": float}

    Ritorna:
        {"average": float, "variance": float, "count": float} oppure None se n<=1.
    """
    total_n = sum(s["count"] for s in stats_list)
    if total_n <= 1:
        return None

    global_mean = sum(s["average"] * s["count"] for s in stats_list) / total_n

    # corrected total sum of squares:
    total_ss = sum(
        (s["count"] - 1) * s["variance"] + s["count"] * (s["average"] - global_mean) ** 2
        for s in stats_list
    )
    global_var = total_ss / (total_n - 1)

    return {"average": float(global_mean), "variance": float(global_var), "count": float(total_n)}


def _pooled_t_test(group_a: Dict[str, float], group_b: Dict[str, float]) -> Optional[Tuple[float, float]]:
    """
    t-test a due campioni con varianza pooled usando statistiche riassuntive.
    Ritorna (t_score, p_value) oppure None se non calcolabile.
    """
    n_a, n_b = group_a["count"], group_b["count"]
    if n_a < 2 or n_b < 2:
        return None

    mean_a, mean_b = group_a["average"], group_b["average"]
    var_a, var_b = group_a["variance"], group_b["variance"]

    pooled_var = (((n_a - 1) * var_a) + ((n_b - 1) * var_b)) / (n_a + n_b - 2)
    denom = (pooled_var / n_a + pooled_var / n_b) ** 0.5
    if denom == 0:
        return None

    t_score = (mean_a - mean_b) / denom
    dof = int(n_a + n_b - 2)
    p_value = float(2 * (1 - t.cdf(np.abs(t_score), dof)))
    return float(t_score), p_value


@algorithm_client
def central(
    client: AlgorithmClient,
    organizations_to_include: List[int],
    columns: Optional[List[str]] = None,
    group_col: Optional[str] = None,
) -> Dict[str, Dict[str, float]]:
    """
    Two-sample t-test.

    Caso A (nuovo, consigliato): `group_col` fornito
      → t-test tra i due livelli di `group_col`, aggregando TUTTI i centri.

    Caso B (legacy): `group_col` assente
      → confronto nodo vs nodo: tutti i record dell'org[0] vs tutti i record dell'org[1].
         (richiede esattamente 2 organizzazioni)

    Output: { "<col>": {"t_score": float, "p_value": float}, ... }
    """
    if not organizations_to_include:
        raise UserInputError("Provide at least one organization id in 'organizations_to_include'.")

    # ------------------------------
    # Case A: per-group aggregation
    # ------------------------------
    if group_col:
        info(f"Running aggregated two-sample t-test using group_col='{group_col}' across ALL organizations.")

        subtask_input = {"method": "partial", "kwargs": {"columns": columns, "group_col": group_col}}
        task = client.task.create(
            input_=subtask_input,
            organizations=organizations_to_include,
            name="Subtask: per-group mean/variance/count",
            description=(
                "Compute local mean, sample variance, and count per group "
                f"for {columns if columns else 'all numeric columns'}."
            ),
        )
        results = client.wait_for_results(task_id=task.get("id"))
        if not results:
            raise UserInputError("No results received from the organizations.")

        # results: list per-org; each org_result: { <group_val>: { <col>: {stats} } }
        global_groups = set().union(*(org_result.keys() for org_result in results))
        if len(global_groups) != 2:
            raise UserInputError(
                f"Exactly 2 distinct groups in '{group_col}' are required globally, found: {sorted(global_groups)}"
            )
        group_a, group_b = sorted(global_groups)

        # union of columns
        all_columns = set().union(
            *(set(org_result.get(group_a, {}).keys()) for org_result in results),
            *(set(org_result.get(group_b, {}).keys()) for org_result in results),
        )

        out: Dict[str, Dict[str, float]] = {}
        for col in sorted(all_columns):
            a_list = [
                org_result[group_a][col]
                for org_result in results
                if group_a in org_result and col in org_result[group_a]
            ]
            b_list = [
                org_result[group_b][col]
                for org_result in results
                if group_b in org_result and col in org_result[group_b]
            ]
            if not a_list or not b_list:
                continue

            comb_a = _combine_stats(a_list)
            comb_b = _combine_stats(b_list)
            if not comb_a or not comb_b:
                continue

            t_res = _pooled_t_test(comb_a, comb_b)
            if not t_res:
                continue
            t_score, p_value = t_res
            out[col] = {"t_score": t_score, "p_value": p_value}

        return out

    # ------------------------------
    # Case B: LEGACY node vs node
    # ------------------------------
    if len(organizations_to_include) != 2:
        raise UserInputError(
            "Legacy mode requires exactly two organizations when 'group_col' is not provided: org[0] vs org[1]."
        )

    info("Running legacy two-sample t-test: ALL records from org[0] vs org[1] (no group_col).")

    # Ask to each node global stats (without groups)
    subtask_input = {"method": "partial", "kwargs": {"columns": columns, "group_col": None}}
    task = client.task.create(
        input_=subtask_input,
        organizations=organizations_to_include,
        name="Subtask: overall mean/variance/count (legacy, no group_col)",
        description=(
            "Compute local overall mean, sample variance, and count "
            f"for {columns if columns else 'all numeric columns'}."
        ),
    )
    results = client.wait_for_results(task_id=task.get("id"))
    if not results or len(results) != 2:
        raise UserInputError("Expected results from exactly two organizations in legacy mode.")

    org_a_stats, org_b_stats = results[0], results[1]

    # columns present in at least one of the two
    all_columns = sorted(set(org_a_stats.keys()) | set(org_b_stats.keys()))

    out: Dict[str, Dict[str, float]] = {}
    for col in all_columns:
        if col not in org_a_stats or col not in org_b_stats:
            continue
        t_res = _pooled_t_test(org_a_stats[col], org_b_stats[col])
        if not t_res:
            continue
        t_score, p_value = t_res
        out[col] = {"t_score": t_score, "p_value": p_value}

    return out
