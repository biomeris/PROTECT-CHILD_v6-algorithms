"""Exact federated one-way ANOVA based on per-group sufficient statistics."""
from typing import Any
import math
from scipy.stats import f as f_distribution
from vantage6.algorithm.client import AlgorithmClient
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.tools.util import info


@central
@algorithm_client
def central_function(
    client: AlgorithmClient,
    group_col: str | None = None,
    groups: list[str] | None = None,
    features: list[str] | None = None,
    organizations_to_include: list[int] | None = None,
) -> dict[str, Any]:
    """Fit exact one-way ANOVA for each numeric feature.

    'groups' is accepted as a backwards-compatible alias; only the first
    grouping column is supported.
    """
    group_col = group_col or (groups[0] if groups else None)
    if not group_col:
        return {"error": "Provide a grouping column using 'group_col'."}
    if not group_col.strip():
        return {"error": "Grouping column cannot be blank."}

    available = [item["id"] for item in client.organization.list()]
    if organizations_to_include is None:
        org_ids = available
    else:
        org_ids = list(organizations_to_include)
        if not set(org_ids).issubset(set(available)):
            return {"error": "Unknown organization requested."}
    if not org_ids or len(set(org_ids)) != len(org_ids):
        return {"error": "Supply a nonempty list of distinct organizations."}

    task = client.task.create(
        method="federated_function",
        arguments={"group_col": group_col, "features": features},
        organizations=org_ids,
        name="Federated ANOVA sufficient statistics",
        description="Local group counts, sums and sums of squares",
    )
    replies = client.wait_for_results(task_id=task["id"])
    if not isinstance(replies, list) or len(replies) != len(org_ids):
        return {"error": "Missing or unexpected number of node results."}

    columns = None
    agg: dict[str, dict[str, dict[str, Any]]] = {}
    for index, reply in enumerate(replies):
        if not isinstance(reply, dict) or "error" in reply:
            return {"error": f"Organization result {index}: {reply}"}
        if reply.get("group_col") != group_col:
            return {"error": "Grouping column mismatch between nodes."}
        local_columns = reply.get("columns")
        if not isinstance(local_columns, list) or not isinstance(reply.get("stats"), dict):
            return {"error": "Malformed node result."}
        if columns is None:
            columns = local_columns
            agg = {feature: {} for feature in columns}
        elif columns != local_columns:
            return {"error": "Feature list/order differs between organizations."}
        for feature in columns:
            for entry in reply["stats"].get(feature, []):
                group = entry["group"]
                key = str(group)
                slot = agg[feature].setdefault(
                    key, {"group": group, "n": 0, "sum": 0.0, "sum_sq": 0.0}
                )
                slot["n"] += int(entry["n"])
                slot["sum"] += float(entry["sum"])
                slot["sum_sq"] += float(entry["sum_sq"])

    if not columns:
        return {"error": "No numeric features were returned."}

    output: dict[str, Any] = {}
    for feature in columns:
        groups_out = []
        for entry in agg[feature].values():
            n = entry["n"]
            if n <= 0:
                continue
            avg = entry["sum"] / n
            within = max(0.0, entry["sum_sq"] - entry["sum"] ** 2 / n)
            groups_out.append({
                "group": entry["group"], "n": n, "mean": avg,
                "variance": within / (n - 1) if n > 1 else None,
                "sse": within,
            })
        groups_out.sort(key=lambda item: str(item["group"]))
        k = len(groups_out)
        n_total = sum(g["n"] for g in groups_out)
        if k < 2 or n_total <= k:
            output[feature] = {"error": "At least 2 groups and positive residual degrees of freedom required."}
            continue
        grand_mean = sum(g["n"] * g["mean"] for g in groups_out) / n_total
        ss_between = sum(g["n"] * (g["mean"] - grand_mean) ** 2 for g in groups_out)
        ss_within = sum(g["sse"] for g in groups_out)
        df_between, df_within = k - 1, n_total - k
        ms_between, ms_within = ss_between / df_between, ss_within / df_within
        if ms_within == 0:
            f_stat = None if ms_between == 0 else float("inf")
            p_value = None if ms_between == 0 else 0.0
        else:
            f_stat = ms_between / ms_within
            p_value = float(f_distribution.sf(f_stat, df_between, df_within))
        output[feature] = {
            "n_total": n_total, "n_groups": k,
            "grand_mean": grand_mean, "df_between": df_between, "df_within": df_within,
            "ss_between": ss_between, "ss_within": ss_within,
            "ms_between": ms_between, "ms_within": ms_within,
            "f_statistic": f_stat, "p_value": p_value,
            "group_statistics": groups_out,
        }
    info("Federated ANOVA V5 completed")
    return {"test": "federated_anova", "group_col": group_col, "columns": columns, "results": output}
