"""Two-sample pooled-variance Student t-test using federated sufficient statistics."""
from typing import Any
from math import sqrt
from scipy.stats import t as t_distribution
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.client import AlgorithmClient


def _merge(items: list[dict]) -> dict | None:
    n = sum(int(item["n"]) for item in items)
    if n < 2:
        return None
    total = sum(float(item["sum"]) for item in items)
    total_sq = sum(float(item["sum_sq"]) for item in items)
    mean = total / n
    variance = max(0.0, (total_sq - total * total / n) / (n - 1))
    return {"n": n, "mean": mean, "variance": variance}


def _student(a: dict | None, b: dict | None) -> dict | None:
    if a is None or b is None or a["n"] < 2 or b["n"] < 2:
        return None
    na, nb = a["n"], b["n"]
    dof = na + nb - 2
    pooled = ((na - 1) * a["variance"] + (nb - 1) * b["variance"]) / dof
    standard_error = sqrt(pooled * (1 / na + 1 / nb))
    if standard_error == 0:
        return None
    statistic = (a["mean"] - b["mean"]) / standard_error
    return {
        "t_score": float(statistic),
        "p_value": float(2 * t_distribution.sf(abs(statistic), df=dof)),
        "df": dof,
        "n_group1": na,
        "n_group2": nb,
        "mean_group1": a["mean"],
        "mean_group2": b["mean"],
    }


@central
@algorithm_client
def central_function(
    client: AlgorithmClient,
    organizations_to_include: list[int] | None = None,
    columns: list[str] | None = None,
    group_col: str | None = None,
) -> dict[str, Any]:
    """Pooled independent-samples t-test (not Welch's t-test).

    When group_col is given, compare the two global groups across all nodes.
    Otherwise compare two organizations in the supplied order.
    """
    available = [org["id"] for org in client.organization.list()]
    org_ids = available if organizations_to_include is None else list(organizations_to_include)
    if not org_ids or len(set(org_ids)) != len(org_ids) or not set(org_ids).issubset(set(available)):
        return {"error": "Specify distinct organization IDs in the collaboration."}
    if not group_col and len(org_ids) != 2:
        return {"error": "Node-versus-node mode requires exactly 2 organizations."}

    task = client.task.create(
        method="federated_function",
        arguments={"columns": columns, "group_col": group_col},
        organizations=org_ids,
        name="Federated Student t-test sufficient statistics",
        description="Local n, sum and sum of squares for two-sample t-test",
    )
    responses = client.wait_for_results(task_id=task["id"])
    if not isinstance(responses, list) or len(responses) != len(org_ids):
        return {"error": "Incomplete node results."}
    for index, response in enumerate(responses):
        if not isinstance(response, dict) or response.get("error"):
            return {"error": f"Organization result {index}: {response}"}

    output = {}
    if group_col:
        groups = sorted({group for response in responses for group in response.get("stats", {})})
        if len(groups) != 2:
            return {"error": f"Exactly two nonempty global groups required, got {groups}."}
        a, b = groups
        feature_sets = [
            set(col for group in response["stats"].values() for col in group)
            for response in responses
        ]
        features = sorted(set().union(*feature_sets))
        for feature in features:
            stats_a = [response["stats"][a][feature] for response in responses
                       if a in response["stats"] and feature in response["stats"][a]]
            stats_b = [response["stats"][b][feature] for response in responses
                       if b in response["stats"] and feature in response["stats"][b]]
            result = _student(_merge(stats_a), _merge(stats_b))
            if result is not None:
                result["groups"] = [a, b]
                output[feature] = result
    else:
        features = sorted(set(responses[0]["stats"]) & set(responses[1]["stats"]))
        for feature in features:
            a = _merge([responses[0]["stats"][feature]])
            b = _merge([responses[1]["stats"][feature]])
            result = _student(a, b)
            if result is not None:
                result["organizations"] = list(org_ids)
                output[feature] = result

    if not output:
        return {"error": "No valid columns/groups with at least 2 observations each."}
    return output
