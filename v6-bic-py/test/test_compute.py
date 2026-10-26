"""Three-organization Vantage6 v5 MockNetwork tests for all BIC input modes.

Demonstrates aggregation of a shared fitted model; the demonstration
probabilities are not asserted to come from an MLE. Formula agreement
alone is therefore not a proof that these are classical model-comparison
scores for a particular estimator.
"""
from pathlib import Path
import math
import pandas as pd
from vantage6.algorithm.mock.network import MockNetwork

data = pd.read_csv(Path(__file__).parent / "test_data.csv")
data["log_likelihood"] = [
    math.log(float(p)) if int(y) else math.log1p(-float(p))
    for y, p in zip(data["y_true"], data["p_positive"])
]
pieces = [data.iloc[:4].copy(), data.iloc[4:10].copy(), data.iloc[10:].copy()]
global_loglik = math.fsum(data["log_likelihood"].tolist())
global_n = len(data)
k = 3
expected = math.log(global_n) * k - 2 * global_loglik

for mode in ("contributions", "aggregated", "binary"):
    if mode == "aggregated":
        local_data = [
            pd.DataFrame({
                "log_likelihood": [math.fsum(part["log_likelihood"])],
                "n_observations": [len(part)],
                "model_id": ["global_model_demo"],
            })
            for part in pieces
        ]
    else:
        local_data = pieces

    network = MockNetwork(
        datasets=[{"Database 1": {"database": frame}} for frame in local_data],
        module_name="v6-bic-py",
    )
    client = network.user_client
    orgs = [organization["id"] for organization in client.organization.list()]
    databases = [{"type": "dataframe", "dataframe_id": network.hq.dataframes[0]["id"]}]

    task = client.task.create(
        method="central_function",
        arguments={
            "n_parameters": k,
            "input_mode": mode,
            "model_id_column": "model_id",
            "organizations_to_include": orgs,
        },
        organizations=[orgs[0]],
        databases=databases,
    )
    replies = client.wait_for_results(task["id"])
    result = replies[0] if isinstance(replies, list) else replies
    assert isinstance(result, dict) and "error" not in result, result
    assert result["criterion"] == "BIC", result
    assert result["n_observations"] == global_n, result
    assert result["n_parameters"] == k, result
    assert result["model_id"] == "global_model_demo", result
    assert result["model_fitted_by_algorithm"] is False, result
    assert math.isclose(result["log_likelihood"], global_loglik, rel_tol=1e-12, abs_tol=1e-12)
    assert math.isclose(result["bic"], expected, rel_tol=1e-12, abs_tol=1e-12), (mode, result, expected)
    print(f"PASS: BIC mode={mode}, n={global_n}, k={k}, score={result['bic']:.10f}")

print("PASS: all three BIC Vantage6 MockNetwork modes match centralized reference")
