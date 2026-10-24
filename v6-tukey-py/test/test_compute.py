"""Vantage6 v5 MockNetwork test of federated Tukey-Kramer HSD."""
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import studentized_range
from vantage6.algorithm.mock.network import MockNetwork

data = pd.read_csv(Path(__file__).parent / "test_data.csv")
parts = [data.iloc[::2].copy(), data.iloc[1::2].copy()]
network = MockNetwork(
    datasets=[{"Database 1": {"database": part}} for part in parts],
    module_name="v6-tukey-py",
)
client = network.user_client
orgs = [org["id"] for org in client.organization.list()]
database = [{"type": "dataframe", "dataframe_id": network.hq.dataframes[0]["id"]}]
task = client.task.create(
    method="central_function",
    arguments={
        "group_col": "Group", "features": ["age", "Height"],
        "alpha": 0.05, "organizations_to_include": orgs,
    },
    organizations=[orgs[0]], databases=database,
)
responses = client.wait_for_results(task["id"])
actual = responses[0] if isinstance(responses, list) else responses
assert "error" not in actual, actual
valid = data[["Group", "age", "Height"]].dropna()
for feature in ["age", "Height"]:
    item = actual["results"][feature]
    levels = sorted(valid["Group"].unique())
    assert item["n_groups"] == len(levels)
    n = len(valid)
    k = len(levels)
    sse = sum(((sub[feature] - sub[feature].mean()) ** 2).sum()
              for _, sub in valid.groupby("Group"))
    mse = sse / (n-k)
    assert np.isclose(item["mse"], mse, atol=1e-8)
    for pair in item["comparisons"]:
        a = valid.loc[valid["Group"] == pair["group1"], feature].to_numpy(float)
        b = valid.loc[valid["Group"] == pair["group2"], feature].to_numpy(float)
        se = np.sqrt(mse / 2 * (1 / len(a) + 1 / len(b)))
        q = abs(a.mean() - b.mean()) / se
        p = studentized_range.sf(q, k, n-k)
        assert np.isclose(pair["q_stat"], q, atol=1e-8)
        assert np.isclose(pair["p_value"], p, atol=1e-8)
print("PASS: federated Tukey-Kramer matches centralized sufficient-statistics formula")
