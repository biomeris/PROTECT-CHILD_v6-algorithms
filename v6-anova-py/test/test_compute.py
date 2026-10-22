"""Vantage6 v5 MockNetwork test: two heterogeneous partitions versus SciPy."""
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import f_oneway
from vantage6.algorithm.mock.network import MockNetwork

data = pd.read_csv(Path(__file__).parent / "test_data.csv")
parts = [data.iloc[::2].copy(), data.iloc[1::2].copy()]
network = MockNetwork(
    datasets=[{"Database 1": {"database": part}} for part in parts],
    module_name="v6-anova-py",
)
client = network.user_client
orgs = [item["id"] for item in client.organization.list()]
db = [{"type": "dataframe", "dataframe_id": network.hq.dataframes[0]["id"]}]
task = client.task.create(
    method="central_function",
    arguments={"group_col": "Group", "features": ["age", "Height"], "organizations_to_include": orgs},
    organizations=[orgs[0]], databases=db,
)
replies = client.wait_for_results(task["id"])
actual = replies[0] if isinstance(replies, list) else replies
assert "error" not in actual, actual
for feature in ["age", "Height"]:
    frame = data[["Group", feature]].dropna()
    arrays = [part[feature].to_numpy(dtype=float) for _, part in frame.groupby("Group")]
    f, p = f_oneway(*arrays)
    got = actual["results"][feature]
    assert got["n_total"] == len(frame), got
    assert np.isclose(got["f_statistic"], f, rtol=1e-8, atol=1e-8), (feature, got, f)
    assert np.isclose(got["p_value"], p, rtol=1e-8, atol=1e-8), (feature, got, p)
print("PASS: federated ANOVA matches centralized SciPy for two partitions")
