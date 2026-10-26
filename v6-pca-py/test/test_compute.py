"""Check distributed covariance and eigenvalues against centralized numpy."""
from pathlib import Path
import numpy as np
import pandas as pd
from vantage6.algorithm.mock.network import MockNetwork

data = pd.read_csv(Path(__file__).parent / "test_data.csv")
parts = [data.iloc[::2].copy(), data.iloc[1::2].copy()]
network = MockNetwork(
    datasets=[{"Database 1": {"database": part}} for part in parts],
    module_name="v6-pca-py",
)
client = network.user_client
orgs = [org["id"] for org in client.organization.list()]
database = [{"type": "dataframe", "dataframe_id": network.hq.dataframes[0]["id"]}]
features = ["Age", "Height(in)", "Weight(lbs)"]
task = client.task.create(
    method="central_function",
    arguments={"features": features, "n_components": 3, "center": True, "organizations_to_include": orgs},
    organizations=[orgs[0]], databases=database,
)
replies = client.wait_for_results(task["id"])
actual = replies[0] if isinstance(replies, list) else replies
assert "error" not in actual, actual
values = data[features].dropna().to_numpy(dtype=float)
cov = np.cov(values, rowvar=False, ddof=1)
assert actual["n_total"] == len(values)
assert np.allclose(actual["covariance"], cov, atol=1e-8)
expected = np.linalg.eigvalsh(cov)[::-1]
assert np.allclose(actual["explained_variance"], expected, atol=1e-8)
print("PASS: federated PCA covariance and eigenvalues match NumPy")
