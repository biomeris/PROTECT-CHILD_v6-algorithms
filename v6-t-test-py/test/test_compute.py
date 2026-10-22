"""Vantage6 v5 end-to-end t-test compared with SciPy Student's test."""
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import ttest_ind
from vantage6.algorithm.mock.network import MockNetwork

root = Path(__file__).parent
a = pd.read_csv(root / "data_org1_(2_groups).csv")
b = pd.read_csv(root / "data_org2_0_count_(2_groups).csv")
network = MockNetwork(
    datasets=[{"Database 1": {"database": d}} for d in (a, b)],
    module_name="v6-t-test-py",
)
client = network.user_client
orgs = [item["id"] for item in client.organization.list()]
database = [{"type": "dataframe", "dataframe_id": network.hq.dataframes[0]["id"]}]

def run(group_col):
    task = client.task.create(
        method="central_function",
        arguments={"organizations_to_include": orgs, "columns": ["age"], "group_col": group_col},
        organizations=[orgs[0]], databases=database,
    )
    replies = client.wait_for_results(task["id"])
    return replies[0] if isinstance(replies, list) else replies

grouped = run("Group")
assert "error" not in grouped, grouped
combined = pd.concat([a, b], ignore_index=True)
labels = sorted(combined["Group"].dropna().unique())
x = combined.loc[combined["Group"] == labels[0], "age"].dropna()
y = combined.loc[combined["Group"] == labels[1], "age"].dropna()
exact = ttest_ind(x, y, equal_var=True)
assert np.isclose(grouped["age"]["t_score"], exact.statistic, atol=1e-8)
assert np.isclose(grouped["age"]["p_value"], exact.pvalue, atol=1e-8)

legacy = run(None)
assert "error" not in legacy, legacy
exact_legacy = ttest_ind(a["age"].dropna(), b["age"].dropna(), equal_var=True)
assert np.isclose(legacy["age"]["t_score"], exact_legacy.statistic, atol=1e-8)
assert np.isclose(legacy["age"]["p_value"], exact_legacy.pvalue, atol=1e-8)
print("PASS: federated two-sample t-test matches SciPy in both modes")
