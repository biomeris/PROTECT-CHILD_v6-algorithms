"""
Run this script to test your compute function locally (without building a
Docker image) using the mock client.

Run as:

    python test_compute.py

Make sure to do so in an environment where `vantage6-algorithm-tools` is
installed. This can be done by running:

    pip install vantage6-algorithm-tools
"""

import pandas as pd

from vantage6.algorithm.mock.network import MockNetwork

DATABASE_LABEL = "default"

# The Bonferroni correction does not touch node data at all, so a single
# minimal (empty-ish) dataset per mock node is enough to stand up the network.
dummy_data = pd.DataFrame({"col": [1, 2, 3]})

network = MockNetwork(
    datasets=[
        {DATABASE_LABEL: {"database": dummy_data}},
    ],
    module_name="v6-bonferroni-py",
)

client = network.user_client
organizations = client.organization.list()
print(organizations)
org_ids = [organization["id"] for organization in organizations]

p_values = {
    "site_a": 0.01,
    "site_b": 0.20,
    "site_c": 0.001,
}

central_task = client.task.create(
    method="compute_bonferroni_correction",
    arguments={"p_values": p_values, "alpha": 0.05},
    organizations=[org_ids[0]],
    databases=[{"type": "dataframe", "dataframe_id": network.hq.dataframes[0]["id"]}],
)
results = client.wait_for_results(central_task.get("id"))
print(results)
