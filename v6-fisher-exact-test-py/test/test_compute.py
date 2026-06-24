"""
Run this script to test you compute function locally (without building a Docker image)
using the mock client.

Run as:

    python test_compute.py

Make sure to do so in an environment where `vantage6-algorithm-tools` is
installed. This can be done by running:

    pip install vantage6-algorithm-tools
"""

import pandas as pd

from vantage6.algorithm.mock.network import MockNetwork
from pathlib import Path

# get path of current directory
current_path = Path(__file__).parent

# The MockNetwork expects a list of datasets. In this instance we are not interested in
# extracting the data from its source. Therefore, we supply the data as a Pandas
# dataframe avoiding the need to extract the data first
data1 = pd.read_csv(current_path / "test_data_1.csv")
data2 = pd.read_csv(current_path / "test_data_2.csv")
data3 = pd.read_csv(current_path / "test_data_3.csv")
data4 = pd.read_csv(current_path / "test_data_4.csv")
DATABASE_LABEL = "default"

# Create a MockNetwork with identical datasets for three nodes
network = MockNetwork(
    datasets=[
        {DATABASE_LABEL: {"database": data1}},
        {DATABASE_LABEL: {"database": data2}},
        {DATABASE_LABEL: {"database": data3}},
        {DATABASE_LABEL: {"database": data4}},
    ],
    module_name="v6-fisher-exact-test-py",
)

# Once the network is created, we can get the client to interact with the MockNetwork.
client = network.user_client

# List mock organizations
organizations = client.organization.list()
print(organizations)
org_ids = [organization["id"] for organization in organizations]

group_column = "Gender"
outcome_column = "Outcome"

# Run the central method on 1 node and get the results
central_task = client.task.create(
    method="compute_fisher_exact_test",
    arguments={"group_column": group_column, "outcome_column": outcome_column},
    organizations=[org_ids[0]],
    databases=[{"type": "dataframe", "dataframe_id": network.hq.dataframes[0]["id"]}],
)
results = client.wait_for_results(central_task.get("id"))
print(results)

# # Run the federated method for all organizations
# task = client.task.create(
#     method="get_unique_values",
#     arguments={
#         "group_column": group_column,
#         "outcome_column": outcome_column,
#     },
#     organizations=org_ids,
#     databases=[{"type": "dataframe", "dataframe_id": network.hq.dataframes[0]["id"]}],
# )
# print(task)

# # Get the results from the task
# results = client.wait_for_results(task.get("id"))
# print(results)
