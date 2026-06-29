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
data = pd.read_csv(current_path / "test_data.csv")
DATABASE_LABEL = "default"

# Create a MockNetwork with identical datasets for three nodes
network = MockNetwork(
    datasets=[
        {DATABASE_LABEL: {"database": data}},
        {DATABASE_LABEL: {"database": data}},
        {DATABASE_LABEL: {"database": data}},
    ],
    module_name="extractionIDAT"
)

# Once the network is created, we can get the client to interact with the MockNetwork.
client = network.user_client

# List mock organizations
organizations = client.organization.list()
print(organizations)
org_ids = [organization["id"] for organization in organizations]

# Run the central method on 1 node and get the results
central_task = client.task.create(
    method="central_function",
    arguments={
        # TODO add sensible values
        "arg1": "some_value",

    },
    organizations=[org_ids[0]],
    databases=[
        {"type": "dataframe", "dataframe_id": network.server.dataframes[0]["id"]}
    ],
)
results = client.wait_for_results(central_task.get("id"))
print(results)

# Run the federated method for all organizations
task = client.task.create(
    method="federated_function",
    arguments={
        # TODO add sensible values
        "arg1": "some_value",
    },
    organizations=org_ids,
    databases=[
        {"type": "dataframe", "dataframe_id": network.server.dataframes[0]["id"]}
    ],
)
print(task)

# Get the results from the task
results = client.wait_for_results(task.get("id"))
print(results)
