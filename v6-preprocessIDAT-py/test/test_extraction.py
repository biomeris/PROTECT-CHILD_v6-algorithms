"""
Run this script to test you extraction function locally (without building a Docker
image) using the mock client.

Run as:

    python test_extraction.py

Make sure to do so in an environment where `vantage6-algorithm-tools` is
installed. This can be done by running:

    pip install vantage6-algorithm-tools
"""
from vantage6.algorithm.mock.network import MockNetwork
from pathlib import Path

# get path of current directory
current_path = Path(__file__).parent

# The MockNetwork expects a list of datasets. In the case of an extraction job, this
# needs to an URI. In this example, we use a CSV file that was included in this
# template. In case you want to connect to a database you need to make sure that the
# database is reachable.
DATABASE_LABEL = "default"
network = MockNetwork(
    datasets=[
        {
            DATABASE_LABEL: {
                "database": current_path / "test_data.csv",
                "db_type": "csv",
            },
        },
        {
            DATABASE_LABEL: {
                "database": current_path / "test_data.csv",
                "db_type": "csv",
            },
        },
        {
            DATABASE_LABEL: {
                "database": current_path / "test_data.csv",
                "db_type": "csv",
            },
        },
    ],
    module_name="preprocessIDAT"
)

# Once the network is created, we can get the client to interact with the MockNetwork.
client = network.user_client

# List mock organizations
organizations = client.organization.list()
print(organizations)
org_ids = [organization["id"] for organization in organizations]

# Run the data extraction function
task = client.dataframe.create(
    method="data_extraction_function",
    arguments={
        # TODO add sensible values
        "arg1": "some_value",

    },
    organizations=org_ids,
    label=DATABASE_LABEL,
)

# Wait for the task to complete
results = client.wait_for_results(task.get("id"))
print("results:", results)

print("dataframes:")
for node in network.nodes:
    print(node.dataframes)
