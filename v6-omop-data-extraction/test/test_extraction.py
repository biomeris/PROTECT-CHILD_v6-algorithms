"""
Run this script to test your data extraction function locally (without building a
Docker image) using MockNetwork.

Run as:

    uv sync --group dev
    uv run python test/test_extraction.py

Requires vantage6-algorithm-tools.
"""
from pathlib import Path

from vantage6.algorithm.mock.network import MockNetwork

current_path = Path(__file__).parent

# The MockNetwork expects a list of datasets. In the case of an extraction job, this
# needs to an URI. In this example, we use a CSV file that was included in this
# template. In case you want to connect to a database you need to make sure that the
# database is reachable.
# For extraction tests, provide a file URI and db_type per node.
DATABASE_LABEL = "Database"
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
    ],
    module_name="v6-omop-data-extraction",
)

# Once the network is created, we can get the client to interact with the MockNetwork.
client = network.user_client

organizations = client.organization.list()
print(organizations)
org_ids = [organization["id"] for organization in organizations]

dataframe = client.dataframe.create(
    label=DATABASE_LABEL,
    method="cohort_data_extraction",
    arguments={
        # TODO add sensible values
        "cohort_id": "some_value",

    },
)
print("dataframe:", dataframe)

print("dataframes per node:")
for node in network.nodes:
    print(node.dataframes)
