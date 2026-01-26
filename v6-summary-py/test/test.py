"""
Run this script to test your algorithm locally (without building a Docker
image) using the mock client.

Run as:

    python test.py

Make sure to do so in an environment where `vantage6-algorithm-tools` is
installed. This can be done by running:

    pip install vantage6-algorithm-tools
"""

import pandas as pd

from vantage6.algorithm.tools.util import info
from vantage6.algorithm.tools.mock_client import MockAlgorithmClient

df1 = pd.read_csv("./data_org1.csv")
df2 = pd.read_csv("./data_org2.csv")

## Mock client
client = MockAlgorithmClient(
    datasets=[
        # Data for first organization
        [
            {
                "database": df1,
            }
        ],
        # Data for second organization
        [
            {
                "database": df2,
            }
        ],
    ],
    module="v6-summary-py",
)

# list mock organizations
organizations = client.organization.list()
org_ids = [organization["id"] for organization in organizations]

# Run the partial method for all organizations
# Define columns to compute the summary statistics for
# numeric_columns = ["age", "Height", "Weight"]

# input_ = {
#    "method": "summary_per_data_station",
#    "kwargs": {
#        # "numeric_columns": numeric_columns,
#    },
# }

# task = client.task.create(
#     input_=input_,
#     organizations=org_ids,
#     name="Subtask summary",
#     description="Compute summary per data station",
# )

# # wait for node to return results of the subtask.
# info("Waiting for results")
# results = client.wait_for_results(task_id=task.get("id"))
# info("Results obtained!")

# print(results)

# Run the central method on 1 node and get the results
central_task = client.task.create(
    input_={"method": "summary", "kwargs": {}},
    organizations=[org_ids[0]],
)
results = client.wait_for_results(central_task.get("id"))
print(results)
