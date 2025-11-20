"""
Run this script to test your algorithm locally (without building a Docker
image) using the mock client.

Run as:

    python test.py

Make sure to do so in an environment where `vantage6-algorithm-tools` is
installed. This can be done by running:

    pip install vantage6-algorithm-tools
"""

from vantage6.algorithm.tools.mock_client import MockAlgorithmClient
from pathlib import Path

# get path of current directory
current_path = Path(__file__).parent

## Mock client
client = MockAlgorithmClient(
    datasets=[
        # Data for first organization
        [
            {
                "database": current_path / "data_org1_(2_groups).csv",
                "db_type": "csv",
                "input_data": {},
            }
        ],
        # Data for second organization
        [
            {
                "database": current_path / "data_org2_0_count_(2_groups).csv",
                "db_type": "csv",
                "input_data": {},
            }
        ],
    ],
    module="v6-t-test-py",
)

# list mock organizations
organizations = client.organization.list()
print(organizations)
org_ids = [organization["id"] for organization in organizations]
print(org_ids)

# Run the central method on 1 node and get the results
central_task = client.task.create(
    input_={
        "method": "central",
        "kwargs": {"organizations_to_include": org_ids, 'columns':['age'], "group_col": 'Group'},
    },
    organizations=[org_ids[0]],
)
results = client.wait_for_results(central_task.get("id"))
print(results)

"""# Run the partial method for all organizations
task = client.task.create(
    input_={
        "method": "partial",
        "kwargs": {
            # "columns": "age",
        },
    },
    organizations=org_ids,
)
print(task)

# Get the results from the task
results = client.wait_for_results(task.get("id"))
print(results)"""
