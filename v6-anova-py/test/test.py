"""
Run this script to test your federated ANOVA algorithm locally (without building a Docker
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

# Mock client for federated ANOVA
client = MockAlgorithmClient(
    datasets=[
        # Data for first organization
        [{
            "database": current_path / "test_data.csv",  # Update path if needed
            "db_type": "csv",
            "input_data": {}
        }],
        # Data for second organization
        [{
            "database": current_path / "test_data.csv",  # Update path if needed
            "db_type": "csv",
            "input_data": {}
        }]
    ],
    module="v6-anova-py"  # Ensure this matches the module name (no dashes)
)

# list mock organizations
organizations = client.organization.list()
print("Organizations:", organizations)
org_ids = [organization["id"] for organization in organizations]
print("Organization IDs:", org_ids)

# Run the central method on 1 node and get the results
central_task = client.task.create(
    input_={
        "method": "central",
        "kwargs": {
            "groups": ['Group'],  # Replace with your actual column for groups
            "features": ['age', 'Height'],  # Replace with your actual numeric columns
        }
    },
    organizations=[org_ids[0]],
)
central_results = client.wait_for_results(central_task.get("id"))
print("\nCentral results:")
print(central_results)

# Run the partial method for all organizations
partial_task = client.task.create(
    input_={
        "method": "partial",
        "kwargs": {
            "groups": ['Group'],  # Replace with your actual column for groups
            "features": ['age', 'Height'],  # Replace with your actual numeric columns
        }
    },
    organizations=org_ids
)
print("\nPartial task created:")
print(partial_task)

# Get the results from the task
partial_results = client.wait_for_results(partial_task.get("id"))
print("\nPartial results:")
print(partial_results)
