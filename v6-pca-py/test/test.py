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

# Algorithm/package name
# The algorithm name you chose may contain '-' which is not a valid Python
# module name. The MockAlgorithmClient expects an importable module.
ALGORITHM_NAME = "v6-pca-py"
MODULE_NAME = ALGORITHM_NAME.replace("-", "-")

# PCA test parameters
# If None, the partial function will auto-select numeric columns.
FEATURES = None
N_COMPONENTS = 2
CENTER = True

# Mock client with two organizations using the same CSV
client = MockAlgorithmClient(
    datasets=[
        # Data for first organization
        [{
            "database": current_path / "test_data.csv",
            "db_type": "csv",
            "input_data": {}
        }],
        # Data for second organization
        [{
            "database": current_path / "test_data.csv",
            "db_type": "csv",
            "input_data": {}
        }]
    ],
    module=MODULE_NAME
)

# list mock organizations
organizations = client.organization.list()
print("Organizations:")
print(organizations)
org_ids = [organization["id"] for organization in organizations]

# -------------------------
# 1) Test PARTIAL directly
# -------------------------
partial_task = client.task.create(
    input_={
        "method": "partial",
        "kwargs": {
            "features": FEATURES
        }
    },
    organizations=org_ids,
    name="Test partial PCA stats",
    description="Local PCA sufficient statistics"
)

partial_results = client.wait_for_results(partial_task.get("id"))
print("\nPartial results:")
print(partial_results)

# -------------------------
# 2) Test CENTRAL end-to-end
# -------------------------
# This will internally create partial tasks and aggregate them.
central_task = client.task.create(
    input_={
        "method": "central",
        "kwargs": {
            "features": FEATURES,
            "n_components": N_COMPONENTS,
            "center": CENTER
        }
    },
    organizations=[org_ids[0]],
    name="Test central PCA",
    description="Federated PCA aggregation and eigen-decomposition"
)

central_results = client.wait_for_results(central_task.get("id"))
print("\nCentral results:")
print(central_results)
