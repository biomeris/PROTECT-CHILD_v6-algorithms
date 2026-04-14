"""
Run this script to test your federated Tukey HSD algorithm locally
(without building a Docker image) using the mock client.

Run as:

    python test.py

Make sure to do so in an environment where `vantage6-algorithm-tools` is
installed. This can be done by running:

    pip install vantage6-algorithm-tools
"""

from pathlib import Path
import sys

from vantage6.algorithm.tools.mock_client import MockAlgorithmClient

# Path of current directory
current_path = Path(__file__).parent

# Ensure project root is on sys.path so the algorithm package can be imported
project_root = current_path.parent
sys.path.insert(0, str(project_root))

# IMPORTANT:
# This must match the importable Python package name of your algorithm folder.
# Example:
#   v6-tukey-py  -> v6_tukey_py
MODULE_NAME = "v6-tukey-py"

# Test parameters
GROUP_COL = "Group"
FEATURES = ["age", "Height"]
ALPHA = 0.05

# Mock client for federated Tukey HSD
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

# List mock organizations
organizations = client.organization.list()
print("Organizations:", organizations)
org_ids = [organization["id"] for organization in organizations]
print("Organization IDs:", org_ids)

# -------------------------
# 1) Test CENTRAL end-to-end
# -------------------------
central_task = client.task.create(
    input_={
        "method": "central",
        "kwargs": {
            "organizations_to_include": org_ids,
            "group_col": GROUP_COL,
            "features": FEATURES,
            "alpha": ALPHA
        }
    },
    organizations=[org_ids[0]],
    name="Test central federated Tukey HSD",
    description="Federated Tukey HSD aggregation and pairwise comparisons"
)

central_results = client.wait_for_results(central_task.get("id"))
print("\nCentral results:")
print(central_results)

# -------------------------
# 2) Test PARTIAL directly
# -------------------------
partial_task = client.task.create(
    input_={
        "method": "partial",
        "kwargs": {
            "group_col": GROUP_COL,
            "features": FEATURES
        }
    },
    organizations=org_ids,
    name="Test partial federated Tukey HSD",
    description="Local sufficient statistics for federated Tukey HSD"
)

print("\nPartial task created:")
print(partial_task)

partial_results = client.wait_for_results(partial_task.get("id"))
print("\nPartial results:")
print(partial_results)