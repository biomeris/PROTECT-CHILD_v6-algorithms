"""
Run this script to test your federated Kruskal-Wallis test algorithm locally
(without building a Docker image) using the mock client.

Run as:

    python test.py

Make sure to do so in an environment where `vantage6-algorithm-tools` is
installed. This can be done by running:

    pip install vantage6-algorithm-tools

To check the statistics without installing vantage6 at all, run `test_math.py`
instead — it exercises the same arithmetic against scipy.
"""
from vantage6.algorithm.tools.mock_client import MockAlgorithmClient
from pathlib import Path

# get path of current directory
current_path = Path(__file__).parent

# Mock client for federated Kruskal-Wallis test
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
    module="v6-kruskal-wallis-py"
)

# list mock organizations
organizations = client.organization.list()
print("Organizations:", organizations)
org_ids = [organization["id"] for organization in organizations]
print("Organization IDs:", org_ids)

# Run the central method across both organizations
central_task = client.task.create(
    input_={
        "method": "central",
        "kwargs": {
            "organizations_to_include": org_ids,
            "group_col": "Group",                        # column defining the groups
            "columns": ["age", "Weight", "Height"],      # numeric columns to test
        }
    },
    organizations=[org_ids[0]],
)
central_results = client.wait_for_results(central_task.get("id"))
print("\nCentral results:")
for col, stats in central_results[0].items():
    groups_summary = ", ".join(
        f"{g} n={stats['n_per_group'][g]}" for g in stats["groups"]
    )
    print(
        f"  {col}: H={stats['h_statistic']:.4f}, "
        f"p={stats['p_value']:.4f}, "
        f"df={stats['degrees_of_freedom']} "
        f"({groups_summary})"
    )

# Inspect exactly what the nodes return in round 1 — everything printed below
# is an aggregate: counts, sums and sums of squares. No individual value is
# ever part of this payload.
moments_task = client.task.create(
    input_={
        "method": "partial_moments",
        "kwargs": {
            "group_col": "Group",
            "columns": ["age", "Weight", "Height"],
        }
    },
    organizations=org_ids,
)
partial_results = client.wait_for_results(moments_task.get("id"))
print("\nRound-1 payload leaving each node (moments only):")
for i, res in enumerate(partial_results):
    print(f"  Node {i}: {res}")
