"""
Run this script to test your federated LDA algorithm locally (without building
a Docker image) using the mock client.

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

# Mock client for federated LDA
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
    module="v6-lda-py"
)

# list mock organizations
organizations = client.organization.list()
print("Organizations:", organizations)
org_ids = [organization["id"] for organization in organizations]
print("Organization IDs:", org_ids)

# Run the central method on 1 node (it will dispatch to all orgs internally)
central_task = client.task.create(
    input_={
        "method": "central",
        "kwargs": {
            "class_col": "Group",                       # class label column
            "features": ["age", "Weight", "Height"],    # numeric feature columns
            "n_components": None,                       # None = max (n_classes - 1)
        }
    },
    organizations=[org_ids[0]],
)
central_results = client.wait_for_results(central_task.get("id"))
result = central_results[0]

print("\nCentral LDA results:")
print(f"  Features  : {result['columns']}")
print(f"  Classes   : {result['classes']}")
print(f"  n_total   : {result['n_total']}")
print(f"  n_components returned: {result['n_components']}")
print(f"\n  Explained variance ratio: {result['explained_variance_ratio']}")
print(f"\n  Class means:")
for cls, mean in result["class_means"].items():
    print(f"    {cls}: {[round(v, 3) for v in mean]}")
print(f"\n  Scalings (discriminant axes, shape n_features x n_components):")
for i, row in enumerate(result["scalings"]):
    print(f"    {result['columns'][i]}: {[round(v, 4) for v in row]}")

# Run the partial method directly for inspection
partial_task = client.task.create(
    input_={
        "method": "partial",
        "kwargs": {
            "class_col": "Group",
            "features": ["age", "Weight", "Height"],
        }
    },
    organizations=org_ids,
)
partial_results = client.wait_for_results(partial_task.get("id"))
print("\nPartial results (per node):")
for i, res in enumerate(partial_results):
    print(f"  Node {i} - columns: {res['columns']}")
    for label, stats in res["classes"].items():
        print(f"    class '{label}': n={stats['n']}, mean={[round(v, 2) for v in [s/stats['n'] for s in stats['sum']]]}")
