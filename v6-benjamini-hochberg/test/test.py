"""
Run this script to test the federated Benjamini-Hochberg algorithm locally
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

# Mock client for the federated Benjamini-Hochberg algorithm
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
    module="v6-benjamini-hochberg"
)

# list mock organizations
organizations = client.organization.list()
print("Organizations:", organizations)
org_ids = [organization["id"] for organization in organizations]
print("Organization IDs:", org_ids)

# ── 1. Correction of p-values produced by a previous federated task ──────────
# This entry point contacts no node and reads no database.
central_task = client.task.create(
    input_={
        "method": "central",
        "kwargs": {
            "p_values": {
                "age": 0.0001,
                "Weight": 0.0031,
                "Height": 0.0402,
                "bmi": 0.2100,
                "sbp": 0.6700,
            },
            "alpha": 0.05,
        }
    },
    organizations=[org_ids[0]],
)
correction = client.wait_for_results(central_task.get("id"))[0]
print(
    f"\nBH on supplied p-values (alpha={correction['alpha']}): "
    f"{correction['n_rejected']}/{correction['n_tested']} rejected"
)
for name, r in sorted(correction["results"].items(), key=lambda kv: kv[1]["rank"] or 0):
    print(
        f"  rank {r['rank']}  {name:8s} p={r['p_value']:.4f}  "
        f"q={r['adjusted_p_value']:.4f}  crit={r['critical_value']:.4f}  "
        f"{'REJECT' if r['reject'] else '-'}"
    )

# ── 2. Federated per-column test, then correction ────────────────────────────
# Nodes return moments in round 1 and binned record counts in round 2. No raw
# value ever reaches the central server.
federated_task = client.task.create(
    input_={
        "method": "central_federated",
        "kwargs": {
            "organizations_to_include": org_ids,
            "group_col": "Group",                                  # three groups
            "columns": ["age", "Weight", "Height", "bmi", "sbp"],  # one hypothesis each
            "test": "kruskal-wallis",
            "alpha": 0.05,
        }
    },
    organizations=[org_ids[0]],
)
results = client.wait_for_results(federated_task.get("id"))[0]
print(
    f"\nFederated {results['test']} + BH (alpha={results['alpha']}): "
    f"{results['n_rejected']}/{results['n_tested']} rejected"
)
for col, stats in sorted(results["columns"].items(), key=lambda kv: kv[1]["rank"]):
    print(
        f"  rank {stats['rank']}  {col:8s} "
        f"{stats['statistic_name']}={stats['statistic']:.4f}  "
        f"p={stats['p_value']:.3e}  q={stats['adjusted_p_value']:.3e}  "
        f"n={stats['n_total']}  {'REJECT' if stats['reject'] else '-'}"
    )
if results["excluded"]:
    print("  excluded:", results["excluded"])
print("  privacy:", results["privacy"])

# ── 3. Same, but with a two-group Mann-Whitney test ──────────────────────────
mwu_task = client.task.create(
    input_={
        "method": "central_federated",
        "kwargs": {
            "organizations_to_include": org_ids,
            "group_col": "Cohort",                                 # two groups
            "columns": ["age", "Weight", "Height", "bmi", "sbp"],
            "test": "mann-whitney",
            "alpha": 0.05,
        }
    },
    organizations=[org_ids[0]],
)
mwu_results = client.wait_for_results(mwu_task.get("id"))[0]
print(
    f"\nFederated {mwu_results['test']} + BH (alpha={mwu_results['alpha']}): "
    f"{mwu_results['n_rejected']}/{mwu_results['n_tested']} rejected"
)
for col, stats in sorted(mwu_results["columns"].items(), key=lambda kv: kv[1]["rank"]):
    print(
        f"  rank {stats['rank']}  {col:8s} "
        f"{stats['statistic_name']}={stats['statistic']:.4f}  "
        f"p={stats['p_value']:.3e}  q={stats['adjusted_p_value']:.3e}  "
        f"{'REJECT' if stats['reject'] else '-'}"
    )

# ── 4. Inspect exactly what the nodes return ─────────────────────────────────
# Everything printed below is an aggregate: counts, sums and sums of squares.
moments_task = client.task.create(
    input_={
        "method": "partial_moments",
        "kwargs": {
            "group_col": "Group",
            "columns": ["age", "Weight"],
        }
    },
    organizations=org_ids,
)
print("\nRound-1 payload leaving each node (moments only):")
for i, res in enumerate(client.wait_for_results(moments_task.get("id"))):
    print(f"  Node {i}: {res}")
