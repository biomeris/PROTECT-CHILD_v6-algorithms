"""
Run this script to test the federated C-statistic algorithm locally (without
building a Docker image) using the mock client.

Run as:

    python test.py

Make sure to do so in an environment where `vantage6-algorithm-tools` is
installed. This can be done by running:

    pip install vantage6-algorithm-tools

To check the statistics without installing vantage6 at all, run `test_math.py`
instead — it exercises the same arithmetic against scikit-learn.
"""
from vantage6.algorithm.tools.mock_client import MockAlgorithmClient
from pathlib import Path

# get path of current directory
current_path = Path(__file__).parent

# Mock client for the federated C-statistic algorithm
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
    module="v6-C-statistics"
)

# list mock organizations
organizations = client.organization.list()
print("Organizations:", organizations)
org_ids = [organization["id"] for organization in organizations]
print("Organization IDs:", org_ids)

# ── 1. C-statistic for several score columns against a 0/1 outcome ───────────
central_task = client.task.create(
    input_={
        "method": "central",
        "kwargs": {
            "organizations_to_include": org_ids,
            "outcome_col": "recurrence",                                      # 0/1, positive_label inferred as 1
            "columns": ["risk_score_strong", "risk_score_moderate", "unrelated_score"],
            "alpha": 0.05,
        }
    },
    organizations=[org_ids[0]],
)
results = client.wait_for_results(central_task.get("id"))[0]
print("\nFederated C-statistic (outcome='recurrence', positive_label inferred=1):")
for col, r in results.items():
    print(
        f"  {col:20s} C={r['c_statistic']:.4f}  "
        f"95% CI [{r['ci_lower']:.4f}, {r['ci_upper']:.4f}]  "
        f"SE={r['standard_error']:.4f}  p={r['p_value']:.3e}  "
        f"n_event={r['n_event']} n_non_event={r['n_non_event']}"
    )

# ── 2. Same, but with a boolean outcome column and explicit positive_label ──
bool_task = client.task.create(
    input_={
        "method": "central",
        "kwargs": {
            "organizations_to_include": org_ids,
            "outcome_col": "recurrence_bool",
            "columns": ["risk_score_strong"],
            "positive_label": True,
            "alpha": 0.05,
        }
    },
    organizations=[org_ids[0]],
)
bool_results = client.wait_for_results(bool_task.get("id"))[0]
print("\nFederated C-statistic (boolean outcome, explicit positive_label=True):")
for col, r in bool_results.items():
    print(f"  {col:20s} C={r['c_statistic']:.4f}  p={r['p_value']:.3e}")

# ── 3. Correct the p-values from step 1 for multiple comparisons ────────────
# v6-benjamini-hochberg's `central` needs no node contact — pure post-hoc.
bh_pkg_root = current_path.parent.parent / "v6-benjamini-hochberg"
try:
    import sys

    sys.path.insert(0, str(bh_pkg_root))
    from importlib import import_module

    bh_central = import_module("v6-benjamini-hochberg.central")

    correction = bh_central.central(
        p_values={col: r["p_value"] for col, r in results.items()}, alpha=0.05
    )
    print(
        f"\nBenjamini-Hochberg correction across the {correction['n_tested']} score "
        f"columns: {correction['n_rejected']} remain significant"
    )
    for col, r in sorted(correction["results"].items(), key=lambda kv: kv[1]["rank"] or 0):
        print(
            f"  rank {r['rank']}  {col:20s} p={r['p_value']:.3e}  "
            f"q={r['adjusted_p_value']:.3e}  {'REJECT' if r['reject'] else '-'}"
        )
except (ImportError, ModuleNotFoundError):
    print("\n(v6-benjamini-hochberg not found alongside this repo; skipping step 3)")

# ── 4. Inspect exactly what the nodes return ─────────────────────────────────
# Everything printed below is an aggregate: counts, sums and sums of squares.
moments_task = client.task.create(
    input_={
        "method": "partial_moments",
        "kwargs": {
            "outcome_col": "recurrence",
            "columns": ["risk_score_strong"],
        }
    },
    organizations=org_ids,
)
print("\nRound-1 payload leaving each node (moments only):")
for i, res in enumerate(client.wait_for_results(moments_task.get("id"))):
    print(f"  Node {i}: {res}")
