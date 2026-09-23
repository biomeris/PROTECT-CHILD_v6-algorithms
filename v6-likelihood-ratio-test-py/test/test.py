"""
Run this script to test the federated likelihood ratio test algorithm locally
(without building a Docker image) using the mock client.

Run as:

    python test.py

Make sure to do so in an environment where `vantage6-algorithm-tools` is
installed. This can be done by running:

    pip install vantage6-algorithm-tools

To check the statistics without installing vantage6 at all, run `test_math.py`
instead — it exercises the same arithmetic against scikit-learn and by
simulation.
"""
from vantage6.algorithm.tools.mock_client import MockAlgorithmClient
from pathlib import Path

# get path of current directory
current_path = Path(__file__).parent

# Mock client for the federated likelihood ratio test algorithm
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
    module="v6-likelihood-ratio-test"
)

# list mock organizations
organizations = client.organization.list()
print("Organizations:", organizations)
org_ids = [organization["id"] for organization in organizations]
print("Organization IDs:", org_ids)

# ── Likelihood ratio test for every covariate against a 0/1 outcome ─────────
# Fits the full model {age, risk_score, noise_var} and, for each covariate, the
# model with just that covariate dropped.
central_task = client.task.create(
    input_={
        "method": "central",
        "kwargs": {
            "organizations_to_include": org_ids,
            "outcome_col": "recurrence",                          # 0/1, positive_label inferred as 1
            "covariates": ["age", "risk_score", "noise_var"],
        }
    },
    organizations=[org_ids[0]],
)
results = client.wait_for_results(central_task.get("id"))[0]

print("\nFederated likelihood ratio test (outcome='recurrence'):")
any_result = next(iter(results.values()))
print(
    f"  full model: logL={any_result['log_likelihood_full']:.4f}  "
    f"n_total={any_result['n_total']}  n_event={any_result['n_event']}  "
    f"n_non_event={any_result['n_non_event']}  "
    f"iterations={any_result['n_iterations_full']}  "
    f"converged={any_result['converged_full']}"
)
for covariate, r in results.items():
    print(
        f"  drop '{covariate}': LR={r['lr_statistic']:.4f}  df={r['degrees_of_freedom']}  "
        f"p={r['p_value']:.3e}  logL_reduced={r['log_likelihood_reduced']:.4f}  "
        f"iterations={r['n_iterations_reduced']}  converged={r['converged_reduced']}"
    )

# ── Inspect exactly what the nodes return in round 0 ─────────────────────────
# Everything printed below is an aggregate: counts, sums and sums of squares.
moments_task = client.task.create(
    input_={
        "method": "partial_moments",
        "kwargs": {
            "outcome_col": "recurrence",
            "covariates": ["age", "risk_score", "noise_var"],
        }
    },
    organizations=org_ids,
)
print("\nRound-0 payload leaving each node (moments only):")
for i, res in enumerate(client.wait_for_results(moments_task.get("id"))):
    print(f"  Node {i}: {res}")
