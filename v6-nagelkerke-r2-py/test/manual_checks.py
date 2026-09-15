import pandas as pd, numpy as np
import statsmodels.api as sm
from vantage6.algorithm.mock.network import MockNetwork

DATA = "test/test_data.csv"
FEATURES = ["x1", "x2"]
OUTCOME = "outcome"

df = pd.read_csv(DATA)


def run_central(splits, intercept, coefficients, min_samples=5):
    network = MockNetwork(
        module_name="v6-nagelkerke-r2-py",
        datasets=[{"data": {"database": s, "db_type": "csv"}} for s in splits],
    )
    client = network.user_client
    df_id = client.dataframe.list()[0]["id"]
    task = client.task.create(
        method="central",
        organizations=[network.organization_ids[0]],
        arguments={"features": FEATURES, "outcome_column": OUTCOME,
                   "intercept": intercept, "coefficients": coefficients,
                   "min_samples": min_samples},
        databases=[{"type": "dataframe", "dataframe_id": df_id}],
    )
    return client.result.from_task(task["id"])[0]


def make_splits(n_nodes):
    return [df.iloc[i::n_nodes].reset_index(drop=True) for i in range(n_nodes)]


# --- 1. Cross-check against statsmodels' own log-likelihoods ---
print("=== Check 1: statsmodels cross-validation ===")
X = sm.add_constant(df[FEATURES])
fit = sm.Logit(df[OUTCOME], X).fit(disp=0)
sm_intercept = fit.params["const"]
sm_coefs = fit.params[FEATURES].tolist()
sm_llf = fit.llf          # fitted model log-likelihood
sm_llnull = fit.llnull    # null model log-likelihood
r2 = run_central(make_splits(3), sm_intercept, sm_coefs)
print(f"  statsmodels llf={sm_llf:.6f}  vs ours loglik_full={r2['loglik_full']:.6f}")
print(f"  statsmodels llnull={sm_llnull:.6f}  vs ours loglik_null={r2['loglik_null']:.6f}")
assert abs(sm_llf - r2["loglik_full"]) < 1e-6
assert abs(sm_llnull - r2["loglik_null"]) < 1e-6
print("  PASS: matches statsmodels exactly.\n")

# --- 2. Node-count invariance: 1, 2, 5, 10 nodes should all give identical R2 ---
print("=== Check 2: node-count invariance ===")
baseline = None
for n_nodes in (1, 2, 5, 10):
    r = run_central(make_splits(n_nodes), sm_intercept, sm_coefs)
    print(f"  {n_nodes:2d} nodes -> R2_N={r['r2_nagelkerke']:.8f}  n_total={r['n_total']}")
    if baseline is None:
        baseline = r["r2_nagelkerke"]
    assert abs(r["r2_nagelkerke"] - baseline) < 1e-10
print("  PASS: identical across node counts.\n")

# --- 3. Null model (coefficients = 0) should give R2 ~= 0 ---
print("=== Check 3: null model gives R2 ~= 0 ===")
p_bar = df[OUTCOME].mean()
null_intercept = np.log(p_bar / (1 - p_bar))   # logit(p_bar): the MLE null intercept
r = run_central(make_splits(3), null_intercept, [0.0, 0.0])
print(f"  R2_CS={r['r2_cox_snell']:.8f}  R2_N={r['r2_nagelkerke']:.8f}")
assert abs(r["r2_nagelkerke"]) < 1e-8
print("  PASS: R2 ~= 0 for the true null model.\n")

# --- 4. min_samples privacy guard triggers on a too-small node ---
print("=== Check 4: min_samples guard ===")
tiny_splits = [df.iloc[:3].reset_index(drop=True), df.iloc[3:].reset_index(drop=True)]
r = run_central(tiny_splits, sm_intercept, sm_coefs, min_samples=5)
print(f"  result: {r}")
assert r["n_nodes"] == 1  # the 3-row node should have been dropped
assert r["n_total"] == len(df) - 3
print("  PASS: undersized node excluded, n_total reduced accordingly.\n")

# --- 5. Degenerate outcome guard (all-0 or all-1) ---
print("=== Check 5: degenerate outcome guard ===")
degenerate = df.copy()
degenerate[OUTCOME] = 0
r = run_central([degenerate], sm_intercept, sm_coefs)
print(f"  result: {r}")
assert "error" in r
print("  PASS: returns error instead of crashing.\n")

print("ALL MANUAL CHECKS PASSED")
