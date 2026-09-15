"""
Test of the federated Nagelkerke R2 with MockNetwork (vantage6 v5).

Fits a logistic regression centrally (sklearn) on the full toy dataset to get
a known intercept/coefficients, splits the CSV across 3 "nodes" (separate
DataFrames -> automatic data extraction), runs the federated `central`
end-to-end with that fixed model, and checks the federated R2 against a
manually computed centralized Nagelkerke R2 on the full dataset.

Run (with the package installed: pip install -e .):
    python test/test.py
"""

import os
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from vantage6.algorithm.mock.network import MockNetwork

DATA = os.path.join(os.path.dirname(__file__), "test_data.csv")
FEATURES = ["x1", "x2"]
OUTCOME = "outcome"

df = pd.read_csv(DATA)

# --- Fit a model centrally to get a known intercept/coefficients ---
model = LogisticRegression().fit(df[FEATURES], df[OUTCOME])
intercept = float(model.intercept_[0])
coefficients = model.coef_[0].tolist()

# --- Centralized Nagelkerke R2 (manual, on the full dataset) ---
X = df[FEATURES].to_numpy(float)
y = df[OUTCOME].to_numpy(float)
n = len(df)
eta = intercept + X @ np.asarray(coefficients)
p_hat = np.clip(1 / (1 + np.exp(-eta)), 1e-12, 1 - 1e-12)
loglik_full_c = float(np.sum(y * np.log(p_hat) + (1 - y) * np.log(1 - p_hat)))
p_bar = y.mean()
loglik_null_c = n * (p_bar * np.log(p_bar) + (1 - p_bar) * np.log(1 - p_bar))
r2_cs_c = 1 - np.exp((2.0 / n) * (loglik_null_c - loglik_full_c))
r2_n_c = r2_cs_c / (1 - np.exp((2.0 / n) * loglik_null_c))

# --- Data: split across 3 nodes (DataFrames -> automatic extraction) ---
boundaries = np.linspace(0, len(df), 4).astype(int)
splits = [df.iloc[boundaries[i]:boundaries[i + 1]].reset_index(drop=True) for i in range(3)]

network = MockNetwork(
    module_name="v6-nagelkerke-r2-py",
    datasets=[
        {"data": {"database": splits[0], "db_type": "csv"}},
        {"data": {"database": splits[1], "db_type": "csv"}},
        {"data": {"database": splits[2], "db_type": "csv"}},
    ],
)
client = network.user_client
df_id = client.dataframe.list()[0]["id"]

# --- Central end-to-end ---
task = client.task.create(
    method="central",
    organizations=[network.organization_ids[0]],
    arguments={"features": FEATURES, "outcome_column": OUTCOME,
               "intercept": intercept, "coefficients": coefficients},
    databases=[{"type": "dataframe", "dataframe_id": df_id}],
)
result = client.result.from_task(task["id"])[0]

print("\n=== Central result (federated) ===")
for k, v in result.items():
    print(f"  {k:15s}: {v}")

print("\n=== Validation: federated == centralized ===")
print(f"  centralized R2_CS = {r2_cs_c:.6f} | federated = {result['r2_cox_snell']:.6f}")
print(f"  centralized R2_N  = {r2_n_c:.6f} | federated = {result['r2_nagelkerke']:.6f}")

assert result["n_total"] == n
assert abs(result["r2_cox_snell"] - r2_cs_c) < 1e-8
assert abs(result["r2_nagelkerke"] - r2_n_c) < 1e-8
assert 0.0 <= result["r2_nagelkerke"] <= 1.0
print("\n  PASS: federated == centralized (exact).")
