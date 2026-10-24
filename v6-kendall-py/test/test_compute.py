from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import kendalltau
from vantage6.algorithm.mock.network import MockNetwork

current_path = Path(__file__).parent
data = pd.read_csv(current_path / "test_data.csv")
DATABASE_LABEL = "Database 1"

network = MockNetwork(
    datasets=[
        {DATABASE_LABEL: {"database": data}},
        {DATABASE_LABEL: {"database": data}},
        {DATABASE_LABEL: {"database": data}},
    ],
    module_name="v6-kendall-py",
)
client = network.user_client
DATABASES = [
    {"type": "dataframe", "dataframe_id": network.hq.dataframes[0]["id"]}
]

organizations = client.organization.list()
org_ids = [organization["id"] for organization in organizations]
print("Organization IDs:", org_ids)

COLUMNS = ["Age", "Height"]
N_BINS = 20

task = client.task.create(
    method="federated_function",
    arguments={"mode": "summary", "columns": COLUMNS},
    organizations=org_ids,
    databases=DATABASES,
)
print("\nFederated summary results:")
print(client.wait_for_results(task.get("id")))

central_task = client.task.create(
    method="central_function",
    arguments={
        "columns": COLUMNS,
        "n_bins": N_BINS,
        "organizations_to_include": org_ids,
    },
    organizations=[org_ids[0]],
    databases=DATABASES,
)
central_results = client.wait_for_results(central_task.get("id"))
print("\nCentral results:")
print(central_results)
central_result = central_results[0] if isinstance(central_results, list) else central_results

combined = pd.concat([data] * len(org_ids), ignore_index=True)
exact_pairs = []
for col1, col2 in combinations(COLUMNS, 2):
    pair_df = combined[[col1, col2]].apply(pd.to_numeric, errors="coerce").dropna()
    tau, pval = kendalltau(pair_df[col1], pair_df[col2]) if len(pair_df) >= 2 else (np.nan, np.nan)
    exact_pairs.append({
        "column_x": col1,
        "column_y": col2,
        "tau_exact": float(tau),
        "p_exact": float(pval),
        "n_complete": int(len(pair_df)),
    })

approx_map = {
    (e["column_x"], e["column_y"]): e
    for e in central_result.get("pairs", [])
}
rows = []
for exact in exact_pairs:
    approx = approx_map.get((exact["column_x"], exact["column_y"]), {})
    tau_approx = approx.get("tau_b_approx", np.nan)
    rows.append({
        **exact,
        "tau_approx": tau_approx,
        "abs_diff": abs(exact["tau_exact"] - tau_approx)
        if pd.notna(tau_approx) else np.nan,
    })

print("\nComparison:")
print(pd.DataFrame(rows).to_string(index=False))
