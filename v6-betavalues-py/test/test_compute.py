"""
Run this script to test you compute function locally (without building a Docker image)
using the mock client.

Run as:

    python test/test_compute.py

Make sure to do so in an environment where `vantage6-algorithm-tools` (v5) is
installed. This can be done by running:

    pip install vantage6-algorithm-tools

Mock data (one folder per node, each node is a hospital holding several cohorts):

    hospital_A: cohort_A (3 samples), cohort_B (4 samples)
    hospital_B: cohort_B (2 samples), cohort_C (3 samples)

cohort_B spans both nodes with unequal sample counts, so a plain average of the node
means differs from the correct sample-weighted mean.
"""
import contextlib
import io
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# get path of current directory
current_path = Path(__file__).parent

# Make the local `globalIDAT` package importable without installing it
sys.path.insert(0, str(current_path.parent))

from vantage6.algorithm.mock.network import MockNetwork  # noqa: E402

from globalIDAT.central import GLOBAL_RESULT_COLUMNS, aggregate_cohort_results  # noqa: E402
from globalIDAT.federated import NODE_RESULT_COLUMNS, federated_impl  # noqa: E402

DATABASE_LABEL = "default"
NODE_FOLDERS = ["hospital_A", "hospital_B"]
MIN_SAMPLES = 2


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
    print(f"  PASS: {message}")


raw = {folder: pd.read_csv(current_path / folder / "preprocessed.csv") for folder in NODE_FOLDERS}
raw_all = pd.concat(raw.values(), ignore_index=True)

# ---------------------------------------------------------------------------
# 1. Node-level function: one row per (cohort, probe_id)
# ---------------------------------------------------------------------------
print("\n[1] Node-level summaries")
node_results = {}
for folder, df in raw.items():
    summary = federated_impl(df, min_samples=MIN_SAMPLES)
    node_results[folder] = summary
    print(f"\n{folder}:\n{summary}")

    expected_keys = set(map(tuple, df[["cohort", "probe_id"]].drop_duplicates().to_numpy()))
    check(list(summary.columns) == NODE_RESULT_COLUMNS, f"{folder}: output columns are {NODE_RESULT_COLUMNS}")
    check(
        len(summary) == len(expected_keys)
        and set(map(tuple, summary[["cohort", "probe_id"]].to_numpy())) == expected_keys,
        f"{folder}: exactly one row per (cohort, probe_id)",
    )
    expected_n = df.groupby("cohort")["sample_label"].nunique()
    check(
        (summary["n_samples"] == summary["cohort"].map(expected_n)).all(),
        f"{folder}: n_samples equals the number of samples per cohort",
    )

check(
    set(node_results["hospital_A"]["cohort"]) == {"cohort_A", "cohort_B"},
    "hospital_A returns cohort_A and cohort_B",
)
check(
    set(node_results["hospital_B"]["cohort"]) == {"cohort_B", "cohort_C"},
    "hospital_B returns cohort_B and cohort_C",
)

# ---------------------------------------------------------------------------
# 2. End-to-end run of central_function on the mock network
# ---------------------------------------------------------------------------
print("\n[2] End-to-end central_function run on the MockNetwork")
network = MockNetwork(
    datasets=[
        {DATABASE_LABEL: {"database": str(current_path / folder / "preprocessed.csv"), "db_type": "csv"}}
        for folder in NODE_FOLDERS
    ],
    module_name="globalIDAT",
)
client = network.user_client
org_ids = [organization["id"] for organization in client.organization.list()]

dataframe = client.dataframe.create(
    label=DATABASE_LABEL,
    method="data_extraction_function",
    arguments={"arg1": None},
    name=DATABASE_LABEL,
)
task = client.task.create(
    method="central_function",
    organizations=[org_ids[0]],
    arguments={"cohort_column": "cohort", "min_samples": MIN_SAMPLES},
    databases=[{"dataframe_id": dataframe["id"]}],
)
results = client.wait_for_results(task.get("id"))
global_summary = pd.DataFrame(results[0])
print(f"\nGlobal summary:\n{global_summary}")

check(list(global_summary.columns) == GLOBAL_RESULT_COLUMNS, f"output columns are {GLOBAL_RESULT_COLUMNS}")
check(
    set(global_summary["cohort"]) == {"cohort_A", "cohort_B", "cohort_C"},
    "global result contains cohort_A, cohort_B and cohort_C",
)
expected_totals = raw_all.groupby("cohort")["sample_label"].nunique()
check(
    (global_summary["n_samples_total"] == global_summary["cohort"].map(expected_totals)).all(),
    f"n_samples_total per cohort is {expected_totals.to_dict()}",
)

# Independent reference: pooled per-sample mean over the raw rows of all nodes
reference = (
    raw_all.groupby(["cohort", "probe_id"])
    .agg(beta_ref=("beta", "mean"), m_ref=("m_value", "mean"))
    .reset_index()
)
merged = global_summary.merge(reference, on=["cohort", "probe_id"], how="outer", validate="1:1")
check(not merged.isna().any().any(), "every (cohort, probe_id) in the raw data appears in the global result")
check(
    np.allclose(merged["beta_mean_global"], merged["beta_ref"])
    and np.allclose(merged["m_mean_global"], merged["m_ref"]),
    "global means of all cohorts equal the pooled means of the raw data",
)

# cohort_B spans both nodes (4 + 2 samples): the weighted mean must match the raw
# data and a plain average of node means must not
cohort_b = merged[merged["cohort"] == "cohort_B"].set_index("probe_id")
node_b = pd.concat(node_results.values()).query("cohort == 'cohort_B'")
check(
    node_b["n_samples"].nunique() > 1,
    f"cohort_B has unequal sample counts per node: {node_b['n_samples'].tolist()}",
)
plain_avg = node_b.groupby("probe_id")[["beta_mean", "m_mean"]].mean()
check(
    np.allclose(cohort_b["beta_mean_global"], cohort_b["beta_ref"])
    and np.allclose(cohort_b["m_mean_global"], cohort_b["m_ref"]),
    "cohort_B global mean equals the sample-weighted mean of the raw data",
)
check(
    not np.allclose(plain_avg["beta_mean"], cohort_b["beta_ref"].loc[plain_avg.index])
    and not np.allclose(plain_avg["m_mean"], cohort_b["m_ref"].loc[plain_avg.index]),
    "a plain average of cohort_B node means would differ (test is discriminating)",
)

# ---------------------------------------------------------------------------
# 3. Privacy threshold: a cohort with a single sample on a node is dropped
# ---------------------------------------------------------------------------
print("\n[3] Cohort below min_samples is dropped")
singleton = raw["hospital_B"].query("cohort == 'cohort_C'").copy()
singleton = singleton[singleton["sample_label"] == singleton["sample_label"].iloc[0]]
singleton["cohort"] = "cohort_D"
singleton["sample_label"] = "6264509104_R01C01"
with_singleton = pd.concat([raw["hospital_B"], singleton], ignore_index=True)

log = io.StringIO()
with contextlib.redirect_stdout(log):
    summary = federated_impl(with_singleton, min_samples=MIN_SAMPLES)
log = log.getvalue()
print(log, end="")

check("cohort_D" not in set(summary["cohort"]), "cohort_D (1 sample) is not in the node result")
check(
    summary.reset_index(drop=True).equals(node_results["hospital_B"].reset_index(drop=True)),
    "the other cohorts on the node are unaffected",
)
check(
    "Dropping cohort 'cohort_D'" in log and f"min_samples={MIN_SAMPLES}" in log,
    "an info message names the dropped cohort and the reason",
)
check(
    "6264509104_R01C01" not in log
    and not any(f"{v:.6f}" in log for v in pd.concat([singleton["beta"], singleton["m_value"]])),
    "the log contains no sample labels or values",
)

# ---------------------------------------------------------------------------
# 4. Edge cases
# ---------------------------------------------------------------------------
print("\n[4] Edge cases")
with contextlib.redirect_stdout(io.StringIO()):
    all_dropped = federated_impl(singleton, min_samples=MIN_SAMPLES)
check(
    all_dropped.empty and list(all_dropped.columns) == NODE_RESULT_COLUMNS,
    "all cohorts dropped -> empty node result with the right columns",
)
empty_global = aggregate_cohort_results([all_dropped.to_dict(orient="records"), []])
check(
    empty_global.empty and list(empty_global.columns) == GLOBAL_RESULT_COLUMNS,
    "no node results -> empty global result with the right columns",
)

renamed = raw["hospital_A"].rename(columns={"cohort": "study_group"})
with contextlib.redirect_stdout(io.StringIO()):
    custom = federated_impl(renamed, cohort_column="study_group", min_samples=MIN_SAMPLES)
check(custom.equals(node_results["hospital_A"]), "a custom cohort_column name is honoured")

try:
    with contextlib.redirect_stdout(io.StringIO()):
        federated_impl(renamed, min_samples=MIN_SAMPLES)
except ValueError as exc:
    check("cohort" in str(exc) and "cohort_column" in str(exc), f"missing cohort column raises: {exc}")
else:
    raise AssertionError("missing cohort column did not raise")

print("\nAll checks passed.")
