"""
Run this script to test you compute function locally (without building a Docker image)
using the mock client.

Run as:

    python test/test_compute.py

Make sure to do so in an environment where `vantage6-algorithm-tools` (v5) and
`pyaging` are installed. This can be done by running:

    pip install vantage6-algorithm-tools pyaging

Mock data (one file per node, long format with a cohort column):

    test_data_1.csv: cohort_A (5 samples)
    test_data_2.csv: cohort_A (3 samples), cohort_B (7 samples)
    test_data_3.csv: cohort_B (4 samples), cohort_C (11 samples)

cohort_A and cohort_B span two nodes with unequal sample counts, so a plain average
of the node means differs from the correct sample-weighted mean.
"""
import contextlib
import io
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyaging as pya

pd.set_option("display.width", 200)

# get path of current directory
current_path = Path(__file__).parent

# Make the local `epiclock_v5` package importable without installing it, and run
# from the algorithm root so pyaging finds the clock files in `pyaging_data/`
sys.path.insert(0, str(current_path.parent))
os.chdir(current_path.parent)

from vantage6.algorithm.mock.network import MockNetwork  # noqa: E402

from epiclock_v5.central import GLOBAL_RESULT_COLUMNS, aggregate_cohort_results  # noqa: E402
from epiclock_v5.federated import NODE_RESULT_COLUMNS, federated_impl  # noqa: E402

DATABASE_LABEL = "default"
CLOCKS = ["horvath2013", "hannum", "pcphenoage"]
MIN_SAMPLES = 3

nodes = [pd.read_csv(current_path / f"test_data_{i}.csv") for i in (1, 2, 3)]
EXPECTED_COHORTS = [{"cohort_A"}, {"cohort_A", "cohort_B"}, {"cohort_B", "cohort_C"}]


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
    print(f"  PASS: {message}")


def quiet(fn, *args, **kwargs):
    """Call fn with stdout captured; return (result, captured output)."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        result = fn(*args, **kwargs)
    return result, buffer.getvalue()


# ---------------------------------------------------------------------------
# Independent reference: predict every sample's age per node with pyaging,
# then pool the per-sample ages of each cohort across nodes
# ---------------------------------------------------------------------------
def reference_sample_ages(df: pd.DataFrame, clocks: list) -> pd.DataFrame:
    matrix = df.pivot(index="sample_label", columns="probe_id", values="beta")
    adata = pya.preprocess.df_to_adata(matrix, verbose=False)
    for clk in clocks:
        pya.pred.predict_age(adata, clock_names=[clk], verbose=False)
    ages = adata.obs[clocks].astype(float)
    ages["cohort"] = df.groupby("sample_label")["cohort"].first().reindex(ages.index)
    return ages


reference_ages = pd.concat(
    [quiet(reference_sample_ages, df, CLOCKS)[0] for df in nodes], ignore_index=True
)
reference = (
    reference_ages.melt(id_vars="cohort", var_name="clock", value_name="age")
    .groupby(["cohort", "clock"])["age"]
    .agg(mean_ref="mean", sd_ref=lambda a: a.std(ddof=1), n_ref="count")
    .reset_index()
)

# ---------------------------------------------------------------------------
# 1. Node-level function: one row per (cohort, clock)
# ---------------------------------------------------------------------------
print("\n[1] Node-level summaries")
node_results = []
for i, (df, expected_cohorts) in enumerate(zip(nodes, EXPECTED_COHORTS), start=1):
    summary, _ = quiet(federated_impl, df, CLOCKS, min_samples=MIN_SAMPLES)
    node_results.append(summary)
    print(f"\nnode {i}:\n{summary}")

    check(list(summary.columns) == NODE_RESULT_COLUMNS, f"node {i}: output columns are {NODE_RESULT_COLUMNS}")
    check(
        set(summary["cohort"]) == expected_cohorts
        and len(summary) == len(expected_cohorts) * len(CLOCKS)
        and not summary.duplicated(["cohort", "clock"]).any(),
        f"node {i}: exactly one row per (cohort, clock) for {sorted(expected_cohorts)}",
    )
    expected_n = df.groupby("cohort")["sample_label"].nunique()
    check(
        (summary["n_samples"] == summary["cohort"].map(expected_n)).all(),
        f"node {i}: n_samples equals the number of samples per cohort",
    )
    check(
        not {"min", "max", "min_age", "max_age", "individual"} & set(summary.columns),
        f"node {i}: no min/max or individual ages in the output",
    )

# ---------------------------------------------------------------------------
# 2. End-to-end run of central_function on the mock network
# ---------------------------------------------------------------------------
print("\n[2] End-to-end central_function run on the MockNetwork")
network = MockNetwork(
    datasets=[{DATABASE_LABEL: {"database": df}} for df in nodes],
    module_name="epiclock_v5",
)
client = network.user_client
org_ids = [organization["id"] for organization in client.organization.list()]
dataframe_id = client.dataframe.list()[0]["id"]

task = client.task.create(
    method="central_function",
    arguments={"lista_relojes": CLOCKS, "cohort_column": "cohort", "min_samples": MIN_SAMPLES},
    organizations=[org_ids[0]],
    databases=[{"type": "dataframe", "dataframe_id": dataframe_id}],
)
results, _ = quiet(client.wait_for_results, task.get("id"))
json.dumps(results)  # results must be JSON-serialisable for the vantage6 server
global_summary = pd.DataFrame(results[0])
print(f"\nGlobal summary:\n{global_summary}")

check(list(global_summary.columns) == GLOBAL_RESULT_COLUMNS, f"output columns are {GLOBAL_RESULT_COLUMNS}")
check(
    set(global_summary["cohort"]) == {"cohort_A", "cohort_B", "cohort_C"},
    "global result contains cohort_A, cohort_B and cohort_C",
)

merged = global_summary.merge(reference, on=["cohort", "clock"], how="outer", validate="1:1")
check(not merged.isna().any().any(), "every (cohort, clock) appears in both the global result and the reference")
check(
    (merged["n_samples_total"] == merged["n_ref"]).all(),
    "n_samples_total per cohort is {'cohort_A': 8, 'cohort_B': 11, 'cohort_C': 11}",
)
check(np.allclose(merged["mean_age_global"], merged["mean_ref"]), "global mean ages equal the pooled per-sample means")
check(np.allclose(merged["sd_age_global"], merged["sd_ref"]), "global SDs equal the pooled per-sample SDs")
check(
    (merged.set_index("cohort")["n_nodes"].groupby("cohort").first() == pd.Series({"cohort_A": 2, "cohort_B": 2, "cohort_C": 1})).all(),
    "cohort_A and cohort_B are pooled across 2 nodes, cohort_C comes from 1",
)

# The cross-node cohorts must not match a plain average of node means, nor the
# old pooled SD that ignored differences between node means
node_all = pd.concat(node_results, ignore_index=True)
for cohort in ("cohort_A", "cohort_B"):
    parts = node_all[node_all["cohort"] == cohort]
    ref = reference[reference["cohort"] == cohort].set_index("clock")
    plain_mean = parts.groupby("clock")["mean_age"].mean()
    within_only_sd = parts.groupby("clock").apply(
        lambda g: np.sqrt(((g["n_samples"] - 1) * g["sd_age"] ** 2).sum() / (g["n_samples"].sum() - len(g))),
        include_groups=False,
    )
    check(
        not np.allclose(plain_mean, ref.loc[plain_mean.index, "mean_ref"])
        and not np.allclose(within_only_sd, ref.loc[within_only_sd.index, "sd_ref"]),
        f"{cohort}: a plain average of node means or within-node-only SD would differ (test is discriminating)",
    )

# ---------------------------------------------------------------------------
# 3. Privacy threshold: a cohort with a single sample on a node is dropped
# ---------------------------------------------------------------------------
print("\n[3] Cohort below min_samples is dropped")
node3 = nodes[2]
singleton = node3[node3["sample_label"] == node3["sample_label"].iloc[0]].copy()
singleton["cohort"] = "cohort_D"
singleton["sample_label"] = "N3_Sample_99"
with_singleton = pd.concat([node3, singleton], ignore_index=True)

summary, log = quiet(federated_impl, with_singleton, CLOCKS, min_samples=MIN_SAMPLES)
print(log, end="")
check("cohort_D" not in set(summary["cohort"]), "cohort_D (1 sample) is not in the node result")
check(summary.equals(node_results[2]), "the other cohorts on the node are unaffected")
check(
    "Dropping cohort 'cohort_D'" in log and f"min_samples={MIN_SAMPLES}" in log,
    "an info message names the dropped cohort and the reason",
)
check("N3_Sample_99" not in log, "the log contains no sample labels")

# ---------------------------------------------------------------------------
# 4. Edge cases
# ---------------------------------------------------------------------------
print("\n[4] Edge cases")
all_dropped, _ = quiet(federated_impl, singleton, CLOCKS, min_samples=MIN_SAMPLES)
check(
    all_dropped.empty and list(all_dropped.columns) == NODE_RESULT_COLUMNS,
    "all cohorts dropped -> empty node result with the right columns",
)
empty_global = aggregate_cohort_results([all_dropped.to_dict(orient="records"), []])
check(
    empty_global.empty and list(empty_global.columns) == GLOBAL_RESULT_COLUMNS,
    "no node results -> empty global result with the right columns",
)

renamed = nodes[1].rename(columns={"cohort": "study_group"})
custom, _ = quiet(federated_impl, renamed, CLOCKS, cohort_column="study_group", min_samples=MIN_SAMPLES)
check(custom.equals(node_results[1]), "a custom cohort_column name is honoured")

try:
    quiet(federated_impl, renamed, CLOCKS, min_samples=MIN_SAMPLES)
except ValueError as exc:
    check("cohort" in str(exc) and "cohort_column" in str(exc), f"missing cohort column raises: {exc}")
else:
    raise AssertionError("missing cohort column did not raise")

try:
    federated_impl(nodes[0], CLOCKS, min_samples=1)
except ValueError:
    check(True, "min_samples below 2 is rejected (mean and SD of 1 sample is the sample itself)")
else:
    raise AssertionError("min_samples=1 did not raise")

print("\nAll checks passed.")
