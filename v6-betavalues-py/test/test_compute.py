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
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

# get path of current directory
current_path = Path(__file__).parent

# Make the local `globalIDAT` package importable without installing it
sys.path.insert(0, str(current_path.parent))

from vantage6.algorithm.mock.network import MockNetwork  # noqa: E402

from globalIDAT.central import (  # noqa: E402
    CENTRAL_RESULT_COLUMNS,
    GLOBAL_RESULT_COLUMNS,
    aggregate_cohort_results,
    cohort_totals,
)
from globalIDAT.federated import (  # noqa: E402
    MAX_NODE_PROBES,
    MAX_RESULT_PROBES,
    NODE_RESULT_COLUMNS,
    _validate_probes,
    _validate_top_n,
    cohort_sizes,
    federated_impl,
    matching_probe_ids,
    top_probes_impl,
)

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

check(list(global_summary.columns) == CENTRAL_RESULT_COLUMNS, f"output columns are {CENTRAL_RESULT_COLUMNS}")
check(global_summary["probe_id"].nunique() == raw_all["probe_id"].nunique(), "default top_n (1000) covers all 5 mock probes")
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

reference_delta = (
    reference.groupby("probe_id")["beta_ref"].agg(lambda b: b.max() - b.min()).rename("delta_ref")
)
deltas = global_summary.drop_duplicates("probe_id").set_index("probe_id")["delta_beta"]
check(
    np.allclose(deltas, reference_delta.loc[deltas.index]),
    "delta_beta is the largest minus the smallest cohort mean Beta of the probe",
)
check(deltas.is_monotonic_decreasing, "rows are sorted by delta_beta, largest first")
check(
    (global_summary["n_samples_cohort"] == global_summary["cohort"].map(expected_totals)).all()
    and (global_summary["coverage"] == 1.0).all(),
    "every sample has every mock probe: n_samples_cohort is the cohort size and coverage is 1",
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

# ---------------------------------------------------------------------------
# 5. Targeted query: only the requested probes are returned
# ---------------------------------------------------------------------------
print("\n[5] Targeted query with 'probes'")
task = client.task.create(
    method="central_function",
    organizations=[org_ids[0]],
    arguments={"cohort_column": "cohort", "min_samples": MIN_SAMPLES,
               "probes": ["cg00000769", "cg99999999"]},
    databases=[{"dataframe_id": dataframe["id"]}],
)
targeted = pd.DataFrame(client.wait_for_results(task.get("id"))[0])
print(targeted)
check(set(targeted["probe_id"]) == {"cg00000769"}, "only the requested probe is returned; unknown probes are skipped")
check(set(targeted["cohort"]) == {"cohort_A", "cohort_B", "cohort_C"}, "the probe is reported for every cohort")
check(
    targeted.set_index("cohort")["beta_mean_global"].round(10).equals(
        global_summary.query("probe_id == 'cg00000769'").set_index("cohort")["beta_mean_global"]
        .loc[targeted["cohort"]].round(10)
    ),
    "targeted means equal those of the overview",
)

ids = pd.Series(["cg00000029_TC21", "cg00000029_BC11", "cg00000108_TC11", "cg00000769", "ch.1.123_BC11"])
check(
    set(matching_probe_ids(ids, ["cg00000029"])) == {"cg00000029_TC21", "cg00000029_BC11"},
    "a CpG name selects all its EPIC v2 replicate probes",
)
check(
    set(matching_probe_ids(ids, ["cg00000108_TC11", "cg00000769"])) == {"cg00000108_TC11", "cg00000769"},
    "full EPIC v2 IDs and 450K IDs match exactly",
)

# ---------------------------------------------------------------------------
# 6. top_n on a larger network: the probes that differ most between cohorts
# ---------------------------------------------------------------------------
print("\n[6] top_n selects the probes with the largest difference between cohorts")
rng = np.random.default_rng(6)
N_PROBES, SHIFTED = 300, 12
probe_ids = [f"cg{i:08d}" for i in range(N_PROBES)]
shift = pd.Series(0.0, index=probe_ids)
shift.iloc[:SHIFTED] = np.linspace(0.30, 0.52, SHIFTED)  # cohort_Y is higher on these probes
layout = {  # node -> {cohort: samples}; both cohorts on both nodes, unequal sizes
    "node_1": {"cohort_X": 4, "cohort_Y": 2},
    "node_2": {"cohort_X": 2, "cohort_Y": 5},
}
synthetic = {}
for node, cohorts in layout.items():
    rows = []
    for cohort, n in cohorts.items():
        for s in range(n):
            beta = np.clip(0.3 + rng.normal(0, 0.03, N_PROBES) + (shift.values if cohort == "cohort_Y" else 0), 0.01, 0.99)
            rows.append(pd.DataFrame({
                "probe_id": probe_ids, "sample_label": f"{node}_{cohort}_{s}", "cohort": cohort,
                "beta": beta, "m_value": np.log2(beta / (1 - beta)),
            }))
    synthetic[node] = pd.concat(rows, ignore_index=True)

# QC removed the strongest probe in 2 of node_2's cohort_Y samples (beta/M missing)
GAPPY = probe_ids[SHIFTED - 1]
gaps = (synthetic["node_2"]["probe_id"] == GAPPY) & synthetic["node_2"]["sample_label"].isin(
    ["node_2_cohort_Y_0", "node_2_cohort_Y_1"]
)
synthetic["node_2"].loc[gaps, ["beta", "m_value"]] = np.nan

with tempfile.TemporaryDirectory() as tmp:
    paths = []
    for node, df in synthetic.items():
        path = Path(tmp) / f"{node}.csv"
        df.to_csv(path, index=False)
        paths.append(path)
    big_network = MockNetwork(
        datasets=[{DATABASE_LABEL: {"database": str(path), "db_type": "csv"}} for path in paths],
        module_name="globalIDAT",
    )
    big_client = big_network.user_client
    big_df = big_client.dataframe.create(
        label=DATABASE_LABEL, method="data_extraction_function", arguments={"arg1": None}, name=DATABASE_LABEL,
    )
    big_orgs = [organization["id"] for organization in big_client.organization.list()]
    task = big_client.task.create(
        method="central_function",
        organizations=[big_orgs[0]],
        arguments={"min_samples": MIN_SAMPLES, "top_n": 5},
        databases=[{"dataframe_id": big_df["id"]}],
    )
    top = pd.DataFrame(big_client.wait_for_results(task.get("id"))[0])
print(top)

pooled = pd.concat(synthetic.values(), ignore_index=True)
pooled_means = pooled.groupby(["probe_id", "cohort"])["beta"].mean().unstack()
exact_delta = (pooled_means.max(axis=1) - pooled_means.min(axis=1)).sort_values(ascending=False)
check(top["probe_id"].nunique() == 5 and len(top) == 10, "top_n=5 returns 5 probes x 2 cohorts")
check(
    list(top["probe_id"].drop_duplicates()) == list(exact_delta.index[:5]),
    "they are the 5 probes with the largest pooled delta_beta, in that order",
)
top_delta = top.drop_duplicates("probe_id").set_index("probe_id")["delta_beta"]
check(np.allclose(top_delta, exact_delta.loc[top_delta.index]), "delta_beta equals the pooled difference of cohort means")
gappy = top[top["probe_id"] == GAPPY].set_index("cohort")
check(not gappy.empty, f"{GAPPY} (missing in 2 samples) is kept in the result, not removed")
check(
    gappy.loc["cohort_Y", "n_samples_total"] == 5 and gappy.loc["cohort_Y", "n_samples_cohort"] == 7
    and np.isclose(gappy.loc["cohort_Y", "coverage"], 5 / 7) and gappy.loc["cohort_X", "coverage"] == 1.0,
    f"{GAPPY}: coverage is 5/7 in cohort_Y (2 of 7 samples missing) and 1 in cohort_X",
)
check((top.loc[top["probe_id"] != GAPPY, "coverage"] == 1.0).all(), "the other top probes have coverage 1")
check(
    cohort_sizes(synthetic["node_2"]) == {"cohort_X": 2, "cohort_Y": 5}
    and cohort_totals([{"cohort_sizes": {"a": 2}}, {"cohort_sizes": {"a": 3, "b": 4}}, []]).to_dict() == {"a": 5, "b": 4},
    "nodes report their cohort sizes and the central function sums them",
)
candidates = top_probes_impl(synthetic["node_1"], min_samples=MIN_SAMPLES, top_n=5)
check(
    list(candidates.columns) == ["probe_id", "delta_beta"] and len(candidates) == 5,
    "a node's candidate list holds only probe IDs and the node's delta_beta",
)

# ---------------------------------------------------------------------------
# 7. Size limits: requests above the cap are rejected with a clear message
# ---------------------------------------------------------------------------
print("\n[7] Size limits")


def expect_value_error(fn, text: str, message: str) -> None:
    try:
        fn()
    except ValueError as exc:
        check(text in str(exc), f"{message}: {exc}")
    else:
        raise AssertionError(f"{message}: no error raised")


too_many = [f"cg{i:08d}" for i in range(MAX_RESULT_PROBES + 1)]
check(len(_validate_probes(too_many[:-1], MAX_RESULT_PROBES)) == MAX_RESULT_PROBES, f"{MAX_RESULT_PROBES:,} probes are accepted")
expect_value_error(lambda: _validate_probes(too_many, MAX_RESULT_PROBES), "maximum is 10,000", "10,001 probes are rejected")
expect_value_error(lambda: _validate_top_n(MAX_RESULT_PROBES + 1), "between 1 and 10,000", "top_n=10,001 is rejected")
expect_value_error(lambda: _validate_top_n(0), "between 1 and 10,000", "top_n=0 is rejected")
expect_value_error(lambda: _validate_probes("cg00000769", MAX_RESULT_PROBES), "must be a list", "a single string is rejected")
expect_value_error(lambda: _validate_probes(None, MAX_NODE_PROBES), "list of probes is required", "a node never returns all probes")
expect_value_error(
    lambda: _validate_probes([f"cg{i:08d}" for i in range(MAX_NODE_PROBES + 1)], MAX_NODE_PROBES),
    f"maximum is {MAX_NODE_PROBES:,}",
    "a node rejects more than its own limit",
)
check(
    len(_validate_probes(["cg1", " cg1 ", "cg2", ""], MAX_RESULT_PROBES)) == 2,
    "duplicate and empty probe IDs are ignored",
)

print("\nAll checks passed.")
