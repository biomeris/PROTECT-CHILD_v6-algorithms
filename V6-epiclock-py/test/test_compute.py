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

Clock coverage: the nodes hold different shares of each clock's CpGs. For the
default clocks:

    hannum       100% on nodes 1, 2, 3   -> coverage_ok for cohort_A, cohort_B, cohort_C
    horvath2013  100% on nodes 2, 3      -> coverage_ok for cohort_B, cohort_C
    pedbe        100% on node 3          -> coverage_ok for cohort_C
    pcphenoage   < 1% everywhere         -> coverage_ok for no cohort

The other clocks (dnamphenoage, zhangen, weidner, lin, bocklandt, corticalclock)
have a few CpGs per node. Cohorts pooled from two nodes therefore have different
coverage_min and coverage_max. Section [5] checks the coverage reporting and the
EPIC v2 identifiers.
"""
import contextlib
import io
import json
import os
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pyaging as pya

pd.set_option("display.width", 200)

# get path of current directory
current_path = Path(__file__).parent

# Make the local `epiclock_v5` package importable without installing it, and run
# from the algorithm root so relative paths resolve
sys.path.insert(0, str(current_path.parent))
os.chdir(current_path.parent)

from vantage6.algorithm.mock.network import MockNetwork  # noqa: E402

# Use the rename mapping packaged with the algorithm (no manifest override)
os.environ.pop("MANIFEST_PATH", None)
os.environ.pop("EPICLOCK_LEGACY_MAP", None)

from epiclock_v5.central import (  # noqa: E402
    GLOBAL_RESULT_COLUMNS, aggregate_cohort_results, check_requested_clocks, summarise_clock_coverage,
)
from epiclock_v5.federated import DEFAULT_CLOCKS, NODE_RESULT_COLUMNS, federated_impl  # noqa: E402
from epiclock_v5.epicv2 import (  # noqa: E402
    adapt_long, adapt_wide, clock_features, load_legacy_map, load_packaged_legacy_map, to_clock_ids,
)

DATABASE_LABEL = "default"
# Clocks chosen by the user in these tests: the defaults plus other pyaging clocks
CLOCKS = ["horvath2013", "hannum", "pcphenoage", "pedbe"]
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
global_summary = pd.DataFrame(results[0]["results"])
coverage_summary = pd.DataFrame(results[0]["clock_coverage"]).set_index("clock")
n_nodes_mapped = results[0]["n_nodes_legacy_loci_mapping"]
print(f"\nClock coverage:\n{coverage_summary}")
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

# ---------------------------------------------------------------------------
# 5. EPIC v2 probe identifiers and clock coverage
# ---------------------------------------------------------------------------
print("\n[5] EPIC v2 identifiers and clock coverage")
check(
    list(to_clock_ids(["cg00000029_TC21", "cg00000029_BC21", "cg00000108_BO11", "cg00000165", "rs1019916_TC11", "ch.1.2F"]))
    == ["cg00000029", "cg00000029", "cg00000108", "cg00000165", "rs1019916", "ch.1.2F"],
    "EPIC v2 suffixes (_TC21, _BC21, _BO11, ...) are stripped; plain identifiers are kept",
)

# Node 2 in EPIC v2 form: suffixed identifiers and a replicate probe for one CpG
node2 = nodes[1]
first_cpg = node2["probe_id"].iloc[0]
epic = node2.assign(probe_id=node2["probe_id"] + "_TC21")
replicate = epic[epic["probe_id"] == f"{first_cpg}_TC21"].assign(
    probe_id=f"{first_cpg}_BC21", beta=lambda d: d["beta"] + 0.02
)
epic = pd.concat([epic, replicate], ignore_index=True)

adapted = adapt_long(epic, value_columns=("beta",))
expected_plain = node2.assign(beta=np.where(node2["probe_id"] == first_cpg, node2["beta"] + 0.01, node2["beta"]))
merged = adapted.merge(expected_plain, on=["probe_id", "sample_label"], suffixes=("", "_expected"))
check(
    len(adapted) == len(node2) and len(merged) == len(node2)
    and np.allclose(merged["beta"], merged["beta_expected"]) and (merged["cohort"] == merged["cohort_expected"]).all(),
    "adapt_long: identifiers converted, the replicate probe averaged, cohort kept",
)
wide = epic.pivot(index="sample_label", columns="probe_id", values="beta")
wide_adapted = adapt_wide(wide)
check(
    wide_adapted.shape[1] == node2["probe_id"].nunique()
    and np.allclose(wide_adapted[first_cpg], expected_plain.pivot(index="sample_label", columns="probe_id", values="beta")[first_cpg]),
    "adapt_wide: same conversion for a wide table (one column per probe)",
)

epic_summary, _ = quiet(federated_impl, epic, CLOCKS, min_samples=MIN_SAMPLES)
plain_summary, _ = quiet(federated_impl, expected_plain, CLOCKS, min_samples=MIN_SAMPLES)
check(
    epic_summary.equals(plain_summary),
    "EPIC v2 data gives the same results as the same data with clock identifiers",
)

# Coverage: what each node reports equals the share of clock CpGs in its data
features = {clk: set(clock_features(clk)) for clk in CLOCKS}
node_coverage = [
    {clk: len(features[clk] & set(df["probe_id"])) / len(features[clk]) for clk in CLOCKS} for df in nodes
]
for clk in CLOCKS:
    per_node = [cov[clk] for cov in node_coverage]
    row = coverage_summary.loc[clk]
    check(
        np.isclose(row["coverage_min"], min(per_node)) and np.isclose(row["coverage_max"], max(per_node))
        and row["n_nodes_reported"] == 3 and row["n_nodes_failed"] == 0
        and row["n_nodes_coverage_ok"] == sum(v >= 0.9 for v in per_node),
        f"{clk}: per-clock summary {min(per_node):.2%}-{max(per_node):.2%}, "
        f"{sum(v >= 0.9 for v in per_node)} node(s) with coverage_ok, none failed",
    )

# With the default threshold (0.9) low-coverage clocks are still returned, flagged
for i, summary in enumerate(node_results, start=1):
    expected = summary["clock"].map(node_coverage[i - 1])
    ok = sorted(c for c, v in node_coverage[i - 1].items() if v >= 0.9)
    check(
        set(summary["clock"]) == set(CLOCKS)
        and np.allclose(summary["coverage"].astype(float), expected)
        and (summary["coverage_ok"].astype(bool) == (expected >= 0.9)).all(),
        f"node {i}: every clock returned with its coverage; coverage_ok=True exactly for {ok}",
    )
expected_ok = [
    all(node_coverage[n][clk] >= 0.9 for n in {"cohort_A": [0, 1], "cohort_B": [1, 2], "cohort_C": [2]}[cohort])
    for cohort, clk in zip(global_summary["cohort"], global_summary["clock"])
]
check(
    len(global_summary) == 3 * len(CLOCKS) and list(global_summary["coverage_ok"]) == expected_ok,
    f"global result keeps all {3 * len(CLOCKS)} (cohort, clock) rows; coverage_ok is true exactly where "
    "every contributing node reaches 0.9",
)
designed = {
    "cohort_A": {"hannum"},
    "cohort_B": {"hannum", "horvath2013"},
    "cohort_C": {"hannum", "horvath2013", "pedbe"},
}
default_chosen = [c for c in DEFAULT_CLOCKS if c in CLOCKS]
ok_pairs = global_summary[global_summary["coverage_ok"] & global_summary["clock"].isin(default_chosen)]
for cohort, clocks in designed.items():
    expected = clocks & set(default_chosen)
    got = set(ok_pairs.loc[ok_pairs["cohort"] == cohort, "clock"])
    check(got == expected, f"{cohort}: coverage_ok for {sorted(expected) or 'no'} default clock(s)")

# Cohorts pooled from two nodes report the lowest and highest coverage of those nodes
cohort_nodes = {"cohort_A": [0, 1], "cohort_B": [1, 2], "cohort_C": [2]}
for cohort, node_ids in cohort_nodes.items():
    for clk in CLOCKS:
        row = global_summary[(global_summary["cohort"] == cohort) & (global_summary["clock"] == clk)].iloc[0]
        values = [node_coverage[n][clk] for n in node_ids]
        check(
            np.isclose(row["coverage_min"], min(values)) and np.isclose(row["coverage_max"], max(values))
            and (min(values) == max(values) or row["coverage_min"] < row["coverage_max"]),
            f"{cohort} / {clk}: coverage_min {min(values):.2%}, coverage_max {max(values):.2%} "
            f"(nodes {[n + 1 for n in node_ids]})",
        )


def run_central(**arguments):
    network = MockNetwork(datasets=[{DATABASE_LABEL: {"database": df}} for df in nodes], module_name="epiclock_v5")
    client = network.user_client
    task = client.task.create(
        method="central_function",
        arguments={"lista_relojes": CLOCKS, "cohort_column": "cohort", "min_samples": MIN_SAMPLES, **arguments},
        organizations=[client.organization.list()[0]["id"]],
        databases=[{"type": "dataframe", "dataframe_id": client.dataframe.list()[0]["id"]}],
    )
    return pd.DataFrame(quiet(client.wait_for_results, task.get("id"))[0][0]["results"])


# The threshold only sets the flag: true only if every contributing node reaches it.
# Use a chosen clock whose coverage is higher on node 3 than on node 2, and a
# threshold in between: cohort_C (node 3 only) is coverage_ok, cohort_A (nodes 1-2)
# and cohort_B (nodes 2-3) are not.
clk = next((c for c in CLOCKS if node_coverage[2][c] > node_coverage[1][c]), None)
threshold = (node_coverage[1][clk] + node_coverage[2][clk]) / 2 if clk else 0.5
mid = run_central(coverage_threshold=threshold)
flags = mid.set_index(["cohort", "clock"])["coverage_ok"]
if clk:
    check(
        not flags[("cohort_A", clk)] and not flags[("cohort_B", clk)] and flags[("cohort_C", clk)],
        f"threshold {threshold:.3f}: {clk} coverage_ok only for cohort_C (node 3 only, "
        f"{node_coverage[2][clk]:.3f}), not for cohorts with a node below it",
    )
else:
    print("  (no chosen clock has higher coverage on node 3 than on node 2; skipping the threshold check)")
all_ok = run_central(coverage_threshold=0.0)

network = MockNetwork(datasets=[{DATABASE_LABEL: {"database": df}} for df in nodes], module_name="epiclock_v5")
client = network.user_client
task = client.task.create(
    method="central_function",
    arguments={"cohort_column": "cohort", "min_samples": MIN_SAMPLES},  # no lista_relojes
    organizations=[client.organization.list()[0]["id"]],
    databases=[{"type": "dataframe", "dataframe_id": client.dataframe.list()[0]["id"]}],
)
default_run = pd.DataFrame(quiet(client.wait_for_results, task.get("id"))[0][0]["results"])
shared = [c for c in DEFAULT_CLOCKS if c in CLOCKS]  # compare the clocks run both ways
check(
    set(default_run["clock"]) == set(DEFAULT_CLOCKS)
    and default_run[default_run["clock"].isin(shared)].sort_values(["cohort", "clock"]).reset_index(drop=True).equals(
        global_summary[global_summary["clock"].isin(shared)]
        .sort_values(["cohort", "clock"]).reset_index(drop=True)),
    f"without lista_relojes only the default clocks {DEFAULT_CLOCKS} are calculated, with the same results",
)
store = json.loads((current_path.parent / "algorithm_store.json").read_text())
store_defaults = [a.get("default_value") for f in store["functions"] for a in f["arguments"] if a["name"] == "lista_relojes"]
check(
    len(store_defaults) == 2 and all(d == DEFAULT_CLOCKS for d in store_defaults),
    "algorithm_store.json offers the same default clocks as DEFAULT_CLOCKS (both functions)",
)
check(all_ok["coverage_ok"].all(), "threshold 0 -> every result coverage_ok, nothing removed")

# The pooled means and SDs do not depend on the threshold and equal the reference
pooled = ["cohort", "clock", "mean_age_global", "sd_age_global", "n_samples_total", "n_nodes",
          "coverage_min", "coverage_max"]
check(
    all_ok[pooled].equals(global_summary[pooled]) and mid[pooled].equals(global_summary[pooled]),
    "global means, SDs, counts and coverage are identical for threshold 0, the default and in between",
)
vs_reference = all_ok.merge(reference, on=["cohort", "clock"], validate="1:1")
check(
    len(vs_reference) == len(reference)
    and np.allclose(vs_reference["mean_age_global"], vs_reference["mean_ref"])
    and np.allclose(vs_reference["sd_age_global"], vs_reference["sd_ref"]),
    "and they still equal the pooled per-sample means and SDs of the raw data",
)

# A clock that fails to compute produces no results and is reported as failed
fake_results = [
    {"results": [], "clock_coverage": {"horvath2013": 0.95, "broken_clock": None}},
    {"results": [], "clock_coverage": {"horvath2013": 0.50, "broken_clock": None}},
]
summary_rows = {r["clock"]: r for r in summarise_clock_coverage(fake_results, ["horvath2013", "broken_clock"], 0.9)}
check(
    summary_rows["broken_clock"]["n_nodes_failed"] == 2 and summary_rows["broken_clock"]["n_nodes_coverage_ok"] == 0
    and summary_rows["broken_clock"]["coverage_min"] is None
    and summary_rows["horvath2013"]["n_nodes_failed"] == 0 and summary_rows["horvath2013"]["n_nodes_coverage_ok"] == 1,
    "failed clocks are counted as failed; nodes meeting the threshold are counted separately",
)

try:
    federated_impl(nodes[0], CLOCKS, min_samples=MIN_SAMPLES, coverage_threshold=1.5)
except ValueError:
    check(True, "coverage_threshold outside [0, 1] is rejected")
else:
    raise AssertionError("coverage_threshold=1.5 did not raise")

# ---------------------------------------------------------------------------
# 6. Renamed EPIC v2 CpGs: mapping to 450K / EPIC v1 identifiers via the manifest
# ---------------------------------------------------------------------------
print("\n[6] Renamed EPIC v2 CpGs (Methyl450_Loci / EPICv1_Loci)")

packaged = load_packaged_legacy_map()
check(
    packaged is not None and len(packaged) == 1085 and packaged.get("cg11707779") == "cg14361627",
    "the rename mapping ships with the package: 1085 CpGs, incl. cg11707779 -> cg14361627",
)
check(n_nodes_mapped == 3, "without any manifest, all 3 nodes map renamed CpGs with the packaged mapping")

manifest_text = """Illumina, Inc.,,,
[Heading],,,
Descriptor File Name,EPIC-8v2-0_A2,,
[Assay],,,
IlmnID,Name,Methyl450_Loci,EPICv1_Loci
cg11707779_TC21,cg11707779,cg14361627,cg14361627
cg11707779_BC21,cg11707779,cg14361627,
cg00000001_TC11,cg00000001,cg00000001,cg00000001
cg00000002_TC11,cg00000002,cg00000003;cg00000004,
cg00000005_TC11,cg00000005,cg00000001,
cg00000006_TC11,cg00000006,cg00000099,
cg00000007_TC11,cg00000007,cg00000099,
"""
with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as handle:
    handle.write(manifest_text)
    toy_manifest = handle.name

legacy = load_legacy_map(toy_manifest)
check(
    legacy == {"cg11707779": "cg14361627"},
    "only unambiguous renames are mapped (not: same name, several loci, a locus that is another "
    "EPIC v2 CpG, a locus claimed by two CpGs)",
)
check(
    list(to_clock_ids(["cg11707779_TC21", "cg11707779_BC21", "cg00000001_TC11"], legacy))
    == ["cg14361627", "cg14361627", "cg00000001"],
    "to_clock_ids strips the suffix, then maps renamed CpGs (replicates included)",
)

# Node 1 plus a renamed EPIC v2 probe whose 450K identifier is a Hannum CpG
hannum = set(clock_features("hannum"))
check("cg14361627" in hannum and "cg11707779" not in hannum, "cg14361627 is a Hannum CpG, cg11707779 is not")
node1_without = nodes[0][nodes[0]["probe_id"] != "cg14361627"]  # node 1 has all Hannum CpGs
renamed = node1_without[node1_without["probe_id"] == node1_without["probe_id"].iloc[0]].assign(probe_id="cg11707779_TC21")
with_renamed = pd.concat([node1_without, renamed], ignore_index=True)

mapping_clocks = ["hannum", "horvath2013"]  # fixed: this checks the mapping, not the user's choice
os.environ["EPICLOCK_LEGACY_MAP"] = "none"
try:
    without_map, log_off = quiet(federated_impl, with_renamed, mapping_clocks, min_samples=MIN_SAMPLES)
finally:
    os.environ.pop("EPICLOCK_LEGACY_MAP")
with_map, log = quiet(federated_impl, with_renamed, mapping_clocks, min_samples=MIN_SAMPLES)
cov_without = without_map.attrs["clock_coverage"]["hannum"]
cov_with = with_map.attrs["clock_coverage"]["hannum"]
check(
    np.isclose(cov_with - cov_without, 1 / len(hannum)) and with_map.attrs["legacy_loci_mapping"]
    and not without_map.attrs["legacy_loci_mapping"],
    f"with the packaged mapping the renamed probe counts for Hannum: coverage {cov_without:.2%} -> {cov_with:.2%}",
)
check(
    with_map.attrs["clock_coverage"]["horvath2013"] == without_map.attrs["clock_coverage"]["horvath2013"],
    "clocks without renamed CpGs are unaffected",
)
check(
    "mapping 1085" in log and "packaged mapping" in log and "disabled" in log_off,
    "the node logs which mapping it uses; EPICLOCK_LEGACY_MAP=none switches it off",
)
os.environ["MANIFEST_PATH"] = toy_manifest
try:
    with_toy, log_toy = quiet(federated_impl, with_renamed, mapping_clocks, min_samples=MIN_SAMPLES)
finally:
    os.environ.pop("MANIFEST_PATH")
check(
    "mapping 1 to" in log_toy and "manifest" in log_toy
    and with_toy.attrs["clock_coverage"]["hannum"] == cov_with,
    "$MANIFEST_PATH overrides the packaged mapping (here a 1-entry manifest)",
)

real_manifest = current_path.parent.parent / "v6-dmp-dmr-py" / "test" / "EPIC-8v2-0_A2.csv"
if real_manifest.exists() and real_manifest.stat().st_size > 1_000_000:
    real = load_legacy_map(str(real_manifest))
    check(
        real == packaged,
        f"the packaged mapping equals the one built from the real EPIC v2 manifest ({len(real)} CpGs)",
    )
else:
    print("  (real EPIC v2 manifest not found; skipping that check)")
os.remove(toy_manifest)

# ---------------------------------------------------------------------------
# 7. Clock bundle (clocks.json in the Docker image)
# ---------------------------------------------------------------------------
print("\n[7] Clock bundle")
fake_bundle = {"clocks": {
    clk: {"extra_features": [], "tissue": ["blood"], "population": "adults"} for clk in CLOCKS
}}
fake_bundle["clocks"]["hannum"]["extra_features"] = ["age", "female"]   # pretend hannum needs age/sex
fake_bundle["clocks"]["pedbe"]["population"] = "children"

check_requested_clocks(CLOCKS, fake_bundle)
check_requested_clocks(["anything"], None)
check(True, "bundled clocks are accepted; without a bundle (local development) any clock is accepted")
try:
    check_requested_clocks(CLOCKS + ["horvath2O13"], fake_bundle)
except ValueError as exc:
    check("horvath2O13" in str(exc) and f"{len(CLOCKS)} clocks are available" in str(exc),
          "a clock that is not bundled is rejected before any task is sent, listing the available clocks")
else:
    raise AssertionError("unbundled clock was not rejected")

with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
    json.dump(fake_bundle, handle)
    bundle_path = handle.name
os.environ["EPICLOCK_CLOCK_BUNDLE"] = bundle_path
try:
    network = MockNetwork(datasets=[{DATABASE_LABEL: {"database": df}} for df in nodes], module_name="epiclock_v5")
    client = network.user_client
    task = client.task.create(
        method="central_function",
        arguments={"lista_relojes": CLOCKS, "cohort_column": "cohort", "min_samples": MIN_SAMPLES},
        organizations=[client.organization.list()[0]["id"]],
        databases=[{"type": "dataframe", "dataframe_id": client.dataframe.list()[0]["id"]}],
    )
    bundled_output, log = quiet(client.wait_for_results, task.get("id"))
finally:
    os.environ.pop("EPICLOCK_CLOCK_BUNDLE")
    os.remove(bundle_path)
bundled_cov = pd.DataFrame(bundled_output[0]["clock_coverage"]).set_index("clock")
check(
    bundled_cov.loc["hannum", "extra_features"] == ["age", "female"]
    and bundled_cov.loc["pedbe", "population"] == "children"
    and bundled_cov.loc["horvath2013", "tissue"] == ["blood"],
    "with a bundle, the per-clock summary reports extra inputs, tissue and population",
)
check(
    pd.DataFrame(bundled_output[0]["results"]).equals(pd.DataFrame(results[0]["results"])),
    "the bundle does not change any result",
)

print("\nAll checks passed.")
