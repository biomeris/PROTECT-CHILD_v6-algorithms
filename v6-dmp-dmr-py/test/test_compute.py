"""
Run this script to test the federated DMP/DMR analysis locally (without building a
Docker image).

Run as:

    python test/test_compute.py

Make sure to do so in an environment where `vantage6-algorithm-tools` (v5),
`pylluminator`, `scipy` and `statsmodels` are installed.

Mock data: `Hospital A` and `Hospital B` hold long-format beta/M values (the output
of v6-preprocessIDAT-py) for 9 patients each, with both cohorts in each hospital:
cohort_A (4 patients) and cohort_B (5 patients). Every expected value below is
computed independently from the raw data with scipy / statsmodels.

These 108 probes are too far apart to form DMRs, so section [7] generates data for
clusters of neighbouring EPIC v2 probes (taken from the manifest) to check the
DMP -> DMR hand-off end to end.

The tests use min_samples=3; the default is 2. Section [9] onwards checks the
EPIC v2 probe identifiers of real data, the capped result and the compact node
payload, and that the code only uses what a real vantage6 node accepts.
"""
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats
from statsmodels.stats.multitest import multipletests

pd.set_option("display.width", 200)

current_path = Path(__file__).parent
# Import the local package without installing it, and run from the algorithm root
# so the default manifest path (test/EPIC-8v2-0_A2.csv) resolves
sys.path.insert(0, str(current_path.parent))
os.chdir(current_path.parent)

from vantage6.algorithm.mock.network import MockNetwork  # noqa: E402

from differentialy_methylated_positions_regions.annotation import load_annotation  # noqa: E402
from differentialy_methylated_positions_regions.central import (  # noqa: E402
    MAX_RESULT_ROWS, _meta_analyse_dmps, aggregate_results, find_dmrs,
)
from differentialy_methylated_positions_regions.moderation import fit_prior  # noqa: E402
from differentialy_methylated_positions_regions.federated import (  # noqa: E402
    NODE_RESULT_COLUMNS, compute_methylation_impl, decode_table,
)

DATABASE_LABEL = "default"
A, B = "cohort_A", "cohort_B"
MIN_SAMPLES = 3
HOSPITALS = {
    "Hospital A": current_path / "Hospital A" / "hospital_a_epic_v2.csv",
    "Hospital B": current_path / "Hospital B" / "hospital_b_epic_v2.csv",
}
raw = {name: pd.read_csv(path) for name, path in HOSPITALS.items()}


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
    print(f"  PASS: {message}")


def print_results(output: dict, title: str, top: int = 10) -> None:
    """Print the top DMPs and DMRs of a central result as readable tables."""
    comparison = output["comparison"]
    a, b = comparison["cohort_a"], comparison["cohort_b"]
    print(f"\n=== {title}: {b} vs {a} (estimate = {comparison['estimate']}, "
          f"{comparison['estimate_scale']} scale) ===")
    print(f"Hospitals used: {output['n_nodes_used']}, skipped: {len(output['skipped_nodes'])}")
    for warning in output["warnings"]:
        print(f"WARNING: {warning}")

    dmps = pd.DataFrame(output["global_dmps"])
    print(f"\nTop {min(top, len(dmps))} DMPs (of {len(dmps)} probes)")
    if dmps.empty:
        print("  (none)")
    else:
        table = dmps.head(top).assign(
            location=lambda d: d["chromosome"] + ":" + d["position"].astype("Int64").astype(str),
            hospitals=lambda d: d["n_nodes"],
            patients=lambda d: d["n_samples_a"].astype(str) + " / " + d["n_samples_b"].astype(str),
        )[["probe_id", "location", "genes", "mean_beta_a", "mean_beta_b", "delta_beta",
           "estimate", "adj_p_value", "hospitals", "patients"]].rename(columns={
            "mean_beta_a": f"beta_{a}", "mean_beta_b": f"beta_{b}",
            "estimate": f"estimate_{comparison['estimate_scale']}", "patients": f"n_{a}/n_{b}",
        })
        print(table.to_string(index=False, float_format=lambda v: f"{v:.3g}"))

    dmrs = pd.DataFrame(output["global_dmrs"])
    print(f"\nTop {min(top, len(dmrs))} DMRs (of {len(dmrs)} regions)")
    if dmrs.empty:
        print("  (none)")
    else:
        table = dmrs.head(top).assign(
            location=lambda d: d["chromosome"] + ":" + d["start"].astype(str) + "-" + d["end"].astype(str),
        )[["location", "n_probes", "genes", "direction", "mean_delta_beta", "mean_estimate",
           "max_probe_adj_p_value"]].rename(columns={"mean_estimate": f"mean_estimate_{comparison['estimate_scale']}"})
        print(table.to_string(index=False, float_format=lambda v: f"{v:.3g}"))
    print()


def reference_node(df: pd.DataFrame) -> pd.DataFrame:
    """Independent per-probe two-sample comparison (B - A) on M-values."""
    m = df.pivot(index="probe_id", columns="sample_label", values="m_value")
    beta = df.pivot(index="probe_id", columns="sample_label", values="beta")
    cohort = df.groupby("sample_label")["cohort"].first()
    a, b = cohort[cohort == A].index, cohort[cohort == B].index
    t = stats.ttest_ind(m[b], m[a], axis=1)
    est = m[b].mean(axis=1) - m[a].mean(axis=1)
    return pd.DataFrame({
        "estimate": est,
        "std_err": est / t.statistic,
        "p_value": t.pvalue,
        "n_a": len(a),
        "n_b": len(b),
        "sum_beta_a": beta[a].sum(axis=1),
        "sum_beta_b": beta[b].sum(axis=1),
    })


# ---------------------------------------------------------------------------
# 1. Per-hospital comparison
# ---------------------------------------------------------------------------
print("\n[1] Per-hospital DMPs")
node_results, plain_results = {}, {}
for name, df in raw.items():
    result = compute_methylation_impl(df, A, B, min_samples=MIN_SAMPLES, moderate_variance=False)
    plain_results[name] = result
    node_results[name] = compute_methylation_impl(df, A, B, min_samples=MIN_SAMPLES)
    dmps = decode_table(result["dmps"]).set_index("probe_id")
    ref = reference_node(df).loc[dmps.index]
    print(f"\n{name}: {result['n_a']} x {A}, {result['n_b']} x {B}\n{dmps.head(3)}")

    check(result["status"] == "ok" and (result["n_a"], result["n_b"]) == (4, 5), f"{name}: 4 {A} and 5 {B} samples analysed")
    check(len(dmps) == df["probe_id"].nunique(), f"{name}: one row per probe")
    check(
        np.allclose(dmps["estimate"].astype(float), ref["estimate"])
        and np.allclose(dmps["std_err"].astype(float), ref["std_err"])
        and np.allclose(dmps["p_value"].astype(float), ref["p_value"]),
        f"{name}: without moderation, estimate, std_err and p-value equal a two-sample t-test ({B} - {A})",
    )
    check(
        result["dmps"].get("format") == "parquet+base64"
        and list(decode_table(result["dmps"]).columns) == NODE_RESULT_COLUMNS,
        f"{name}: the node sends a compact table of statistics only (no annotation)",
    )

# ---------------------------------------------------------------------------
# 2. End-to-end run on the mock network vs an independent meta-analysis
# ---------------------------------------------------------------------------
print("\n[2] End-to-end central_methylation_analysis on the MockNetwork")
network = MockNetwork(
    datasets=[{DATABASE_LABEL: {"database": df}} for df in raw.values()],
    module_name="differentialy_methylated_positions_regions",
)
client = network.user_client
org_ids = [org["id"] for org in client.organization.list()]
task = client.task.create(
    method="central_methylation_analysis",
    arguments={"cohort_a": A, "cohort_b": B, "min_samples": MIN_SAMPLES,
               "moderate_variance": False, "meta_model": "fixed"},
    organizations=[org_ids[0]],
    databases=[{"type": "dataframe", "dataframe_id": client.dataframe.list()[0]["id"]}],
)
output = client.wait_for_results(task.get("id"))[0]
json.dumps(output)  # must be JSON-serialisable for the vantage6 server
global_dmps = pd.DataFrame(output["global_dmps"]).set_index("probe_id")
print_results(output, "Hospital A + Hospital B (plain statistics, fixed effect)")

refs = [reference_node(df) for df in raw.values()]
w = sum(1 / r["std_err"] ** 2 for r in refs)
ref_est = sum(r["estimate"] / r["std_err"] ** 2 for r in refs) / w
ref_se = np.sqrt(1 / w)
ref_p = 2 * stats.norm.sf(np.abs(ref_est / ref_se))
ref_q = sum((r["estimate"] - ref_est) ** 2 / r["std_err"] ** 2 for r in refs)
ref = pd.DataFrame({
    "estimate": ref_est,
    "std_err": ref_se,
    "p_value": ref_p,
    "adj_p_value": multipletests(ref_p, method="fdr_bh")[1],
    "heterogeneity_p_value": stats.chi2.sf(ref_q, len(refs) - 1),
}).loc[global_dmps.index]

check(output["n_nodes_used"] == 2 and output["skipped_nodes"] == [], "both hospitals contribute, none skipped")
check(
    output["comparison"]["estimate"] == f"{B} - {A}" and output["comparison"]["estimate_scale"] == "M-value"
    and (global_dmps["estimate_scale"] == "M-value").all(),
    "the output states the direction and the scale (M-value) of the estimate, also on every DMP row",
)
check(
    all(np.allclose(global_dmps[c].astype(float), ref[c]) for c in ["estimate", "std_err", "p_value"]),
    "global estimate, std_err and p-value equal the inverse-variance meta-analysis",
)
check(np.allclose(global_dmps["adj_p_value"].astype(float), ref["adj_p_value"]), "adj_p_value is Benjamini-Hochberg over all probes")
check(
    np.allclose(global_dmps["heterogeneity_p_value"].astype(float), ref["heterogeneity_p_value"]),
    "heterogeneity_p_value is Cochran's Q test",
)
check(
    (global_dmps["n_samples_a"] == 8).all() and (global_dmps["n_samples_b"] == 10).all() and (global_dmps["n_nodes"] == 2).all(),
    f"each probe pools 8 {A} and 10 {B} samples from 2 hospitals",
)
check(list(global_dmps["p_value"]) == sorted(global_dmps["p_value"]), "DMPs are sorted by p-value")
check(global_dmps[["chromosome", "position", "genes"]].notna().all().all(), "every DMP is annotated centrally from the packaged manifest")
check(
    output["summary"]["n_probes_tested"] == 108 and output["summary"]["n_dmps_returned"] == 108
    and output["summary"]["n_probes_annotated"] == 108,
    "summary: 108 probes tested and annotated; all returned (fewer than top_n)",
)

heatmap = pd.DataFrame(output["heatmap_data"]).set_index("probe_id")
pooled = pd.concat(raw.values()).groupby(["probe_id", "cohort"])["beta"].mean().unstack()
check(
    np.allclose(global_dmps["delta_beta"].astype(float), (pooled[B] - pooled[A]).loc[global_dmps.index]),
    "delta_beta is the difference of the pooled mean beta values (B - A)",
)
check(output["warnings"] == [], "no warnings when two hospitals contribute")
check(
    list(heatmap.index) == list(global_dmps.index[:50])
    and np.allclose(heatmap[f"mean_beta_{A}"], pooled.loc[heatmap.index, A])
    and np.allclose(heatmap[f"mean_beta_{B}"], pooled.loc[heatmap.index, B]),
    "heatmap: top 50 DMPs with per-cohort mean beta pooled over all patients (not per hospital)",
)

# ---------------------------------------------------------------------------
# 3. Hospitals without enough samples of both cohorts are skipped
# ---------------------------------------------------------------------------
print("\n[3] Hospitals without both cohorts are skipped")
only_b = raw["Hospital B"].assign(cohort=B, sample_label=lambda d: "C" + d["sample_label"])
skipped = compute_methylation_impl(only_b, A, B, min_samples=MIN_SAMPLES)
check(skipped["status"] == "skipped" and "min_samples=3" in skipped["reason"], f"a hospital with only {B} is skipped: {skipped['reason']}")

few_a = raw["Hospital A"][~raw["Hospital A"]["sample_label"].isin(["A001", "A002"])]
check(
    compute_methylation_impl(few_a, A, B, min_samples=MIN_SAMPLES)["status"] == "skipped",
    f"a hospital with 2 {A} samples (< min_samples) is skipped",
)

with_skipped = aggregate_results(list(node_results.values()) + [skipped], A, B)
direct = aggregate_results(list(node_results.values()), A, B)
check(
    with_skipped["n_nodes_used"] == 2 and len(with_skipped["skipped_nodes"]) == 1
    and with_skipped["global_dmps"] == direct["global_dmps"],
    "a skipped hospital is reported and does not change the results",
)

# ---------------------------------------------------------------------------
# 4. Other cohorts at a hospital are ignored
# ---------------------------------------------------------------------------
print("\n[4] Other cohorts at a hospital are ignored")
extra = raw["Hospital B"].assign(cohort="cohort_C", sample_label=lambda d: "C" + d["sample_label"])
with_c = compute_methylation_impl(pd.concat([raw["Hospital A"], extra]), A, B, min_samples=MIN_SAMPLES)
check(
    decode_table(with_c["dmps"]).equals(decode_table(node_results["Hospital A"]["dmps"]))
    and (with_c["n_a"], with_c["n_b"]) == (node_results["Hospital A"]["n_a"], node_results["Hospital A"]["n_b"]),
    "adding cohort_C patients to Hospital A does not change its result",
)

# ---------------------------------------------------------------------------
# 5. DMR construction on a hand-made DMP table
# ---------------------------------------------------------------------------
print("\n[5] DMR construction")
toy = pd.DataFrame([
    # probe, chr, pos, estimate, z, adj_p
    ("p1", "chr1", 100, 1.0, 4.0, 0.001),
    ("p2", "chr1", 600, 1.2, 5.0, 0.001),
    ("p3", "chr1", 1500, 0.9, 3.0, 0.010),   # 900 bp from p2 -> same region
    ("p4", "chr1", 2600, 1.1, 4.0, 0.001),   # 1100 bp from p3 -> new run, alone
    ("p5", "chr2", 100, 1.0, 4.0, 0.001),
    ("p6", "chr2", 300, -1.0, -4.0, 0.001),  # opposite direction -> breaks
    ("p7", "chr3", 100, -1.0, -4.0, 0.001),
    ("p8", "chr3", 200, -0.1, -0.5, 0.600),  # not significant -> breaks
    ("p9", "chr3", 300, -1.0, -4.0, 0.001),
    ("q1", "chr4", 100, -2.0, -6.0, 0.001),
    ("q2", "chr4", 400, -2.0, -6.0, 0.001),
    ("u1", "chr0", 0, 1.0, 4.0, 0.001),      # unmapped probes (chromosome 0, position 0)
    ("u2", "chr0", 0, 1.0, 4.0, 0.001),      # must never form a region
], columns=["probe_id", "chromosome", "position", "estimate", "z", "adj_p_value"])
toy["genes"] = "G"
regions = find_dmrs(toy, max_gap=1000, fdr=0.05, min_probes=2)
print(regions)
check(
    list(regions["probe_ids"]) == ["q1;q2", "p1;p2;p3"],
    "regions: p1-p3 (gaps <= 1000 bp) and q1-q2; gaps, direction changes, non-significant and unmapped probes break runs",
)
check(
    np.isclose(regions.set_index("probe_ids").loc["p1;p2;p3", "stouffer_p_value"], 2 * stats.norm.sf(12 / np.sqrt(3)))
    and list(regions.set_index("probe_ids")["direction"]) == ["hypo", "hyper"],
    "stouffer_p_value combines probe z-scores; direction follows the sign",
)
check(
    list(regions["max_probe_adj_p_value"]) == sorted(regions["max_probe_adj_p_value"])
    and "adj_p_value" not in regions.columns,
    "regions are ranked by their least significant probe; the descriptive Stouffer p-value is not adjusted",
)

# ---------------------------------------------------------------------------
# 6. Input errors
# ---------------------------------------------------------------------------
print("\n[6] Input errors")
for kwargs, text in [
    ({"df_metilacion": raw["Hospital A"].drop(columns="cohort")}, "Cohort column 'cohort' not found"),
    ({"min_samples": 1}, "min_samples must be an integer of at least 2"),
    ({"cohort_b": A}, "must be different cohorts"),
]:
    args = {"df_metilacion": raw["Hospital A"], "cohort_a": A, "cohort_b": B, "min_samples": MIN_SAMPLES, **kwargs}
    try:
        compute_methylation_impl(**args)
    except ValueError as exc:
        check(text in str(exc), f"raises: {exc}")
    else:
        raise AssertionError(f"no error for {kwargs.keys()}")

# ---------------------------------------------------------------------------
# 7. DMRs end to end: clusters of neighbouring probes from the manifest
# ---------------------------------------------------------------------------
print("\n[7] DMRs end to end on clustered probes")
manifest = load_annotation().rename(columns={"name": "Name", "chromosome": "CHR", "position": "MAPINFO"})
manifest = manifest[["Name", "CHR", "MAPINFO"]].dropna()
manifest = manifest[manifest["Name"].str.startswith("cg") & ~manifest["Name"].duplicated(keep=False)]
manifest = manifest[manifest["CHR"].str.fullmatch(r"(chr)?[1-9]\d*")]
manifest = manifest.assign(MAPINFO=manifest["MAPINFO"].astype(int))
manifest = manifest[manifest["MAPINFO"] > 0].sort_values(["CHR", "MAPINFO"])
manifest["new_run"] = (manifest["CHR"] != manifest["CHR"].shift()) | (manifest["MAPINFO"].diff() > 500)
manifest["run"] = manifest["new_run"].cumsum()
runs = [g["Name"].tolist()[:4] for _, g in manifest.groupby("run", sort=False) if len(g) >= 4][:2]
effect_cluster, null_cluster = runs
background = manifest[~manifest["run"].isin(manifest.loc[manifest["Name"].isin(effect_cluster + null_cluster), "run"])]
background = background.sample(60, random_state=0)["Name"].tolist()
probes = effect_cluster + null_cluster + background
print(f"effect cluster: {effect_cluster}\nnull cluster:   {null_cluster}")

rng = np.random.default_rng(7)
baseline = pd.Series(rng.uniform(0.3, 0.7, len(probes)), index=probes)
cluster_data = []
for h, batch in enumerate([0.0, 0.04]):  # the second hospital has a batch offset
    rows = []
    for cohort in (A, B):
        for i in range(6):
            beta = baseline + batch + rng.normal(0, 0.03, len(probes))
            if cohort == B:
                beta[effect_cluster] += 0.2
            beta = beta.clip(0.01, 0.99)
            rows.append(pd.DataFrame({
                "probe_id": probes, "sample_label": f"H{h}_{cohort}_{i}", "cohort": cohort,
                "beta": beta.values, "m_value": np.log2(beta / (1 - beta)).values,
            }))
    cluster_data.append(pd.concat(rows, ignore_index=True))

network = MockNetwork(
    datasets=[{DATABASE_LABEL: {"database": df}} for df in cluster_data],
    module_name="differentialy_methylated_positions_regions",
)
client = network.user_client
task = client.task.create(
    method="central_methylation_analysis",
    arguments={"cohort_a": A, "cohort_b": B, "min_samples": MIN_SAMPLES},
    organizations=[client.organization.list()[0]["id"]],
    databases=[{"type": "dataframe", "dataframe_id": client.dataframe.list()[0]["id"]}],
)
cluster_output = client.wait_for_results(task.get("id"))[0]
json.dumps(cluster_output)
cluster_dmrs = pd.DataFrame(cluster_output["global_dmrs"])
print_results(cluster_output, "Clustered probes (generated data)", top=5)

check(len(cluster_dmrs) >= 1, "the federated run produces at least one DMR")
effect_region = cluster_dmrs[cluster_dmrs["probe_ids"] == ";".join(effect_cluster)]
check(len(effect_region) == 1, "one DMR is exactly the 4-probe cluster with a cohort effect")
check(
    effect_region["direction"].iloc[0] == "hyper"
    and abs(effect_region["mean_delta_beta"].iloc[0] - 0.2) < 0.03
    and effect_region["estimate_scale"].iloc[0] == "M-value",
    f"the DMR is hypermethylated in {B} with mean_delta_beta ~ 0.2 (estimate on the M-value scale)",
)
check(
    not any(set(r.split(";")) & set(null_cluster) for r in cluster_dmrs["probe_ids"]),
    "the neighbouring cluster without an effect is not called a DMR",
)

# ---------------------------------------------------------------------------
# 8. Only one hospital contributes
# ---------------------------------------------------------------------------
print("\n[8] Only one hospital contributes")
single = aggregate_results([node_results["Hospital A"], skipped], A, B)
single_dmps = pd.DataFrame(single["global_dmps"]).set_index("probe_id")
node_a = decode_table(node_results["Hospital A"]["dmps"]).set_index("probe_id").loc[single_dmps.index]
print(single["warnings"])
check(single["n_nodes_used"] == 1 and len(single["warnings"]) == 1 and "Only 1 hospital" in single["warnings"][0],
      "a warning says only one hospital contributed")
check(
    np.allclose(single_dmps["estimate"].astype(float), node_a["estimate"].astype(float))
    and np.allclose(single_dmps["std_err"].astype(float), node_a["std_err"].astype(float)),
    "estimates and standard errors equal that hospital's own results",
)
check(single_dmps["heterogeneity_p_value"].isna().all(), "heterogeneity_p_value is empty (cannot be assessed)")

none_left = aggregate_results([skipped], A, B)
check(none_left["global_dmps"] == [] and "No hospital" in none_left["warnings"][0], "no contributing hospital -> empty results and a warning")

# ---------------------------------------------------------------------------
# 9. Real EPIC v2 identifiers (IlmnID with suffix) are annotated
# ---------------------------------------------------------------------------
print("\n[9] EPIC v2 identifiers from the IDAT extraction")
table = load_annotation()
to_ilmn = table.drop_duplicates("name").set_index("name")["ilmn_id"]
epic_ids = {name: df.assign(probe_id=df["probe_id"].map(to_ilmn)) for name, df in raw.items()}
check(
    all(df["probe_id"].str.contains(r"_[TB][CO]\d+$").all() for df in epic_ids.values()),
    "the mock probes renamed to their EPIC v2 IlmnID (e.g. cg..._BC21)",
)
epic_output = aggregate_results(
    [compute_methylation_impl(df, A, B, min_samples=MIN_SAMPLES) for df in epic_ids.values()], A, B
)
epic_dmps = pd.DataFrame(epic_output["global_dmps"])
check(
    epic_dmps[["chromosome", "position", "genes"]].notna().all().all()
    and epic_output["summary"]["n_probes_annotated"] == 108,
    "EPIC v2 IlmnIDs are annotated (matched on IlmnID, not on Name)",
)
default_dmps = pd.DataFrame(aggregate_results(list(node_results.values()), A, B)["global_dmps"]).set_index("probe_id")
plain = default_dmps.reset_index().assign(probe_id=lambda d: d["probe_id"].map(to_ilmn)).set_index("probe_id")
check(
    np.allclose(epic_dmps.set_index("probe_id")["estimate"].astype(float), plain.loc[epic_dmps["probe_id"], "estimate"].astype(float))
    and (epic_dmps.set_index("probe_id")["chromosome"] == plain.loc[epic_dmps["probe_id"], "chromosome"]).all(),
    "results are identical whether probes are named by IlmnID or by Name",
)

# ---------------------------------------------------------------------------
# 10. Capped result: correction and DMRs over all probes, top_n returned
# ---------------------------------------------------------------------------
print("\n[10] Capped result")
capped = aggregate_results(list(node_results.values()), A, B, top_n=10)
capped_dmps = pd.DataFrame(capped["global_dmps"]).set_index("probe_id")
check(len(capped_dmps) == 10 and capped["summary"]["n_dmps_returned"] == 10, "top_n=10 returns 10 DMPs")
check(
    list(capped_dmps.index) == list(default_dmps.index[:10])
    and np.allclose(capped_dmps["adj_p_value"].astype(float), default_dmps["adj_p_value"].iloc[:10].astype(float)),
    "they are the 10 smallest p-values, with adj_p_value corrected over all 108 probes (not over 10)",
)
check(capped["summary"]["n_probes_tested"] == 108, "summary counts every probe tested, not only those returned")
for bad in (0, MAX_RESULT_ROWS + 1):
    try:
        aggregate_results(list(node_results.values()), A, B, top_n=bad)
    except ValueError as exc:
        check("top_n must be an integer between 1 and 10,000" in str(exc), f"top_n={bad} is rejected: {exc}")
    else:
        raise AssertionError(f"top_n={bad} was accepted")
check(
    pd.DataFrame(aggregate_results(cluster_results := [
        compute_methylation_impl(df, A, B, min_samples=MIN_SAMPLES) for df in cluster_data
    ], A, B, top_n=1)["global_dmrs"])["probe_ids"].tolist() == cluster_dmrs["probe_ids"].tolist(),
    "DMRs are found from all significant probes, even when only 1 DMP is returned",
)

# ---------------------------------------------------------------------------
# 11. Only what a real vantage6 node accepts
# ---------------------------------------------------------------------------
print("\n[11] Real vantage6 client and Docker image")
import ast  # noqa: E402
import importlib  # noqa: E402
import inspect  # noqa: E402
import re  # noqa: E402

from vantage6.algorithm.client import AlgorithmClient  # noqa: E402

real_params = set(inspect.signature(AlgorithmClient.Task.create).parameters) - {"self"}
tree = ast.parse((current_path.parent / "differentialy_methylated_positions_regions" / "central.py").read_text())
calls = [
    node for node in ast.walk(tree)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    and node.func.attr == "create" and ast.unparse(node.func.value) == "client.task"
]
used = {kw.arg for call in calls for kw in call.keywords}
check(calls and used <= real_params, f"client.task.create only uses arguments of the real vantage6 client {sorted(used)}")
pkg_name = re.search(r'ARG PKG_NAME="([^"]+)"', (current_path.parent / "Dockerfile").read_text()).group(1)
module = importlib.import_module(pkg_name)
check(
    all(hasattr(module, f) for f in ("central_methylation_analysis", "rpc_compute_methylation")),
    f"the Dockerfile's PKG_NAME ('{pkg_name}') is the importable module with both functions",
)

# ---------------------------------------------------------------------------
# 12. Moderated variances (empirical Bayes) and random-effects meta-analysis
# ---------------------------------------------------------------------------
print("\n[12] Moderated variances and random effects")
from scipy.stats import t as student_t  # noqa: E402

for name in raw:
    plain_node = decode_table(plain_results[name]["dmps"]).set_index("probe_id")
    moderated = decode_table(node_results[name]["dmps"]).set_index("probe_id").loc[plain_node.index]
    scale = 1 / plain_node["n_a"] + 1 / plain_node["n_b"]
    s2 = plain_node["std_err"] ** 2 / scale
    d = plain_node["n_a"] + plain_node["n_b"] - 2
    d0, s0_2 = fit_prior(s2.to_numpy(), d.to_numpy())
    if np.isfinite(d0):
        expected_se, expected_df = np.sqrt((d0 * s0_2 + d * s2) / (d0 + d) * scale), d + d0
        reported_d0 = np.isclose(node_results[name]["prior_df"], d0)
    else:  # variances vary no more than chance: every probe gets the prior variance
        expected_se, expected_df = np.sqrt(s0_2 * scale), d + 1e6
        reported_d0 = node_results[name]["prior_df"] is None
    check(
        node_results[name]["moderated"] and reported_d0
        and np.allclose(moderated["std_err"], expected_se) and np.allclose(moderated["df"], expected_df),
        f"{name}: std_err = sqrt((d0*s0^2 + d*s^2)/(d0 + d) * (1/n_a + 1/n_b)) with d0={d0:.2f}, df = d + d0",
    )
    check(
        np.allclose(moderated["p_value"], 2 * student_t.sf(np.abs(moderated["estimate"] / moderated["std_err"]), expected_df))
        and np.allclose(moderated["estimate"], plain_node["estimate"]),
        f"{name}: moderated t-test p-values; the estimates are unchanged",
    )

# Random effects (DerSimonian-Laird) by hand from the hospitals' moderated results
tables = [decode_table(r["dmps"]).set_index("probe_id") for r in node_results.values()]
probes = tables[0].index.intersection(tables[1].index)
est = np.column_stack([tb.loc[probes, "estimate"] for tb in tables])
var = np.column_stack([tb.loc[probes, "std_err"] ** 2 for tb in tables])
w = 1 / var
fe = (w * est).sum(1) / w.sum(1)
q = (w * (est - fe[:, None]) ** 2).sum(1)
tau2 = np.clip((q - 1) / (w.sum(1) - (w ** 2).sum(1) / w.sum(1)), 0, None)
w_re = 1 / (var + tau2[:, None])
re_est = (w_re * est).sum(1) / w_re.sum(1)
re_se = np.sqrt(1 / w_re.sum(1))
re = default_dmps.loc[probes]
check(
    np.allclose(re["estimate"].astype(float), re_est) and np.allclose(re["std_err"].astype(float), re_se)
    and np.allclose(re["tau2"].astype(float), tau2),
    "random-effects estimate, std_err and tau2 equal DerSimonian-Laird computed by hand",
)
check(
    (re["std_err"].astype(float) >= np.sqrt(1 / w.sum(1)) - 1e-12).all() and (tau2 > 0).any(),
    "random-effects standard errors are never smaller than fixed-effect ones (wider where hospitals disagree)",
)

# The artefact seen on real EPIC v2 data: a hospital with 3 + 3 samples whose
# variance is near zero by chance, pointing the other way than the other hospital
rng = np.random.default_rng(12)
artefact_probes = [f"cg{i:08d}" for i in range(300)]
artefact = []
for h, (n, sd) in enumerate([(6, 0.3), (3, 0.3)]):
    rows = []
    for cohort, shift in ((A, 0.0), (B, -0.2 if h == 0 else 0.8)):
        for i in range(n):
            m = rng.normal(0, sd, len(artefact_probes))
            m[0] = shift + rng.normal(0, 0.3 if h == 0 else 0.005)  # probe 0: tiny spread at hospital 2
            rows.append(pd.DataFrame({"probe_id": artefact_probes, "sample_label": f"H{h}{cohort}{i}", "cohort": cohort,
                                      "m_value": m, "beta": 1 / (1 + 2.0 ** -m)}))
    artefact.append(pd.concat(rows, ignore_index=True))
naive = aggregate_results([compute_methylation_impl(df, A, B, moderate_variance=False) for df in artefact],
                          A, B, meta_model="fixed")
robust = aggregate_results([compute_methylation_impl(df, A, B) for df in artefact], A, B)
p_naive = pd.DataFrame(naive["global_dmps"]).set_index("probe_id").loc["cg00000000", "adj_p_value"]
p_robust = pd.DataFrame(robust["global_dmps"]).set_index("probe_id").loc["cg00000000", "adj_p_value"]
check(
    p_naive < 1e-3 and p_robust > 0.05,
    f"a probe driven by one hospital's chance near-zero variance: adj p {p_naive:.1e} with plain fixed effect, "
    f"{p_robust:.2f} with moderation + random effects",
)
for bad in ("mixed", ""):
    try:
        aggregate_results(list(node_results.values()), A, B, meta_model=bad)
    except ValueError as exc:
        check("meta_model must be one of" in str(exc), f"meta_model={bad!r} is rejected")
    else:
        raise AssertionError(f"meta_model={bad!r} was accepted")

print("\nAll checks passed.")
