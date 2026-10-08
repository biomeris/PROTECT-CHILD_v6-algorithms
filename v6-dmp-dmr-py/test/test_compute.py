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

The tests use min_samples=3 because the mock hospitals are small; real tasks
should use the default (10) or higher.
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

from differentialy_methylated_positions_regions.central import aggregate_results, find_dmrs  # noqa: E402
from differentialy_methylated_positions_regions.federated import compute_methylation_impl  # noqa: E402

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
node_results = {}
for name, df in raw.items():
    result = compute_methylation_impl(df, A, B, min_samples=MIN_SAMPLES)
    node_results[name] = result
    dmps = pd.DataFrame(result["dmps"]).set_index("probe_id")
    ref = reference_node(df).loc[dmps.index]
    print(f"\n{name}: {result['n_a']} x {A}, {result['n_b']} x {B}\n{dmps.head(3)}")

    check(result["status"] == "ok" and (result["n_a"], result["n_b"]) == (4, 5), f"{name}: 4 {A} and 5 {B} samples analysed")
    check(len(dmps) == df["probe_id"].nunique(), f"{name}: one row per probe")
    check(
        np.allclose(dmps["estimate"].astype(float), ref["estimate"])
        and np.allclose(dmps["std_err"].astype(float), ref["std_err"])
        and np.allclose(dmps["p_value"].astype(float), ref["p_value"]),
        f"{name}: estimate, std_err and raw p-value equal a two-sample t-test ({B} - {A})",
    )
    check(dmps[["chromosome", "position", "genes"]].notna().all().all(), f"{name}: every probe annotated from the manifest")

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
    arguments={"cohort_a": A, "cohort_b": B, "min_samples": MIN_SAMPLES},
    organizations=[org_ids[0]],
    databases=[{"type": "dataframe", "dataframe_id": client.dataframe.list()[0]["id"]}],
)
output = client.wait_for_results(task.get("id"))[0]
json.dumps(output)  # must be JSON-serialisable for the vantage6 server
global_dmps = pd.DataFrame(output["global_dmps"]).set_index("probe_id")
print_results(output, "Hospital A + Hospital B")

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
check(with_c == node_results["Hospital A"], "adding cohort_C patients to Hospital A does not change its result")

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
manifest = pd.read_csv(
    os.environ.get("MANIFEST_PATH", "test/EPIC-8v2-0_A2.csv"),
    skiprows=7, usecols=["Name", "CHR", "MAPINFO"], dtype={"Name": str, "CHR": str},
).dropna()
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
node_a = pd.DataFrame(node_results["Hospital A"]["dmps"]).set_index("probe_id").loc[single_dmps.index]
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

print("\nAll checks passed.")
