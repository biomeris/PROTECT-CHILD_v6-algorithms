"""
central.py
==========
Vantage6 CENTRAL (orchestrator / aggregator) algorithm function.

Sends ``rpc_compute_methylation`` to every participating hospital. Each hospital
compares two cohorts (``cohort_a`` vs ``cohort_b``) within its own patients and
returns per-probe summary statistics only. The central function combines them
into federated DMPs and derives DMRs from them -- without ever seeing
patient-level data. Comparing cohorts within each hospital keeps hospital
differences (batch, scanner, protocol) out of the cohort effect. Hospitals that do
not hold enough samples of both cohorts are skipped and reported.

DMPs (per probe, matched by probe_id across hospitals)
------------------------------------------------------
Fixed-effect inverse-variance meta-analysis of the per-hospital estimates
(cohort_b - cohort_a). The estimate is on the scale the model was fitted on
(``estimate_scale``: M-value by default, beta if use_m_values=False); an M-value
difference is not a beta difference. ``delta_beta`` gives the difference of the
pooled mean beta values for reading the effect on the beta scale.
  * weight_i         = 1 / std_err_i^2
  * estimate         = sum(weight_i * estimate_i) / sum(weight_i)
  * std_err          = sqrt(1 / sum(weight_i))
  * z, p_value       = estimate / std_err, two-sided normal p-value
  * adj_p_value      = Benjamini-Hochberg over all probes
  * heterogeneity_p_value = Cochran's Q test; None when only one hospital
                         contributed to the probe (it cannot be assessed)

If only one hospital takes part, the results equal that hospital's own analysis
(with a normal instead of a t approximation) and the output says so in
``warnings``.

DMRs (from the federated DMPs)
------------------------------
Probes are ordered by chromosome and position (unmapped probes, with chromosome
0 or position 0 in the manifest, are left out). A region is a run of consecutive
probes that all have adj_p_value < dmr_fdr and the same direction of effect, with
at most ``dmr_max_gap`` bp between neighbours, and at least ``dmr_min_probes``
probes. Regions are ranked by ``max_probe_adj_p_value`` (the least significant
probe of the region), then by number of probes and absolute mean estimate.
``stouffer_p_value`` combines the probe z-scores assuming independent probes;
neighbouring CpGs are correlated, so it is far too small and is descriptive only
(it is not adjusted and should not be used to call regions).
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import chi2, norm
from statsmodels.stats.multitest import multipletests

from vantage6.algorithm.client import AlgorithmClient
from vantage6.algorithm.decorator import algorithm_client
from vantage6.algorithm.decorator import central as central_step
from vantage6.algorithm.tools.util import info, warn

from .federated import NODE_RESULT_COLUMNS

DMP_COLUMNS = [
    "probe_id", "chromosome", "position", "genes",
    "estimate", "std_err", "estimate_scale", "z", "p_value", "adj_p_value",
    "heterogeneity_p_value", "mean_beta_a", "mean_beta_b", "delta_beta",
    "n_nodes", "n_samples_a", "n_samples_b",
]
DMR_COLUMNS = [
    "chromosome", "start", "end", "n_probes", "probe_ids", "genes", "direction",
    "mean_estimate", "estimate_scale", "mean_delta_beta", "max_probe_adj_p_value",
    "stouffer_p_value",
]


def _json_records(df: pd.DataFrame) -> list[dict]:
    """DataFrame -> list of dicts with NaN replaced by None (valid JSON)."""
    return df.astype(object).where(df.notna(), None).to_dict(orient="records")


def _meta_analyse_dmps(node_dmps: pd.DataFrame, scale: str) -> pd.DataFrame:
    """Inverse-variance fixed-effect meta-analysis per probe."""
    d = node_dmps.copy()
    for col in ["estimate", "std_err", "n_a", "n_b", "mean_beta_a", "mean_beta_b"]:
        d[col] = pd.to_numeric(d[col], errors="coerce")
    d = d[np.isfinite(d["estimate"]) & np.isfinite(d["std_err"]) & (d["std_err"] > 0)]

    d["w"] = 1.0 / d["std_err"] ** 2
    d["w_est"] = d["w"] * d["estimate"]
    d["w_est2"] = d["w"] * d["estimate"] ** 2
    d["sum_beta_a"] = d["n_a"] * d["mean_beta_a"]
    d["sum_beta_b"] = d["n_b"] * d["mean_beta_b"]

    g = d.groupby("probe_id").agg(
        w=("w", "sum"),
        w_est=("w_est", "sum"),
        w_est2=("w_est2", "sum"),
        n_nodes=("w", "size"),
        n_samples_a=("n_a", "sum"),
        n_samples_b=("n_b", "sum"),
        sum_beta_a=("sum_beta_a", "sum"),
        sum_beta_b=("sum_beta_b", "sum"),
        chromosome=("chromosome", "first"),
        position=("position", "first"),
        genes=("genes", "first"),
    )

    g["estimate"] = g["w_est"] / g["w"]
    g["std_err"] = np.sqrt(1.0 / g["w"])
    g["z"] = g["estimate"] / g["std_err"]
    g["p_value"] = 2 * norm.sf(np.abs(g["z"]))
    q = (g["w_est2"] - g["w_est"] ** 2 / g["w"]).clip(lower=0)
    g["heterogeneity_p_value"] = np.where(
        g["n_nodes"] > 1, chi2.sf(q, (g["n_nodes"] - 1).clip(lower=1)), np.nan
    )
    g["adj_p_value"] = multipletests(g["p_value"], method="fdr_bh")[1] if len(g) else []
    g["mean_beta_a"] = g["sum_beta_a"] / g["n_samples_a"]
    g["mean_beta_b"] = g["sum_beta_b"] / g["n_samples_b"]
    g["delta_beta"] = g["mean_beta_b"] - g["mean_beta_a"]
    g["estimate_scale"] = scale

    g = g.reset_index().sort_values(["p_value", "probe_id"]).reset_index(drop=True)
    g["n_nodes"] = g["n_nodes"].astype(int)
    g["n_samples_a"] = g["n_samples_a"].astype(int)
    g["n_samples_b"] = g["n_samples_b"].astype(int)
    return g


def find_dmrs(
    dmps: pd.DataFrame,
    max_gap: int = 1000,
    fdr: float = 0.05,
    min_probes: int = 2,
) -> pd.DataFrame:
    """Group significant, same-direction neighbouring DMPs into regions."""
    d = dmps.dropna(subset=["chromosome", "position"]).copy()
    d["position"] = pd.to_numeric(d["position"], errors="coerce")
    # Unmapped probes have chromosome 0 / position 0 in the manifest; they have no
    # genomic location and must not be grouped into regions
    unmapped = (d["position"].isna() | (d["position"] <= 0)
                | d["chromosome"].astype(str).str.lower().isin(["0", "chr0", "nan", "none", ""]))
    d = d[~unmapped]
    if d.empty:
        return pd.DataFrame(columns=DMR_COLUMNS)
    d["position"] = d["position"].astype(int)
    d = d.sort_values(["chromosome", "position"]).reset_index(drop=True)

    significant = (d["adj_p_value"] < fdr).to_numpy()
    sign = np.sign(d["estimate"]).to_numpy()
    chrom = d["chromosome"].to_numpy()
    pos = d["position"].to_numpy()

    runs, current = [], []
    for i in range(len(d)):
        continues = (
            current
            and significant[i]
            and chrom[i] == chrom[current[-1]]
            and pos[i] - pos[current[-1]] <= max_gap
            and sign[i] == sign[current[-1]]
        )
        if continues:
            current.append(i)
            continue
        if len(current) >= min_probes:
            runs.append(current)
        current = [i] if significant[i] else []
    if len(current) >= min_probes:
        runs.append(current)

    rows = []
    for run in runs:
        r = d.iloc[run]
        z = r["z"].sum() / np.sqrt(len(r))  # Stouffer, assumes independent probes
        genes = sorted({g for gs in r["genes"].dropna() for g in str(gs).split(";") if g and g != "Intergenic"})
        rows.append({
            "chromosome": r["chromosome"].iloc[0],
            "start": int(r["position"].min()),
            "end": int(r["position"].max()),
            "n_probes": len(r),
            "probe_ids": ";".join(r["probe_id"]),
            "genes": ";".join(genes) if genes else "Intergenic",
            "direction": "hyper" if r["estimate"].iloc[0] > 0 else "hypo",
            "mean_estimate": float(r["estimate"].mean()),
            "estimate_scale": r["estimate_scale"].iloc[0] if "estimate_scale" in r else None,
            "mean_delta_beta": float(r["delta_beta"].mean()) if "delta_beta" in r else None,
            "max_probe_adj_p_value": float(r["adj_p_value"].max()),
            "stouffer_p_value": float(2 * norm.sf(abs(z))),
        })

    dmrs = pd.DataFrame(rows, columns=DMR_COLUMNS)
    if dmrs.empty:
        return dmrs
    dmrs["_abs_estimate"] = dmrs["mean_estimate"].abs()
    dmrs = dmrs.sort_values(
        ["max_probe_adj_p_value", "n_probes", "_abs_estimate"], ascending=[True, False, False]
    )
    return dmrs[DMR_COLUMNS].reset_index(drop=True)


def aggregate_results(
    results_list: list,
    cohort_a: str,
    cohort_b: str,
    use_m_values: bool = True,
    dmr_max_gap: int = 1000,
    dmr_fdr: float = 0.05,
    dmr_min_probes: int = 2,
    heatmap_top: int = 50,
) -> dict:
    """Pure meta-analysis of the node results, importable from tests."""
    node_dmps, skipped = [], []
    for res in results_list:
        if isinstance(res, dict) and res.get("status") == "ok":
            node_dmps.append(pd.DataFrame(res["dmps"], columns=NODE_RESULT_COLUMNS))
        elif isinstance(res, dict) and res.get("status") == "skipped":
            skipped.append(res.get("reason", "skipped"))
        else:
            warn(f"Unexpected node result ignored: {str(res)[:200]}")
            skipped.append("unexpected result")

    scale = "M-value" if use_m_values else "beta"
    output = {
        "comparison": {
            "cohort_a": cohort_a,
            "cohort_b": cohort_b,
            "estimate": f"{cohort_b} - {cohort_a}",
            "estimate_scale": scale,
        },
        "n_nodes_used": len(node_dmps),
        "skipped_nodes": skipped,
        "warnings": [],
        "global_dmps": [],
        "global_dmrs": [],
        "heatmap_data": [],
    }
    if skipped:
        info(f"{len(skipped)} node(s) skipped")
    if not node_dmps:
        message = "No hospital had enough samples of both cohorts; there are no results."
        warn(message)
        output["warnings"].append(message)
        return output
    if len(node_dmps) == 1:
        message = (
            "Only 1 hospital contributed: the results equal that hospital's own analysis "
            "(normal approximation), there is no replication across hospitals and "
            "heterogeneity cannot be assessed (heterogeneity_p_value is empty)."
        )
        warn(message)
        output["warnings"].append(message)

    info(f"Meta-analysing DMPs from {len(node_dmps)} node(s)...")
    dmps = _meta_analyse_dmps(pd.concat(node_dmps, ignore_index=True), scale)

    info("Deriving DMRs from the federated DMPs...")
    dmrs = find_dmrs(dmps, max_gap=dmr_max_gap, fdr=dmr_fdr, min_probes=dmr_min_probes)

    heatmap = dmps.head(heatmap_top)[["probe_id", "genes", "mean_beta_a", "mean_beta_b"]].rename(
        columns={"mean_beta_a": f"mean_beta_{cohort_a}", "mean_beta_b": f"mean_beta_{cohort_b}"}
    )

    output["global_dmps"] = _json_records(dmps[DMP_COLUMNS])
    output["global_dmrs"] = _json_records(dmrs)
    output["heatmap_data"] = _json_records(heatmap)
    info(f"Federated analysis completed: {len(dmps)} probes, {len(dmrs)} DMRs")
    return output


@central_step
@algorithm_client
def central_methylation_analysis(
    client: AlgorithmClient,
    cohort_a: str,
    cohort_b: str,
    cohort_column: str = "cohort",
    use_m_values: bool = True,
    min_samples: int = 10,
    dmr_max_gap: int = 1000,
    dmr_fdr: float = 0.05,
    dmr_min_probes: int = 2,
) -> Any:
    """Federated DMP/DMR analysis of cohort_b vs cohort_a.

    Parameters
    ----------
    cohort_a, cohort_b : str
        Cohort labels (values of the cohort column) to compare; cohort_a is the
        reference.
    cohort_column : str
        Column in each node's data that holds the cohort of each sample.
    use_m_values : bool
        Fit the per-probe models on M-values (default) or beta values.
    min_samples : int
        Minimum samples per cohort a hospital needs to take part (at least 2,
        default 10). Hospitals share per-probe means and standard errors per
        cohort, so small values reveal a lot about individual patients.
    dmr_max_gap : int
        Maximum distance in bp between neighbouring probes of a DMR.
    dmr_fdr : float
        Probe adj_p_value threshold for DMR probes.
    dmr_min_probes : int
        Minimum number of probes in a DMR.
    """
    info(f"Federated DMP/DMR analysis: {cohort_b} vs {cohort_a}")
    organizations = client.organization.list()
    org_ids = [org.get("id") for org in organizations]

    task = client.task.create(
        method="rpc_compute_methylation",
        organizations=org_ids,
        arguments={
            "cohort_a": cohort_a,
            "cohort_b": cohort_b,
            "cohort_column": cohort_column,
            "use_m_values": use_m_values,
            "min_samples": min_samples,
        },
    )

    info("Waiting for the nodes to finish...")
    results = client.wait_for_results(task_id=task.get("id"))

    info("Local results received. Starting meta-analysis...")
    return aggregate_results(
        results, cohort_a, cohort_b, use_m_values,
        dmr_max_gap=dmr_max_gap, dmr_fdr=dmr_fdr, dmr_min_probes=dmr_min_probes,
    )
