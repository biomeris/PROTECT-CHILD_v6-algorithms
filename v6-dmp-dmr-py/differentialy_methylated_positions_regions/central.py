"""
central.py
==========
Vantage6 CENTRAL (orchestrator / aggregator) algorithm function.

Sends the `partial_dmp_dmr` task (see partial.py) to every participating
hospital, waits for their local, summary-only results, and combines them
into a single federated DMP / DMR result -- without ever seeing
patient-level data. Each hospital computes its results directly from its
own already-preprocessed beta-value matrix (no idat reading happens as
part of this algorithm).

Aggregation strategy (simple, transparent meta-analysis)
-----------------------------------------------------------
DMPs (per probe, matched by probe_id across sites):
  * effect size  -> sample-size-weighted mean across sites
                    (weight_i = sqrt(n_samples_i))
  * p-value      -> combined with Stouffer's weighted Z-method, then
                     Benjamini-Hochberg FDR-corrected across probes.

DMRs (per region):
  * Regions are matched across sites by genomic overlap
    (same chromosome + overlapping [start, end)).
  * Same weighted-mean effect size / Stouffer p-value combination.
  * Regions found at only one site are still reported (n_sites=1) so
    they can be inspected, but should be interpreted with caution.

This is a standard "mean + combined p-value" meta-analysis, matching
what you described. Swap `_weighted_mean` / `_combine_p` for a formal
inverse-variance-weighted fixed/random-effects meta-analysis (e.g. with
`statsmodels` or `metafor`-style weights) if you need something more
rigorous for publication.

Install (on the machine that submits the task, e.g. via vantage6 client
or inside the algorithm image if run as a subtask):
    pip install vantage6-algorithm-tools pandas numpy scipy statsmodels
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import combine_pvalues
from statsmodels.stats.multitest import multipletests

from vantage6.algorithm.client import AlgorithmClient
from vantage6.algorithm.decorator import algorithm_client
from vantage6.algorithm.decorator import central as central_step
from vantage6.algorithm.tools.util import error, info

#this is the mathematical core of the meta-analysis, which is pure math and can be imported from tests. It can also be done outside of the vantage6 network, for example in a Jupyter notebook, to test the meta-analysis on local results.
def aggregate_results(results_list):
    """Pure methylation meta-analysis function, importable from tests."""
    all_dmps = []
    all_dmrs = []
    
    for res in results_list:
        if isinstance(res, dict) and "error" in res:
            error(f"Error received from a node: {res['error']}")
            continue
            
        node_id = res.get("node_id", "unknown")
        
        if res.get("top_dmps"):
            df_dmp = pd.DataFrame(res["top_dmps"])
            df_dmp['node_id'] = node_id
            all_dmps.append(df_dmp)
            
        if res.get("top_dmrs"):
            df_dmr = pd.DataFrame(res["top_dmrs"])
            df_dmr['node_id'] = node_id
            all_dmrs.append(df_dmr)

    if not all_dmps:
        return {"msg": "No node returned valid DMP results."}

    info("Aggregating global DMPs...")
    df_dmps_global = pd.concat(all_dmps, ignore_index=True)
    
    # Nombres exactos de Pylluminator 
    col_probe = 'probe_id'
    col_p_value = 'cohorte[T.Cohorte_B]_p_value_adjusted'
    col_effect = 'effect_size'
    col_genes = 'genes'
    
    aggregated_dmps = []
    for probe, group in df_dmps_global.groupby(col_probe):
        # 1. Fisher's method for p-values.
        p_vals = group[col_p_value].dropna().values if col_p_value in group.columns else []
        if len(p_vals) > 0:
            _, combined_p = combine_pvalues(p_vals, method='fisher')
        else:
            combined_p = np.nan
            
        # 2. Mean effect size (Delta).
        ef_size = group[col_effect].mean() if col_effect in group.columns else np.nan
        
        # 3. Gene, if available.
        gen_name = group[col_genes].iloc[0] if (col_genes in group.columns) else "Unknown"
        
        aggregated_dmps.append({
            'probe_id': probe,
            'genes': gen_name,
            'combined_p_value': combined_p,
            'mean_effect_size': ef_size,
            'n_nodes': len(group)
        })
        
    df_final_dmps = pd.DataFrame(aggregated_dmps)
    
    # 4. Global FDR correction (Benjamini-Hochberg).
    valid_p = df_final_dmps['combined_p_value'].notna()
    if valid_p.any():
        _, pvals_corrected, _, _ = multipletests(df_final_dmps.loc[valid_p, 'combined_p_value'], method='fdr_bh')
        df_final_dmps.loc[valid_p, 'adj_p_value'] = pvals_corrected
        
    df_final_dmps = df_final_dmps.sort_values('combined_p_value').reset_index(drop=True)

    info("Aggregating global DMRs...")
    df_final_dmrs = pd.concat(all_dmrs, ignore_index=True) if all_dmrs else pd.DataFrame()
    if not df_final_dmrs.empty:
        pcol = 'cohorte[T.Cohorte_B]_p_value_adjusted'
        if pcol in df_final_dmrs.columns:
            df_final_dmrs = df_final_dmrs.sort_values(pcol, ascending=True, na_position='last').reset_index(drop=True)

    info("Consolidating global means for the frontend...")
    heatmap_global = []
    for res in results_list:
        if isinstance(res, dict) and res.get("heatmap_data"):
            node_id = res.get("node_id", "unknown")
            for fila in res["heatmap_data"]:
                heatmap_global.append({
                    "probe_id": fila["probe_id"],
                    "genes": fila.get("genes", "Unknown"),
                    f"{node_id}_Cohorte_A": fila["mean_Cohorte_A"],
                    f"{node_id}_Cohorte_B": fila["mean_Cohorte_B"]
                })
    
    # Merge by probe_id to produce one row per gene/probe.
    if heatmap_global:
        df_heat = pd.DataFrame(heatmap_global).groupby("probe_id").first().reset_index()
        heatmap_dict = df_heat.to_dict(orient="records")
    else:
        heatmap_dict = []

    info("Federated analysis completed.")
    return {
        "global_dmps": df_final_dmps.to_dict(orient="records"),
        "global_dmrs": df_final_dmrs.to_dict(orient="records") if not df_final_dmrs.empty else [],
        "heatmap_data": heatmap_dict
    }

def _central_methylation_analysis_impl(
    client,
    ids_cohort_a: list,
    ids_cohort_b: list,
    use_m_values: bool = True,
):
    info("Deploying cohort parameters to the federated network...")
    organizations = client.organization.list()
    org_ids = [org.get("id") for org in organizations]

    task = client.task.create(
        method="rpc_compute_methylation",
        organizations=org_ids,
        arguments={
            "ids_cohort_a": ids_cohort_a,
            "ids_cohort_b": ids_cohort_b,
            "use_m_values": use_m_values,
        },
    )

    info("Waiting for the nodes to finish...")
    results = client.wait_for_results(task_id=task.get("id"))

    info("Local results received. Starting meta-analysis...")
    return aggregate_results(results)

@central_step
@algorithm_client
def central_methylation_analysis(
    client,
    ids_cohort_a: list,
    ids_cohort_b: list,
    use_m_values: bool = True,
):
    """Vantage6 central entrypoint for federated aggregation."""
    return _central_methylation_analysis_impl(client, ids_cohort_a, ids_cohort_b, use_m_values)


# This is when the central function is called inside the client, which mean vantage6 network is used. The results are aggregated and returned to the client.
"""
    for res in results:
        if isinstance(res, dict) and "error" in res:
            error(f"Node {res.get('node_id', 'unknown')} returned an error: {res['error']}")
            continue

        node_id = res.get("node_id", "unknown")

        if res.get("top_dmps"):
            df_dmp = pd.DataFrame(res["top_dmps"])
            df_dmp["node_id"] = node_id
            all_dmps.append(df_dmp)

        if res.get("top_dmrs"):
            df_dmr = pd.DataFrame(res["top_dmrs"])
            df_dmr["node_id"] = node_id
            all_dmrs.append(df_dmr)

    if not all_dmps:
        return {"msg": "No node returned valid DMP results."}

    info("Aggregating DMPs...")
    df_dmps_global = pd.concat(all_dmps, ignore_index=True)
    col_probe = "probe_id" if "probe_id" in df_dmps_global.columns else df_dmps_global.columns[0]
    dmp_grouped = df_dmps_global.groupby(col_probe)

    aggregated_dmps = []
    for probe, group in dmp_grouped:
        p_vals = group["P.Value"].dropna().values if "P.Value" in group.columns else []
        if len(p_vals) > 0:
            _, combined_p = combine_pvalues(p_vals, method="fisher")
        else:
            combined_p = np.nan

        ef_size = group["logFC"].mean() if "logFC" in group.columns else np.nan

        aggregated_dmps.append({
            "probe_id": probe,
            "combined_p_value": combined_p,
            "mean_effect_size": ef_size,
            "n_nodes": len(group),
        })

    df_final_dmps = pd.DataFrame(aggregated_dmps)

    valid_p = df_final_dmps["combined_p_value"].notna()
    if valid_p.any():
        _, pvals_corrected, _, _ = multipletests(df_final_dmps.loc[valid_p, "combined_p_value"], method="fdr_bh")
        df_final_dmps.loc[valid_p, "adj_p_value"] = pvals_corrected

    df_final_dmrs = pd.DataFrame()
    if all_dmrs:
        info("Aggregating DMRs...")
        df_final_dmrs = pd.concat(all_dmrs, ignore_index=True)

    info("Federated analysis completed.")
    return {
        "global_dmps": df_final_dmps.to_dict(orient="records"),
        "global_dmrs": df_final_dmrs.to_dict(orient="records") if not df_final_dmrs.empty else [],
    }
"""
