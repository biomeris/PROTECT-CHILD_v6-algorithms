"""
This file contains all federated algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the federated task
or directly to the user (if they requested federated results).
"""
from __future__ import annotations

import traceback
from typing import Any
import inspect 

import pandas as pd
import numpy as np
import os
from scipy.stats import ttest_ind

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.action import federated
from vantage6.algorithm.decorator.data import dataframe

from pylluminator.samples import Samples
from pylluminator.dm import DM
from pylluminator.utils import save_object
import pylluminator.utils as py_utils
import pylluminator.dm as py_dm

# Build a pylluminator Samples object from precomputed beta/M matrix and sample-sheet.
def build_samples_from_matrix(df_matrix: pd.DataFrame, df_metadata: pd.DataFrame, use_m_values: bool = True) -> Samples:
    
    # Ensures column/index names match Pylluminator expectations and derives _betas/_m.
    # Work on copies
    mat = df_matrix.copy()
    meta = df_metadata.copy()

    # Ensure sample label column exists in metadata
    if 'sample_id' not in meta.columns:
        # If metadata indexed by sample_id, reset it
        if meta.index.name == 'sample_id':
            meta = meta.reset_index()
        else:
            # try to coerce by resetting index and naming it sample_id
            meta = meta.reset_index().rename(columns={meta.index.name: 'sample_id'}) if meta.index.name is not None else meta.reset_index()

    sample_label = 'sample_id'

    # Ensure matrix columns and index names match Pylluminator expectations
    mat.columns = mat.columns.astype(str)
    mat.columns.name = sample_label
    mat.index.name = 'probe_id'

    # Instantiate Samples
    samples = Samples(sample_sheet_df=meta)
    samples._signal_df = mat
    # Force copy and ensure Samples internal signal_df index has proper name
    try:
        samples._signal_df = samples._signal_df.copy()
        samples._signal_df.index.name = 'probe_id'
        samples._signal_df.columns.name = sample_label
    except Exception:
        pass
    samples.use_m_values = use_m_values

    # Populate M and beta matrices
    if use_m_values:
        samples._m = mat
        samples._betas = samples._m_to_betas(mat)
        samples._beta = samples._betas
    else:
        samples._betas = mat
        samples._m = samples._betas_to_m(mat)
        samples._beta = samples._betas

    # Ensure probe index is named for downstream functions
    for attr in ('_m', '_betas', '_beta'):
        df = getattr(samples, attr, None)
        if isinstance(df, pd.DataFrame):
            # enforce a copy so index naming sticks
            df = df.copy()
            df.index.name = 'probe_id'
            setattr(samples, attr, df)
    # (adapter: leave internal DataFrames named appropriately)

    # Monkey-patch pylluminator's set_level_as_index to be tolerant of DataFrames where probe_id is a column
    try:
        original_set_level = py_utils.set_level_as_index

        def tolerant_set_level_as_index(df, level, drop_others=False):
            try:
                # if index already named, return
                if getattr(df.index, 'name', None) == level:
                    return df
            except Exception:
                pass
            # if index has no name, set it to the requested level
            try:
                if getattr(df.index, 'name', None) is None:
                    df = df.copy()
                    df.index.name = level
                    return df
            except Exception:
                pass
            # if level exists as a column, set it as index
            if level in df.columns:
                try:
                    if drop_others:
                        return df.reset_index().reset_index(drop=True).set_index(level)
                    else:
                        return df.reset_index().set_index(level)
                except Exception:
                    return df.set_index(level)
            # fallback to original
            return original_set_level(df, level, drop_others=drop_others)

        py_utils.set_level_as_index = tolerant_set_level_as_index
        try:
            py_dm.set_level_as_index = tolerant_set_level_as_index
        except Exception:
            pass
    except Exception:
        pass
    
# --- ANNOTATION EPIC v2 ---
    ruta_manifiesto = os.environ.get("MANIFEST_PATH", "test/EPIC-8v2-0_A2.csv")

    if getattr(samples, "annotation", None) is None:
        try:
            info(f"Loading EPIC v2 manifest from: {ruta_manifiesto}")

            df_manifest = pd.read_csv(ruta_manifiesto, low_memory=False, skiprows=7)

            required_columns = {"Name", "CHR", "MAPINFO", "UCSC_RefGene_Name"}
            missing = sorted(required_columns - set(df_manifest.columns))
            if missing:
                raise ValueError(f"Manifest EPIC v2 missing required columns: {missing}")

            df_manifest = df_manifest.rename(columns={
                "Name": "probe_id",
                "CHR": "chromosome",
                "MAPINFO": "position",
                "UCSC_RefGene_Name": "genes"
            })

            df_manifest = df_manifest[df_manifest["probe_id"].isin(df_matrix.index)].copy()
            df_manifest = df_manifest.drop_duplicates(subset="probe_id")

            df_manifest["probe_id"] = df_manifest["probe_id"].astype(str)
            df_manifest["genes"] = df_manifest["genes"].fillna("Intergenic").astype(str)

            df_manifest["chromosome"] = (
                df_manifest["chromosome"]
                .astype(str)
                .str.replace("chr", "", regex=False)
                .apply(lambda x: f"chr{x}" if x not in ["", "nan", "None"] and not str(x).startswith("chr") else x)
            )
            df_manifest["position"] = pd.to_numeric(df_manifest["position"], errors="coerce").fillna(0).astype(int)

            class RealAnnotation:
                def __init__(self, df_anotacion):
                    self.probe_infos = df_anotacion.copy()
                    self.probe_infos = self.probe_infos.set_index("probe_id", drop=False)
                    self.probe_infos.index.name = None

                    self.genomic_ranges = self.probe_infos[["probe_id", "chromosome", "position"]].copy()
                    self.genomic_ranges["start"] = pd.to_numeric(self.genomic_ranges["position"], errors="coerce").fillna(0).astype(int)
                    self.genomic_ranges["end"] = self.genomic_ranges["start"] + 1
                    self.genomic_ranges = self.genomic_ranges[["probe_id", "chromosome", "start", "end"]].set_index("probe_id")
                    self.genomic_ranges.index.name = "probe_id"

            samples.annotation = RealAnnotation(df_manifest)
            info("Real annotation EPIC v2 injected successfully.")

        except Exception as e:
            warn(f"Failed to load EPICv2 manifest: {e}")

    return samples


# ==============================================================================
# 1. RPC FUNCTION (runs locally on each node/hospital)
# ==============================================================================
@federated
@dataframe(1)
def rpc_compute_methylation(
    df_metilacion: pd.DataFrame, 
    ids_cohort_a: list, 
    ids_cohort_b: list, 
    use_m_values: bool = True
):
    info("Starting local differential methylation analysis (long format)...")
    
    # 1. Filter rows: Keep only patients belonging to the global cohorts
    pacientes_validos_globales = set(ids_cohort_a + ids_cohort_b)
    df_filtrado = df_metilacion[df_metilacion['sample_id'].isin(pacientes_validos_globales)]
    
    pacientes_locales = df_filtrado['sample_id'].unique().tolist()
    info(f"Retained {len(pacientes_locales)} local patients corresponding to the study cohorts.")

    # 2. Generate the virtual Sample Sheet
    metadata_lista = []
    for pid in pacientes_locales:
        cohorte = 'Cohorte_A' if pid in ids_cohort_a else 'Cohorte_B'
        metadata_lista.append({
            'sample_id': pid,
            'cohorte': cohorte
        })

    df_metadata = pd.DataFrame(metadata_lista)

    # Keep `sample_id` as a column — Pylluminator expects the sample label as a column
    info(f"Metadata columns: {df_metadata.columns.tolist()}")
    info(f"Metadata sample ids (first 10): {df_metadata['sample_id'].tolist()[:10]}")

    # Security verification: Ensure there are at least two groups in the metadata for contrast
    if df_metadata['cohorte'].nunique() < 2:
        msg = "Not enough groups in this node to perform the methylation contrast."
        error(msg)
        return {"node_id": os.environ.get("NODE_ID", "unknown"), "error": msg}

    # 3. Pivot from Long Format to Wide Matrix (Pylluminator expects a matrix)
    # Rename 'beta_value' to 'beta' and 'm_value' to 'M' to match what Pylluminator expects
    mapping = {'beta_value': 'beta', 'm_value': 'M'}
    df_filtrado_renombrado = df_filtrado.rename(columns=mapping)
    
    col_selected = 'M' if use_m_values else 'beta'
    info(f"Pivoting matrix using column: {col_selected}")
    
    df_matriz = df_filtrado_renombrado.pivot(index='probe_id', columns='sample_id', values=col_selected)

    df_matriz.index.name = 'probe_id'  
    # Ensure the column name matches the sample-sheet column used by Pylluminator
    df_matriz.columns.name = 'sample_id'
    info(f"Matrix shape: {df_matriz.shape}")
    info(f"Matrix columns (first 10): {list(df_matriz.columns[:10])}")
    info(f"Matrix index name: {df_matriz.index.name}")
    
    # 4. Pylluminator: build Samples from precomputed matrix
    try:
        my_samples = build_samples_from_matrix(df_matriz, df_metadata, use_m_values=use_m_values)
        my_samples.array_type = 'EPICv2'
    except Exception as e:
        error(f"Failure in the injection/build Samples: {e}")
        return {"error": str(e)}
    
    try:
        info("_signal_df index name: {}".format(getattr(my_samples._signal_df.index, 'name', None)))
    except Exception:
        info("_signal_df index name: <unavailable>")
    all_pct_probes = int(my_samples.nb_probes)
    # Ensure my_samples._betas index name is set immediately before DM initialization
    try:
        if hasattr(my_samples, '_betas') and isinstance(my_samples._betas, pd.DataFrame):
            my_samples._betas = my_samples._betas.copy()
            my_samples._betas.index.name = 'probe_id'
        info("_betas index name: {}".format(getattr(my_samples._betas.index, 'name', None)))
    except Exception:
        info("_betas index name: <unavailable>")

    # Prefer using the _betas index which the adapter controls
    if hasattr(my_samples, '_betas') and isinstance(my_samples._betas, pd.DataFrame):
        probe_ids = my_samples._betas.index[:all_pct_probes]
    else:
        probe_ids = df_matriz.index[:all_pct_probes]

    # 5. DMs & DMRs
    formula_dinamica = '~ cohorte'
    info(f"Calculating DMs. Formula: {formula_dinamica}")
    
    try:
        info("_m index name: {}".format(getattr(my_samples._m.index, 'name', None)))
    except Exception:
        info("_m index name: <unavailable>")
    try:
        info("_betas index name: {}".format(getattr(my_samples._betas.index, 'name', None)))
    except Exception:
        info("_betas index name: <unavailable>")
    my_dms = None
    try:
        my_dms = DM(my_samples, formula_dinamica, probe_ids=probe_ids, use_m_values=use_m_values)
    except Exception as e:
        warn(f"Pylluminator DM initialization failed: {e}")
        my_dms = None

    """
    # Extract DMPs with essential information (probe_id, logFC, P.Value, etc.)
    top_dmps_df = []
    if my_dms is not None and getattr(my_dms, 'contrasts', None):
        info("Extracting DMP results...")
        try:
            top_dmps_df = my_dms.get_top_dmp(my_dms.contrasts[0])
        except Exception as e:
            warn(f"Error extracting DMPs: {e}")
    """

    # Extract DMPs with all the information in the manifest (probe_id, genes, chromosome, position, etc.)
    top_dmps_df = []
    if my_dms is not None and getattr(my_dms, 'contrasts', None):
        info("Extracting DMP results with full EPIC v2 annotation...")
        try:
            raw_top_dmps = my_dms.get_top_dmp(my_dms.contrasts[0])
            
            if isinstance(raw_top_dmps, pd.DataFrame) and not raw_top_dmps.empty:
                if 'probe_id' not in raw_top_dmps.columns:
                    raw_top_dmps = raw_top_dmps.reset_index().rename(columns={'index': 'probe_id'})
                
                # Avoid collisions when manifest and result columns share names.
                manifest_df = my_samples.annotation.probe_infos.reset_index(drop=True)

                cols_to_drop = [
                    column
                    for column in manifest_df.columns
                    if column in raw_top_dmps.columns and column != "probe_id"
                ]
                manifest_clean = manifest_df.drop(columns=cols_to_drop)

                top_dmps_df = raw_top_dmps.merge(
                    manifest_clean,
                    on="probe_id",
                    how="left",
                )

                top_dmps_df = top_dmps_df.replace({np.nan: None})
            else:
                top_dmps_df = raw_top_dmps
        except Exception as e:
            warn(f"Error extracting DMPs with full annotation: {e}")    


    # Fallback: calculate a simple per-probe t-test if Pylluminator returns no DMPs or fails to initialize.
    if not isinstance(top_dmps_df, pd.DataFrame) or (isinstance(top_dmps_df, pd.DataFrame) and top_dmps_df.empty):
        info("Pylluminator returned no DMPs; using the per-probe t-test fallback.")
        fallback_dmps = []
        try:
            if None in df_metadata.columns:
                grpA = df_metadata[df_metadata[None] == 'Cohorte_A']['sample_id'].tolist()
                grpB = df_metadata[df_metadata[None] == 'Cohorte_B']['sample_id'].tolist()
            elif 'cohorte' in df_metadata.columns:
                grpA = df_metadata[df_metadata['cohorte'] == 'Cohorte_A']['sample_id'].tolist()
                grpB = df_metadata[df_metadata['cohorte'] == 'Cohorte_B']['sample_id'].tolist()
            else:
                warn("No group column found in metadata; skipping fallback.")
                fallback_dmps = []
                grpA = grpB = []

            if grpA and grpB:
                probes = df_matriz.index.tolist()
                records = []
                for probe in probes:
                    valsA = df_matriz.loc[probe, grpA].dropna().astype(float).values
                    valsB = df_matriz.loc[probe, grpB].dropna().astype(float).values
                    if len(valsA) < 2 or len(valsB) < 2:
                        continue
                    tstat, pval = ttest_ind(valsA, valsB, equal_var=False, nan_policy='omit')
                    meanA = np.nanmean(valsA)
                    meanB = np.nanmean(valsB)
                    records.append({
                        'probe_id': probe,
                        'logFC': meanA - meanB,
                        'P.Value': float(pval)
                    })

                fallback_dmps = pd.DataFrame(records).sort_values('P.Value').reset_index(drop=True)
                info(f"Fallback DMPs computed: {len(fallback_dmps)}")
        except Exception as e:
            warn(f"Fallback DMP calculation failed: {e}")
            fallback_dmps = []

        top_dmps_df = fallback_dmps if isinstance(fallback_dmps, pd.DataFrame) else top_dmps_df

    # Extract DMRs
    info("Calculating DMRs...")
    top_dmrs_df = []
    if my_dms is not None and getattr(my_samples, "annotation", None) is not None:
        try:
            my_dms.compute_dmr(my_dms.contrasts)
            top_dmrs_df = my_dms.get_top_dmr(my_dms.contrasts[0])
        except Exception as e:
            warn(f"DMR computation skipped due to missing annotation or unsupported input: {e}")
            top_dmrs_df = []
    else:
        warn("No probe annotation available or Pylluminator DM initialization failed; skipping DMR computation.")

    contrasts = getattr(my_dms, 'contrasts', None) or [formula_dinamica]

    # Build the heatmap list; it is not needed for the federated analysis itself.
    info("Calculating means for the clinical heatmap (top 50 DMPs)...")
    heatmap_data = []
    if isinstance(top_dmps_df, pd.DataFrame) and not top_dmps_df.empty:
        top_50_probes = top_dmps_df['probe_id'].head(50).tolist()
        
        pac_A = df_metadata[df_metadata['cohorte'] == 'Cohorte_A']['sample_id'].tolist()
        pac_B = df_metadata[df_metadata['cohorte'] == 'Cohorte_B']['sample_id'].tolist()
        
        for probe in top_50_probes:
            mean_A = df_matriz.loc[probe, pac_A].mean() if pac_A else None
            mean_B = df_matriz.loc[probe, pac_B].mean() if pac_B else None
            
            # Look up the gene name in the DMP table, if available.
            gen = top_dmps_df.loc[top_dmps_df['probe_id'] == probe, 'genes'].values[0] if 'genes' in top_dmps_df.columns else "Unknown"

            heatmap_data.append({
                "probe_id": probe,
                "genes": gen,
                "mean_Cohorte_A": mean_A,
                "mean_Cohorte_B": mean_B
            })

    return {
        "node_id": os.environ.get("NODE_ID", "unknown"),
        "contrasts": contrasts,
        "top_dmps": top_dmps_df.to_dict(orient="records") if isinstance(top_dmps_df, pd.DataFrame) else top_dmps_df,
        "top_dmrs": top_dmrs_df.to_dict(orient="records") if isinstance(top_dmrs_df, pd.DataFrame) else top_dmrs_df,
        "heatmap_data": heatmap_data
    }