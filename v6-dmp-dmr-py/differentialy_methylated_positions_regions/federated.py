"""
This file contains all federated algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the federated task
or directly to the user (if they requested federated results).
"""
from __future__ import annotations

import base64
import io
from typing import Any

import pandas as pd
import numpy as np

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.action import federated
from vantage6.algorithm.decorator.data import dataframe

from pylluminator.samples import Samples
from pylluminator.dm import DM
import pylluminator.utils as py_utils
import pylluminator.dm as py_dm

from scipy.stats import t as student_t

from .annotation import annotate
from .moderation import fit_prior, squeeze_variances

# Per-probe statistics a hospital shares; the central function adds the annotation.
# std_err, df and p_value are moderated (empirical Bayes) unless moderate_variance=False
NODE_RESULT_COLUMNS = [
    "probe_id", "estimate", "std_err", "df", "p_value",
    "n_a", "n_b", "mean_beta_a", "mean_beta_b",
]
TABLE_FORMAT = "parquet+base64"


def encode_table(df: pd.DataFrame) -> dict:
    """A DataFrame as compressed Parquet in base64, so it travels as JSON.

    An EPIC v2 node result has ~870k probes: as JSON records it is ~240 MB, as
    Parquet a fraction of that, which keeps it well within what vantage6 stores.
    """
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression="zstd")
    return {"format": TABLE_FORMAT, "data": base64.b64encode(buffer.getvalue()).decode("ascii")}


def decode_table(table, columns: list[str] | None = None) -> pd.DataFrame:
    """Inverse of encode_table; also accepts a list of records (older results)."""
    if isinstance(table, dict) and table.get("format") == TABLE_FORMAT:
        return pd.read_parquet(io.BytesIO(base64.b64decode(table["data"])), columns=columns)
    return pd.DataFrame(table, columns=columns)


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
    # pylluminator expects an annotation on the Samples object; give it the probes'
    # locations from the packaged manifest table (no download, matched on IlmnID)
    if getattr(samples, "annotation", None) is None:
        try:
            df_manifest = annotate(df_matrix.index).dropna(subset=["chromosome"]).reset_index()
            df_manifest["position"] = df_manifest["position"].astype(int)
            info(f"Probe annotation: {len(df_manifest):,} of {len(df_matrix):,} probes found in the manifest")

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

        except Exception as e:
            warn(f"Failed to load the probe annotation: {e}")

    return samples


# ==============================================================================
# 1. RPC FUNCTION (runs locally on each node/hospital)
# ==============================================================================
def compute_methylation_impl(
    df_metilacion: pd.DataFrame,
    cohort_a: str,
    cohort_b: str,
    cohort_column: str = "cohort",
    use_m_values: bool = True,
    min_samples: int = 2,
    moderate_variance: bool = True,
) -> dict:
    """Compare two cohorts within this hospital, probe by probe.

    Parameters
    ----------
    df_metilacion : pd.DataFrame
        Long-format table with one row per (sample, probe) and the columns
        probe_id, sample_label, beta, m_value and ``cohort_column``
        (the output of v6-extractionIDAT-py).
    cohort_a, cohort_b : str
        Cohort labels to compare. ``cohort_a`` is the reference, so estimates are
        cohort_b - cohort_a.
    cohort_column : str
        Column holding the cohort of each sample.
    use_m_values : bool
        Fit the model on M-values (default) or on beta values.
    min_samples : int
        Minimum number of samples per cohort on this hospital (default 2). Below
        it, the hospital is skipped. Probes with fewer non-missing values per
        cohort are dropped. Hospitals share per-probe means and standard errors per
        cohort, so a small cohort reveals more about its patients.
    moderate_variance : bool
        Moderate the per-probe variances with limma's empirical Bayes method
        (default): each variance is shrunk towards a prior estimated from all
        probes, so chance near-zero variances in small cohorts no longer give
        near-zero standard errors. False keeps the plain two-sample statistics.

    Returns
    -------
    dict
        ``{"status": "ok", "n_a", "n_b", "dmps": table}`` where ``table`` is the
        encode_table() form of one row per probe with probe_id, estimate, std_err,
        df, p_value, n_a, n_b, mean_beta_a and mean_beta_b (std_err, df and
        p_value moderated unless moderate_variance=False), plus "moderated",
        "prior_df" and "prior_var"; or
        ``{"status": "skipped", "reason": ...}``.
    """
    if not isinstance(min_samples, int) or isinstance(min_samples, bool) or min_samples < 2:
        raise ValueError(f"min_samples must be an integer of at least 2, got {min_samples!r}")
    if cohort_a == cohort_b:
        raise ValueError("cohort_a and cohort_b must be different cohorts")

    if cohort_column not in df_metilacion.columns:
        error(f"Cohort column '{cohort_column}' not found in the node data")
        raise ValueError(
            f"Cohort column '{cohort_column}' not found in the node data. "
            f"Available columns: {sorted(map(str, df_metilacion.columns))}. "
            "Add a cohort column to the sample sheet or pass the correct name "
            "via the 'cohort_column' argument."
        )
    required = {"probe_id", "sample_label", "beta", "m_value"}
    missing = required - set(df_metilacion.columns)
    if missing:
        raise ValueError(f"Input data missing required columns: {sorted(missing)}")

    info(f"Starting local differential methylation analysis: {cohort_b} vs {cohort_a}")

    # 1. Keep only the samples of the two compared cohorts
    data = df_metilacion[["probe_id", "sample_label", "beta", "m_value", cohort_column]].rename(
        columns={cohort_column: "cohort"}
    )
    data = data[data["cohort"].isin([cohort_a, cohort_b])]
    sizes = data.groupby("cohort")["sample_label"].nunique()
    n_a, n_b = int(sizes.get(cohort_a, 0)), int(sizes.get(cohort_b, 0))

    if n_a < min_samples or n_b < min_samples:
        reason = (
            f"fewer than min_samples={min_samples} samples of '{cohort_a}' and/or "
            f"'{cohort_b}' on this node"
        )
        info(f"Skipping this node: {reason}")
        return {"status": "skipped", "reason": reason}

    info(f"Comparing {n_b} '{cohort_b}' samples with {n_a} '{cohort_a}' samples")

    # 2. Sample sheet and wide matrices (probes x samples)
    df_metadata = (
        data[["sample_label", "cohort"]].drop_duplicates()
        .rename(columns={"sample_label": "sample_id"})
        .reset_index(drop=True)
    )
    value_column = "m_value" if use_m_values else "beta"
    df_matriz = data.pivot(index="probe_id", columns="sample_label", values=value_column)
    df_matriz.index.name = "probe_id"
    df_matriz.columns.name = "sample_id"
    betas = data.pivot(index="probe_id", columns="sample_label", values="beta")
    info(f"Matrix shape: {df_matriz.shape}")

    # 3. Per-probe linear model with pylluminator (cohort_a is the reference level)
    my_samples = build_samples_from_matrix(df_matriz, df_metadata, use_m_values=use_m_values)
    my_samples.array_type = "EPICv2"
    my_dms = DM(
        my_samples,
        "~ cohort",
        reference_value={"cohort": cohort_a},
        probe_ids=df_matriz.index,
        use_m_values=use_m_values,
    )
    if my_dms is None or my_dms.dmp is None or not my_dms.contrasts:
        raise RuntimeError("pylluminator DMP computation failed")

    contrast = f"cohort[T.{cohort_b}]"
    if contrast not in my_dms.contrasts:
        raise RuntimeError(f"Unexpected contrasts from pylluminator: {my_dms.contrasts}")
    dmp = my_dms.dmp[[f"{contrast}_estimate", f"{contrast}_std_err", f"{contrast}_p_value"]]
    dmp.columns = ["estimate", "std_err", "p_value"]
    dmp = dmp.copy()
    dmp.index = dmp.index.astype(str)

    # 4. Per-cohort sample counts and mean beta values per probe
    samples_a = df_metadata.loc[df_metadata["cohort"] == cohort_a, "sample_id"]
    samples_b = df_metadata.loc[df_metadata["cohort"] == cohort_b, "sample_id"]
    analysed = df_matriz.notna()
    dmp["n_a"] = analysed[samples_a].sum(axis=1).reindex(dmp.index)
    dmp["n_b"] = analysed[samples_b].sum(axis=1).reindex(dmp.index)
    dmp["mean_beta_a"] = betas[samples_a].mean(axis=1).reindex(dmp.index)
    dmp["mean_beta_b"] = betas[samples_b].mean(axis=1).reindex(dmp.index)

    # A probe measured in too few samples of a cohort would expose those samples
    keep = (dmp["n_a"] >= min_samples) & (dmp["n_b"] >= min_samples) & dmp["std_err"].notna()
    if (~keep).any():
        info(f"Dropping {int((~keep).sum())} probe(s) with fewer than min_samples={min_samples} values per cohort")
    dmp = dmp[keep].copy()

    # 5. Residual degrees of freedom of the two-group model, and moderation of the
    # variances over all probes (limma's empirical Bayes): with few samples a
    # probe's variance can be near zero by chance, giving a near-zero standard error
    dmp["df"] = (dmp["n_a"] + dmp["n_b"] - 2).astype(float)
    prior = {"moderated": False, "prior_df": None, "prior_var": None}
    if moderate_variance:
        scale = 1 / dmp["n_a"] + 1 / dmp["n_b"]
        s2 = dmp["std_err"] ** 2 / scale
        d0, s0_2 = fit_prior(s2.to_numpy(), dmp["df"].to_numpy())
        s2_post = squeeze_variances(s2.to_numpy(), dmp["df"].to_numpy(), d0, s0_2)
        dmp["std_err"] = np.sqrt(s2_post * scale)
        dmp["df"] = dmp["df"] + (d0 if np.isfinite(d0) else 1e6)
        t_stat = dmp["estimate"] / dmp["std_err"]
        dmp["p_value"] = 2 * student_t.sf(np.abs(t_stat), dmp["df"])
        prior = {"moderated": True, "prior_df": d0 if np.isfinite(d0) else None, "prior_var": s0_2}
        info(f"Variances moderated: prior df d0={d0:.2f}, prior variance s0^2={s0_2:.4g}")

    # 6. Statistics only: the central function annotates the probes it reports
    dmp = dmp.reset_index(names="probe_id")
    dmp[["n_a", "n_b"]] = dmp[["n_a", "n_b"]].astype("int32")
    dmp = dmp[NODE_RESULT_COLUMNS]

    info(f"Node DMPs computed for {len(dmp)} probes")
    return {"status": "ok", "n_a": n_a, "n_b": n_b, **prior, "dmps": encode_table(dmp)}


@federated
@dataframe(1)
def rpc_compute_methylation(
    df_metilacion: pd.DataFrame,
    cohort_a: str,
    cohort_b: str,
    cohort_column: str = "cohort",
    use_m_values: bool = True,
    min_samples: int = 2,
    moderate_variance: bool = True,
):
    """Per-hospital comparison of two cohorts; see ``compute_methylation_impl``."""
    return compute_methylation_impl(
        df_metilacion, cohort_a, cohort_b, cohort_column, use_m_values, min_samples,
        moderate_variance,
    )
