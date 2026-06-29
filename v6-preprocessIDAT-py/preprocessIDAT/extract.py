"""
This file contains all data preprocessing algorithm functions.

We use pylluminator to perform QC and compute Beta and M-values from IDAT files.
The preprocessing function expects the extraction to return a dataframe with an
`idat_dir` column (or will fall back to the included `test/` folder).
"""
from __future__ import annotations
from pathlib import Path
from typing import Any

import pandas as pd
from pylluminator.samples import read_samples
from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.action import preprocessing

DEFAULT_TEST_FOLDER = Path(__file__).resolve().parents[1] / "test"


def preprocessing_impl(
    df1: pd.DataFrame,
    idat_dir: str | None = None,
    pvalue_threshold: float = 0.05,
    min_beads: int = 1,
) -> pd.DataFrame:
    """Preprocess IDAT data and return a merged table of Beta and M values.

    Steps performed:
    - load IDATs from provided `idat_dir` or fallback `test/`
    - pOOBAH detection p-values and masking (threshold: `pvalue_threshold`)
    - mask control, quality, non-CpG, SNP and sex-chromosome probes
    - non-linear dye-bias correction
    - NOOB background correction
    - compute Beta and M values and return a long-form table
    """

    if idat_dir is None:
        idat_dir = DEFAULT_TEST_FOLDER
    elif isinstance(idat_dir, str):
        idat_dir = Path(idat_dir)
    elif isinstance(df1, pd.DataFrame) and "idat_dir" in df1.columns and len(df1["idat_dir"].dropna()) > 0:
        idat_dir = Path(df1["idat_dir"].iloc[0])

    if not idat_dir.exists():
        warn(f"IDAT folder {idat_dir} does not exist, using fallback {DEFAULT_TEST_FOLDER}")
        idat_dir = DEFAULT_TEST_FOLDER

    info(f"Loading IDAT samples from {idat_dir}")
    samples = read_samples(idat_dir, min_beads=min_beads)
    if samples is None:
        error(f"Failed to read IDAT files from {idat_dir}")
        raise ValueError(f"Unable to read IDAT data from {idat_dir}")

    info(f"Running pOOBAH detection and masking (p-value threshold={pvalue_threshold})")
    samples.poobah(threshold=pvalue_threshold)
    samples.mask_quality_probes()
    samples.mask_control_probes()
    samples.mask_non_cg_probes()
    samples.mask_snp_probes()
    samples.mask_xy_probes()

    info("Applying dye-bias correction and NOOB background correction")
    samples.dye_bias_correction_nl()
    samples.noob_background_correction()

    info("Calculating beta and M values")
    samples.calculate_betas()
    beta_df = samples.get_betas(drop_na=True, apply_mask=True)
    m_df = samples.get_m_values(drop_na=True, apply_mask=True)

    if beta_df is None or m_df is None:
        error("Beta or M calculation failed")
        raise RuntimeError("Beta or M calculation returned None")

    beta_df = beta_df.reset_index()
    m_df = m_df.reset_index()

    if "probe_id" not in beta_df.columns or "probe_id" not in m_df.columns:
        error("Beta or M DataFrame missing probe_id column after reset_index")
        raise RuntimeError("probe_id not found in Beta/M output")

    probe_cols = {"type", "channel", "probe_type"}
    beta_df = beta_df[[col for col in beta_df.columns if col == "probe_id" or col not in probe_cols]]
    m_df = m_df[[col for col in m_df.columns if col == "probe_id" or col not in probe_cols]]

    value_vars = [col for col in beta_df.columns if col != "probe_id"]
    beta_long = beta_df.melt(
        id_vars=["probe_id"],
        value_vars=value_vars,
        var_name="sample_label",
        value_name="beta",
    )
    m_long = m_df.melt(
        id_vars=["probe_id"],
        value_vars=value_vars,
        var_name="sample_label",
        value_name="m_value",
    )

    merged = beta_long.merge(
        m_long,
        on=["probe_id", "sample_label"],
        how="left",
        validate="one_to_one",
    )

    info(f"Preprocessing produced {len(merged)} rows")
    return merged


@preprocessing
def data_preprocessing_function(
    df1: pd.DataFrame,
    idat_dir: str | None = None,
    pvalue_threshold: float = 0.05,
    min_beads: int = 1,
) -> Any:
    return preprocessing_impl(df1, idat_dir, pvalue_threshold, min_beads)
