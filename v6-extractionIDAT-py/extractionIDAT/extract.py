"""
Data extraction of IDAT files into Beta and M values.

The extraction reads the IDAT folder of a node (the database the node mounts for
this step), takes the cohort of each sample from the sample sheet in that folder,
runs the quality control and normalisation with pylluminator and returns a
long-format table with one row per (probe, sample):

    probe_id, sample_label, cohort, beta, m_value[, extra sample sheet columns]

The IDAT files are only available to the data extraction step, so everything that
needs them happens here. Later steps (e.g. v6-betavalues-py, v6-dmp-dmr-py) only use
the table stored in the session.
"""
from __future__ import annotations
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from pylluminator.samples import read_samples
from vantage6.algorithm.decorator.action import data_extraction
from vantage6.algorithm.tools.util import info, warn, error

IDAT_SUFFIX = ".idat"
SAMPLE_SHEET_NAMES = ("samplesheet.csv", "sample_sheet.csv")
OUTPUT_COLUMNS = ["probe_id", "sample_label", "cohort", "beta", "m_value"]
# pylluminator index levels that describe the probe design, not the probe itself
PROBE_DESIGN_COLUMNS = {"type", "channel", "probe_type"}


def _find_idat_files(directory: Path) -> list[Path]:
    return [path for path in directory.rglob("*") if path.is_file() and path.suffix.lower() == IDAT_SUFFIX]


def _sample_labels_from_idat_paths(idat_files: list[Path]) -> list[str]:
    labels = set()
    for path in idat_files:
        stem = path.stem
        label = stem.replace("_Grn", "").replace("_Red", "").replace("_grn", "").replace("_red", "")
        labels.add(label)
    return sorted(labels)


def _normalise_column_name(name: str) -> str:
    """Lower-case and snake_case a column name, e.g. 'Sample_ID' -> 'sample_id'."""
    return re.sub(r"[^0-9a-z]+", "_", str(name).strip().lower()).strip("_")


def _find_sample_sheet(directory: Path, sample_sheet_name: str | None) -> Path | None:
    if sample_sheet_name:
        path = directory / sample_sheet_name
        return path if path.is_file() else None
    files = {path.name.lower(): path for path in directory.iterdir() if path.is_file()}
    for name in SAMPLE_SHEET_NAMES:
        if name in files:
            return files[name]
    return None


def _read_sample_sheet(path: Path) -> pd.DataFrame:
    """Read a sample sheet, including Illumina sheets with a [Data] section."""
    df = pd.read_csv(path, dtype=str)
    data_rows = df.index[df.iloc[:, 0].str.strip() == "[Data]"]
    if len(data_rows) == 1:
        df = pd.read_csv(path, dtype=str, skiprows=data_rows[0] + 2)
    elif len(data_rows) > 1:
        raise ValueError(f"Sample sheet {path.name} contains several [Data] sections")
    df.columns = [_normalise_column_name(col) for col in df.columns]
    return df.apply(lambda col: col.str.strip())


def _sheet_rows_by_sample_label(sheet: pd.DataFrame, cohort_column: str) -> dict[str, pd.Series]:
    """Map each possible IDAT sample label in the sheet to its sample sheet row.

    A sample is identified by its sample_id and, if present, by
    '<sentrix_id>_<sentrix_position>' (the default IDAT file name).
    """
    mapping: dict[str, pd.Series] = {}
    for _, row in sheet.iterrows():
        keys = [row["sample_id"]]
        if {"sentrix_id", "sentrix_position"} <= set(sheet.columns):
            keys.append(f"{row['sentrix_id']}_{row['sentrix_position']}")
        for key in keys:
            if pd.isna(key) or key in ("", "nan_nan"):
                continue
            if key in mapping and mapping[key][cohort_column] != row[cohort_column]:
                raise ValueError("The sample sheet assigns a sample to more than one cohort")
            mapping.setdefault(key, row)
    return mapping


def _as_numeric_if_possible(values: pd.Series) -> pd.Series:
    """Convert a sample sheet column to numbers if every non-empty value is one.

    Empty values and 'NA' become missing values; a column with any text stays text.
    """
    cleaned = values.replace({"": None, "NA": None, "NaN": None, "nan": None})
    numeric = pd.to_numeric(cleaned, errors="coerce")
    if numeric.notna().sum() == cleaned.notna().sum():
        return numeric
    return cleaned


def resolve_idat_folder(connection_details: dict, idat_dir: str | None = None) -> Path:
    """Return the IDAT folder: `idat_dir` if given (local tests), else the database URI."""
    location = idat_dir or connection_details.get("uri")
    if not location:
        error("No IDAT folder given: the node passed no database URI")
        raise ValueError(
            "No IDAT folder given. Run this extraction on a node database of type "
            "'folder' that holds the IDAT files and the sample sheet."
        )
    folder = Path(location)
    if folder.is_file():
        folder = folder.parent
    if not folder.is_dir():
        error(f"IDAT folder {folder} does not exist")
        raise ValueError(f"IDAT folder {folder} does not exist or is not a folder")
    return folder


def sample_metadata(
    idat_dir: Path,
    cohort_column: str = "cohort",
    sample_sheet_name: str | None = None,
    sample_columns: list[str] | None = None,
) -> pd.DataFrame:
    """Find the IDAT samples in a folder and their cohort in the sample sheet.

    Returns one row per sample with `sample_label`, `cohort` and one column per
    entry in `sample_columns` (named in snake_case, e.g. 'Age' -> 'age').
    Raises if the folder has no IDATs, the sheet or a requested column is missing,
    or a sample is not in the sheet or has no cohort.
    """
    info(f"Searching for IDAT files in {idat_dir}")
    idat_files = _find_idat_files(idat_dir)
    if not idat_files:
        error(f"No IDAT files found in {idat_dir}")
        raise ValueError(f"No IDAT files (*.idat) found in {idat_dir}")
    sample_labels = _sample_labels_from_idat_paths(idat_files)

    sheet_path = _find_sample_sheet(idat_dir, sample_sheet_name)
    if sheet_path is None:
        expected = sample_sheet_name or " or ".join(SAMPLE_SHEET_NAMES)
        error(f"No sample sheet ({expected}) found in {idat_dir}")
        raise ValueError(
            f"No sample sheet ({expected}) found in {idat_dir}. The sample sheet must "
            f"list every sample with its cohort."
        )

    sheet = _read_sample_sheet(sheet_path)
    cohort_key = _normalise_column_name(cohort_column)
    if cohort_key not in sheet.columns:
        error(f"Cohort column '{cohort_column}' not found in {sheet_path.name}")
        raise ValueError(
            f"Cohort column '{cohort_column}' not found in sample sheet {sheet_path.name}. "
            f"Available columns: {sorted(sheet.columns)}. Add a cohort column to the "
            "sample sheet or pass the correct name via the 'cohort_column' argument."
        )
    if "sample_id" not in sheet.columns:
        error(f"Column 'sample_id' not found in {sheet_path.name}")
        raise ValueError(f"Sample sheet {sheet_path.name} has no 'Sample_ID' column")

    extra_keys = [_normalise_column_name(col) for col in (sample_columns or [])]
    reserved = set(OUTPUT_COLUMNS) & set(extra_keys)
    if reserved:
        raise ValueError(
            f"sample_columns {sorted(reserved)} clash with the output columns {OUTPUT_COLUMNS}"
        )
    unknown = [col for col, key in zip(sample_columns or [], extra_keys) if key not in sheet.columns]
    if unknown:
        error(f"Sample sheet columns {unknown} not found in {sheet_path.name}")
        raise ValueError(
            f"sample_columns {unknown} not found in sample sheet {sheet_path.name}. "
            f"Available columns: {sorted(sheet.columns)}."
        )

    rows = _sheet_rows_by_sample_label(sheet, cohort_key)
    metadata = pd.DataFrame({"sample_label": sample_labels})
    for key, name in [(cohort_key, "cohort")] + [(key, key) for key in extra_keys]:
        metadata[name] = [rows[label][key] if label in rows else None for label in sample_labels]

    n_missing = int(metadata["cohort"].isna().sum() + (metadata["cohort"] == "").sum())
    if n_missing:
        error(f"{n_missing} IDAT sample(s) have no cohort in {sheet_path.name}")
        raise ValueError(
            f"{n_missing} of {len(metadata)} IDAT sample(s) are missing from sample sheet "
            f"{sheet_path.name} or have an empty '{cohort_column}' value. Every sample "
            "needs a cohort."
        )
    for key in extra_keys:
        metadata[key] = _as_numeric_if_possible(metadata[key])
        n_empty = int(metadata[key].isna().sum())
        if n_empty:
            warn(f"Sample sheet column '{key}' is empty for {n_empty} sample(s); kept as missing")

    info(f"Discovered {len(metadata)} IDAT samples in {metadata['cohort'].nunique()} cohort(s)")
    return metadata


def _pylluminator_sample_sheet(sample_labels: list[str]) -> pd.DataFrame:
    """Build a pylluminator sample sheet that labels samples exactly as the extraction does."""
    sentrix = [label.split("_") if label.count("_") == 1 else ["", ""] for label in sample_labels]
    return pd.DataFrame({
        "sample_id": sample_labels,
        "sample_name": sample_labels,
        "sentrix_id": [parts[0] for parts in sentrix],
        "sentrix_position": [parts[1] for parts in sentrix],
    })


def methylation_values(
    idat_dir: Path,
    sample_labels: list[str],
    pvalue_threshold: float = 0.05,
    min_beads: int = 1,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read the IDATs with pylluminator and return (beta, m) tables (probes x samples).

    Steps performed:
    - pOOBAH detection p-values and masking (threshold: `pvalue_threshold`)
    - mask control, quality, non-CpG, SNP and sex-chromosome probes
    - non-linear dye-bias correction
    - NOOB background correction
    - Beta and M values of the probes that pass the masks
    """
    info(f"Loading {len(sample_labels)} IDAT samples from {idat_dir}")
    samples = read_samples(
        idat_dir,
        sample_sheet_df=_pylluminator_sample_sheet(sample_labels),
        min_beads=min_beads,
    )
    if samples is None:
        error(f"pylluminator could not read the IDAT files in {idat_dir}")
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

    tables = []
    for df in (beta_df, m_df):
        df = df.reset_index()
        if "probe_id" not in df.columns:
            error("Beta or M table has no probe_id after reset_index")
            raise RuntimeError("probe_id not found in Beta/M output")
        df = df.drop(columns=[col for col in df.columns if col in PROBE_DESIGN_COLUMNS])
        tables.append(df.set_index("probe_id"))
    beta, m_values = tables

    missing = sorted(set(sample_labels) - set(map(str, beta.columns)))
    if missing:
        error(f"pylluminator returned no values for {len(missing)} sample(s): {missing}")
        raise RuntimeError(
            f"pylluminator returned no Beta values for {len(missing)} of "
            f"{len(sample_labels)} sample(s): {missing}. Check these IDAT files."
        )
    return beta, m_values


def to_long_format(beta: pd.DataFrame, m_values: pd.DataFrame, metadata: pd.DataFrame) -> pd.DataFrame:
    """Combine probes x samples tables and per-sample metadata into the long table.

    M values are aligned to the Beta table (probes and samples), so every Beta value
    gets a row; its M value is missing if pylluminator dropped it. Values are stored as
    float32: about 930k EPIC v2 probes per sample make this table large.
    """
    labels = list(map(str, beta.columns))
    m_aligned = m_values.reindex(index=beta.index, columns=beta.columns)
    n_probes = len(beta.index)

    long = pd.DataFrame({
        "probe_id": np.tile(beta.index.to_numpy(dtype=object), len(labels)),
        "sample_label": np.repeat(np.array(labels, dtype=object), n_probes),
        "beta": beta.to_numpy(dtype=np.float32).ravel(order="F"),
        "m_value": m_aligned.to_numpy(dtype=np.float32).ravel(order="F"),
    })
    per_sample = metadata.set_index("sample_label")
    for column in per_sample.columns:
        long[column] = long["sample_label"].map(per_sample[column])
    extra = [col for col in per_sample.columns if col != "cohort"]
    return long[OUTPUT_COLUMNS + extra]


@data_extraction
def data_extraction_function(
    connection_details: dict,
    cohort_column: str = "cohort",
    sample_sheet_name: str | None = None,
    sample_columns: list[str] | None = None,
    pvalue_threshold: float = 0.05,
    min_beads: int = 1,
) -> Any:
    """Read the node's IDAT folder into a long table of Beta and M values.

    Parameters
    - connection_details: vantage6 provided connection details ('uri' is the IDAT folder)
    - cohort_column: column in the sample sheet that holds the cohort of each sample
    - sample_sheet_name: file name of the sample sheet in the IDAT folder. By default
      'samplesheet.csv' or 'sample_sheet.csv' is used.
    - sample_columns: other sample sheet columns to add to every row, e.g. ['Age']
    - pvalue_threshold: pOOBAH detection p-value threshold for masking probes
    - min_beads: minimum number of beads for a probe to be used
    """
    return extraction_impl(
        connection_details,
        cohort_column=cohort_column,
        sample_sheet_name=sample_sheet_name,
        sample_columns=sample_columns,
        pvalue_threshold=pvalue_threshold,
        min_beads=min_beads,
    )


def extraction_impl(
    connection_details: dict,
    idat_dir: str | None = None,
    cohort_column: str = "cohort",
    sample_sheet_name: str | None = None,
    sample_columns: list[str] | None = None,
    pvalue_threshold: float = 0.05,
    min_beads: int = 1,
) -> pd.DataFrame:
    """Undecorated implementation for local testing (`idat_dir` overrides the URI).

    Returns a DataFrame with `probe_id`, `sample_label`, `cohort`, `beta`, `m_value`
    and the requested `sample_columns`.
    """
    folder = resolve_idat_folder(connection_details, idat_dir)
    metadata = sample_metadata(folder, cohort_column, sample_sheet_name, sample_columns)
    beta, m_values = methylation_values(
        folder, metadata["sample_label"].tolist(), pvalue_threshold, min_beads
    )
    result = to_long_format(beta, m_values, metadata)
    info(
        f"Extraction produced {len(result):,} rows: {beta.shape[0]:,} probes x "
        f"{beta.shape[1]} samples ({result.memory_usage(deep=False).sum() / 1e6:,.0f} MB)"
    )
    return result
