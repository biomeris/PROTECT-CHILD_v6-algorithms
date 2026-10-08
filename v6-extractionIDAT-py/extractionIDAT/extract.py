"""
Data extraction functions for IDAT ingestion.

This extraction discovers IDAT files in the given folder (or the test/ folder),
reads the cohort of each sample from the sample sheet in that folder and returns a
small table with sample labels, cohorts and the path to the IDAT folder.
"""
from __future__ import annotations
import re
from pathlib import Path
from typing import Any

import pandas as pd
from vantage6.algorithm.decorator.action import data_extraction
from vantage6.algorithm.tools.util import info, warn, error

DEFAULT_TEST_FOLDER = Path(__file__).resolve().parents[1] / "test"
IDAT_SUFFIX = ".idat"
SAMPLE_SHEET_NAMES = ("samplesheet.csv", "sample_sheet.csv")
OUTPUT_COLUMNS = ["sample_label", "cohort", "idat_dir"]


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


def _cohort_by_sample_label(sheet: pd.DataFrame, cohort_column: str) -> dict[str, str]:
    """Map each possible IDAT sample label in the sheet to its cohort.

    A sample is identified by its sample_id and, if present, by
    '<sentrix_id>_<sentrix_position>' (the default IDAT file name).
    """
    mapping: dict[str, str] = {}
    for _, row in sheet.iterrows():
        cohort = row[cohort_column]
        keys = [row["sample_id"]]
        if {"sentrix_id", "sentrix_position"} <= set(sheet.columns):
            keys.append(f"{row['sentrix_id']}_{row['sentrix_position']}")
        for key in keys:
            if pd.isna(key) or key in ("", "nan_nan"):
                continue
            if key in mapping and mapping[key] != cohort:
                raise ValueError("The sample sheet assigns a sample to more than one cohort")
            mapping[key] = cohort
    return mapping


@data_extraction
def data_extraction_function(
    connection_details: dict,
    idat_dir: str | None = None,
    cohort_column: str = "cohort",
    sample_sheet_name: str | None = None,
) -> Any:
    """Locate IDAT files and return per-sample metadata, including the cohort.

    Parameters
    - connection_details: vantage6 provided connection details (may include 'uri')
    - idat_dir: optional override path to IDAT folder (for testing)
    - cohort_column: column in the sample sheet that holds the cohort of each sample
    - sample_sheet_name: file name of the sample sheet in the IDAT folder. By default
      'samplesheet.csv' or 'sample_sheet.csv' is used.
    """
    return extraction_impl(connection_details, idat_dir, cohort_column, sample_sheet_name)


def extraction_impl(
    connection_details: dict,
    idat_dir: str | None = None,
    cohort_column: str = "cohort",
    sample_sheet_name: str | None = None,
) -> Any:
    """Undecorated implementation for local testing.

    Returns a DataFrame with `sample_label`, `cohort` and `idat_dir` columns.
    """
    target_path = Path(idat_dir) if idat_dir else Path(connection_details.get("uri", DEFAULT_TEST_FOLDER))
    if target_path.is_file():
        target_path = target_path.parent

    if not target_path.exists():
        warn(f"IDAT directory {target_path} not found, falling back to {DEFAULT_TEST_FOLDER}")
        target_path = DEFAULT_TEST_FOLDER

    if not target_path.exists():
        warn(f"Fallback IDAT directory {target_path} also does not exist.")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    info(f"Searching for IDAT files in {target_path}")
    idat_files = _find_idat_files(target_path)
    if not idat_files:
        warn(f"No IDAT files found in {target_path}")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    sample_labels = _sample_labels_from_idat_paths(idat_files)

    sheet_path = _find_sample_sheet(target_path, sample_sheet_name)
    if sheet_path is None:
        expected = sample_sheet_name or " or ".join(SAMPLE_SHEET_NAMES)
        error(f"No sample sheet ({expected}) found in {target_path}")
        raise ValueError(
            f"No sample sheet ({expected}) found in {target_path}. The sample sheet must "
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

    cohorts = _cohort_by_sample_label(sheet, cohort_key)
    df = pd.DataFrame({
        "sample_label": sample_labels,
        "cohort": [cohorts.get(label) for label in sample_labels],
        "idat_dir": str(target_path),
    })

    n_missing = int(df["cohort"].isna().sum() + (df["cohort"] == "").sum())
    if n_missing:
        error(f"{n_missing} IDAT sample(s) have no cohort in {sheet_path.name}")
        raise ValueError(
            f"{n_missing} of {len(df)} IDAT sample(s) are missing from sample sheet "
            f"{sheet_path.name} or have an empty '{cohort_column}' value. Every sample "
            "needs a cohort."
        )

    info(f"Discovered {len(df)} IDAT sample labels in {df['cohort'].nunique()} cohort(s)")
    return df[OUTPUT_COLUMNS]
