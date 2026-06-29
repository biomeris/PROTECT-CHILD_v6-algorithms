"""
Data extraction functions for IDAT ingestion.

This extraction discovers IDAT files in the given folder (or the test/ folder)
and returns a small table with sample labels and the path to the IDAT folder.
"""
from __future__ import annotations
from pathlib import Path
from typing import Any

import pandas as pd
from vantage6.algorithm.decorator.action import data_extraction
from vantage6.algorithm.tools.util import info, warn

DEFAULT_TEST_FOLDER = Path(__file__).resolve().parents[1] / "test"
IDAT_SUFFIX = ".idat"


def _find_idat_files(directory: Path) -> list[Path]:
    return [path for path in directory.rglob("*") if path.is_file() and path.suffix.lower() == IDAT_SUFFIX]


def _sample_labels_from_idat_paths(idat_files: list[Path]) -> list[str]:
    labels = set()
    for path in idat_files:
        stem = path.stem
        label = stem.replace("_Grn", "").replace("_Red", "").replace("_grn", "").replace("_red", "")
        labels.add(label)
    return sorted(labels)


@data_extraction
def data_extraction_function(connection_details: dict, idat_dir: str | None = None) -> Any:
    """Locate IDAT files and return per-sample metadata for preprocessing.

    Parameters
    - connection_details: vantage6 provided connection details (may include 'uri')
    - idat_dir: optional override path to IDAT folder (for testing)
    """
    return extraction_impl(connection_details, idat_dir)


def extraction_impl(connection_details: dict, idat_dir: str | None = None) -> Any:
    """Undecorated implementation for local testing.

    Returns a DataFrame with `sample_label` and `idat_dir` columns.
    """
    target_path = Path(idat_dir) if idat_dir else Path(connection_details.get("uri", DEFAULT_TEST_FOLDER))
    if target_path.is_file():
        target_path = target_path.parent

    if not target_path.exists():
        warn(f"IDAT directory {target_path} not found, falling back to {DEFAULT_TEST_FOLDER}")
        target_path = DEFAULT_TEST_FOLDER

    if not target_path.exists():
        warn(f"Fallback IDAT directory {target_path} also does not exist.")
        return pd.DataFrame({"sample_label": [], "idat_dir": []})

    info(f"Searching for IDAT files in {target_path}")
    idat_files = _find_idat_files(target_path)
    if not idat_files:
        warn(f"No IDAT files found in {target_path}")
        return pd.DataFrame({"sample_label": [], "idat_dir": []})

    sample_labels = _sample_labels_from_idat_paths(idat_files)
    df = pd.DataFrame({
        "sample_label": sample_labels,
        "idat_dir": str(target_path),
    })
    info(f"Discovered {len(df)} IDAT sample labels")
    return df
