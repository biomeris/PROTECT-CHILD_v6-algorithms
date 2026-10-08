"""
Run this script to test your preprocessing function locally (without building a
Docker image).

Run as:

    python test/test_preprocessing.py

Make sure to do so in an environment where `vantage6-algorithm-tools` and
`pylluminator` are installed. This can be done by running:

    pip install vantage6-algorithm-tools pylluminator

The first check runs the real pipeline: the extraction from the sibling module
`v6-extractionIDAT-py` reads the IDAT folders and sample sheets (with cohorts) in
that module's `test/Hospital A` and `test/Hospital B`, and its output is passed to
the preprocessing. The cohort of every sample must survive the preprocessing.
"""
import contextlib
import io
import sys
from pathlib import Path

import pandas as pd

current_path = Path(__file__).parent
extraction_module = current_path.parent.parent / "v6-extractionIDAT-py"

# Add the parent directories to the path so we can import both packages
sys.path.insert(0, str(current_path.parent))
sys.path.insert(0, str(extraction_module))

from preprocessIDAT.preprocess import preprocessing_impl  # noqa: E402
from extractionIDAT.extract import extraction_impl  # noqa: E402

OUTPUT_COLUMNS = ["probe_id", "sample_label", "cohort", "beta", "m_value"]


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
    print(f"  PASS: {message}")


def run(fn, *args, **kwargs):
    """Call fn with stdout captured; return (result, captured output)."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        result = fn(*args, **kwargs)
    return result, buffer.getvalue()


# ---------------------------------------------------------------------------
# 1. Extraction -> preprocessing keeps the cohort of every sample
# ---------------------------------------------------------------------------
print("\n[1] Extraction output -> preprocessing")
for hospital in ["Hospital A", "Hospital B"]:
    folder = extraction_module / "test" / hospital
    extracted, _ = run(extraction_impl, {}, idat_dir=str(folder))
    print(f"\n{hospital} extraction output:\n{extracted[['sample_label', 'cohort']]}")

    # No idat_dir argument: the folder must come from the extraction output
    result, log = run(preprocessing_impl, extracted)
    print(f"preprocessed shape: {result.shape}")
    print(result.head())

    check(f"Loading IDAT samples from {folder}" in log, f"{hospital}: IDAT folder taken from the extraction output")
    check(list(result.columns) == OUTPUT_COLUMNS, f"{hospital}: output columns are {OUTPUT_COLUMNS}")
    check(
        set(result["sample_label"]) == set(extracted["sample_label"]),
        f"{hospital}: every extracted sample is preprocessed, with the same label",
    )
    expected = dict(zip(extracted["sample_label"], extracted["cohort"]))
    check(
        (result["cohort"] == result["sample_label"].map(expected)).all(),
        f"{hospital}: every row has the cohort of its sample ({expected})",
    )
    check(result["beta"].between(0, 1).all(), f"{hospital}: beta values are within [0, 1]")

# ---------------------------------------------------------------------------
# 2. Without a cohort (old extraction output) the result has no cohort column
# ---------------------------------------------------------------------------
print("\n[2] Extraction output without cohort")
folder = current_path / "hospital_A"
result, log = run(preprocessing_impl, pd.DataFrame({"idat_dir": [str(folder)]}))
check("cohort" not in result.columns and not result.empty, "no cohort column, data still preprocessed")
check("cannot be used by cohort-aware algorithms" in log, "a warning says the result has no cohort")

print("\nAll checks passed.")
