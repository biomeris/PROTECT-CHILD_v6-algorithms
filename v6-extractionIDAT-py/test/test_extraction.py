"""
Run this script to test your extraction function locally (without building a Docker
image) using the mock client.

Run as:

    python test/test_extraction.py

Make sure to do so in an environment where `vantage6-algorithm-tools` is
installed. This can be done by running:

    pip install vantage6-algorithm-tools

Mock data: `Hospital A` and `Hospital B` each hold two samples (IDAT pairs) and a
sample sheet with a Cohort column:

    Hospital A: cohort_A (1 sample), cohort_B (1 sample)
    Hospital B: cohort_B (2 samples)
"""
import sys
import tempfile
from pathlib import Path

import pandas as pd

pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 40)
from vantage6.algorithm.mock.network import MockNetwork

current_path = Path(__file__).parent
sys.path.insert(0, str(current_path.parent))

from extractionIDAT.extract import OUTPUT_COLUMNS, extraction_impl  # noqa: E402

DATABASE_LABEL = "default"
HOSPITALS = ["Hospital A", "Hospital B"]
EXPECTED = {
    "Hospital A": {"6264509100_R01C01": "cohort_A", "6264509100_R01C02": "cohort_B"},
    "Hospital B": {"6264509100_R02C01": "cohort_B", "6264509100_R02C02": "cohort_B"},
}


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
    print(f"  PASS: {message}")


def expect_error(fn, text: str, message: str) -> None:
    try:
        fn()
    except ValueError as exc:
        check(text in str(exc), f"{message}: {exc}")
    else:
        raise AssertionError(f"{message}: no error raised")


# ---------------------------------------------------------------------------
# 1. Extraction through the mock network (decorated function)
# ---------------------------------------------------------------------------
print("\n[1] Extraction through the MockNetwork")
network = MockNetwork(
    datasets=[
        {DATABASE_LABEL: {"database": str(current_path / hospital), "db_type": "folder"}}
        for hospital in HOSPITALS
    ],
    module_name="extractionIDAT",
)
client = network.user_client
dataframe = client.dataframe.create(
    label=DATABASE_LABEL,
    method="data_extraction_function",
    arguments={},
    name=DATABASE_LABEL,
)
for node_id in network.node_ids:
    columns = [c["name"] for c in dataframe["columns"] if c["node_id"] == node_id]
    check(columns == OUTPUT_COLUMNS, f"node {node_id}: extracted dataframe columns are {OUTPUT_COLUMNS}")

# ---------------------------------------------------------------------------
# 2. Cohorts are read from the sample sheet
# ---------------------------------------------------------------------------
print("\n[2] Cohorts from the sample sheet")
for hospital in HOSPITALS:
    df = extraction_impl({}, idat_dir=str(current_path / hospital))
    print(f"\n{hospital}:\n{df}")
    check(
        dict(zip(df["sample_label"], df["cohort"])) == EXPECTED[hospital],
        f"{hospital}: every IDAT sample has the cohort from the sample sheet",
    )
    check((df["idat_dir"] == str(current_path / hospital)).all(), f"{hospital}: idat_dir points to the hospital folder")

# ---------------------------------------------------------------------------
# 3. Sample sheet variants and errors (temporary folders with empty IDAT files)
# ---------------------------------------------------------------------------
print("\n[3] Sample sheet variants and errors")


def make_folder(root: Path, name: str, labels: list[str], sheet_name: str | None, sheet: str | None) -> str:
    folder = root / name
    folder.mkdir()
    for label in labels:
        for channel in ("Grn", "Red"):
            (folder / f"{label}_{channel}.idat").touch()
    if sheet_name:
        (folder / sheet_name).write_text(sheet)
    return str(folder)


LABELS = ["200000000001_R01C01", "200000000001_R02C01"]

with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)

    folder = make_folder(
        root, "pylluminator_name", LABELS, "samplesheet.csv",
        "sample_id,cohort\n200000000001_R01C01,c1\n200000000001_R02C01,c2\n",
    )
    df = extraction_impl({}, idat_dir=folder)
    check(list(df["cohort"]) == ["c1", "c2"], "'samplesheet.csv' (pylluminator's file name) is read")

    folder = make_folder(
        root, "illumina", LABELS, "SampleSheet.csv",
        "[Header],,,,\nInvestigator Name,x,,,\n[Data],,,,\n"
        "Sample_ID,Sample_Name,Sentrix_ID,Sentrix_Position,Study Group\n"
        "LAB-001,P1,200000000001,R01C01,c1\nLAB-002,P2,200000000001,R02C01,c2\n",
    )
    df = extraction_impl({}, idat_dir=folder, cohort_column="Study Group", sample_sheet_name="SampleSheet.csv")
    check(
        list(df["sample_label"]) == LABELS and list(df["cohort"]) == ["c1", "c2"],
        "Illumina sheet with [Data] section, custom cohort column and Sentrix matching",
    )

    folder = make_folder(root, "no_sheet", LABELS, None, None)
    expect_error(lambda: extraction_impl({}, idat_dir=folder), "No sample sheet", "missing sample sheet raises")

    folder = make_folder(
        root, "no_cohort", LABELS, "sample_sheet.csv",
        "Sample_ID\n200000000001_R01C01\n200000000001_R02C01\n",
    )
    expect_error(lambda: extraction_impl({}, idat_dir=folder), "Cohort column 'cohort' not found", "missing cohort column raises")

    folder = make_folder(
        root, "unlisted_sample", LABELS, "sample_sheet.csv",
        "Sample_ID,Cohort\n200000000001_R01C01,c1\n",
    )
    expect_error(lambda: extraction_impl({}, idat_dir=folder), "1 of 2 IDAT sample(s)", "IDAT sample missing from the sheet raises")

    folder = make_folder(
        root, "empty_cohort", LABELS, "sample_sheet.csv",
        "Sample_ID,Cohort\n200000000001_R01C01,c1\n200000000001_R02C01,\n",
    )
    expect_error(lambda: extraction_impl({}, idat_dir=folder), "1 of 2 IDAT sample(s)", "empty cohort value raises")

print("\nAll checks passed.")
