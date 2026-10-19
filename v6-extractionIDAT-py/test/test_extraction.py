"""
Run this script to test your extraction function locally (without building a Docker
image) using the mock client.

Run as:

    python test/test_extraction.py

Make sure to do so in an environment where `vantage6-algorithm-tools` and
`pylluminator` are installed. This can be done by running:

    pip install vantage6-algorithm-tools pylluminator

Mock data: each hospital folder holds IDAT pairs and a sample sheet with a Cohort
column:

    Hospital A: cohort_A (1 sample), cohort_B (1 sample)    450K
    Hospital B: cohort_B (2 samples)                        450K
    Hospital C: cohort_C (1 sample, GEO-style file name)     EPIC v2
"""
import shutil
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 40)
from vantage6.algorithm.mock.network import MockNetwork

current_path = Path(__file__).parent
sys.path.insert(0, str(current_path.parent))

from extractionIDAT.extract import OUTPUT_COLUMNS, extraction_impl, sample_metadata  # noqa: E402

DATABASE_LABEL = "default"
HOSPITALS = ["Hospital A", "Hospital B", "Hospital C"]
EXPECTED = {
    "Hospital A": {"6264509100_R01C01": "cohort_A", "6264509100_R01C02": "cohort_B"},
    "Hospital B": {"6264509100_R02C01": "cohort_B", "6264509100_R02C02": "cohort_B"},
    "Hospital C": {"GSM9870126_209147160068_R08C01": "cohort_C"},
}
# EPIC v2 probe IDs have a suffix such as cg00000769_BC11
EPIC_V2_SUFFIX = r"_[TB][CO]\d+$"


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
# 2. Beta and M values per hospital, with the cohort from the sample sheet
# ---------------------------------------------------------------------------
print("\n[2] Beta and M values with cohorts")
extracted = {}
for hospital in HOSPITALS:
    result = extraction_impl({}, idat_dir=str(current_path / hospital))
    print(f"\n{hospital}: {result.shape}\n{result.head()}")

    check(list(result.columns) == OUTPUT_COLUMNS, f"{hospital}: output columns are {OUTPUT_COLUMNS}")
    check(
        set(result["sample_label"]) == set(EXPECTED[hospital]),
        f"{hospital}: every IDAT sample is extracted, labelled as in the sample sheet",
    )
    check(
        (result["cohort"] == result["sample_label"].map(EXPECTED[hospital])).all(),
        f"{hospital}: every row has the cohort of its sample",
    )
    check(result["beta"].between(0, 1).all(), f"{hospital}: beta values are within [0, 1]")
    check(
        result["beta"].dtype == np.float32 and result["m_value"].dtype == np.float32,
        f"{hospital}: beta and m_value are float32",
    )
    check(not result.duplicated(["probe_id", "sample_label"]).any(), f"{hospital}: one row per (probe, sample)")
    extracted[hospital] = result

for hospital in ["Hospital A", "Hospital B"]:
    per_sample = extracted[hospital].groupby("sample_label").size()
    check(
        not extracted[hospital]["probe_id"].str.contains(EPIC_V2_SUFFIX).any() and per_sample.max() < 500_000,
        f"{hospital}: 450K data ({per_sample.min():,}-{per_sample.max():,} probes per sample, plain probe IDs)",
    )
epic = extracted["Hospital C"]
per_sample = epic.groupby("sample_label").size()
check(
    epic["probe_id"].str.contains(EPIC_V2_SUFFIX).all() and per_sample.min() > 800_000,
    f"Hospital C: EPIC v2 data ({per_sample.min():,} probes per sample, EPIC v2 probe IDs)",
)

# ---------------------------------------------------------------------------
# 3. Extra sample sheet columns (e.g. age) are added to every row
# ---------------------------------------------------------------------------
print("\n[3] Extra sample sheet columns")
with tempfile.TemporaryDirectory() as tmp:
    folder = Path(tmp) / "with_age"
    shutil.copytree(current_path / "Hospital A", folder)
    (folder / "sample_sheet.csv").write_text(
        "Sample_ID,Cohort,Age,Age at menopause,Site\n"
        "6264509100_R01C01,cohort_A,54,50.5,north\n"
        "6264509100_R01C02,cohort_B,61,NA,south\n"
    )
    result = extraction_impl(
        {}, idat_dir=str(folder), sample_columns=["Age", "Age at menopause", "Site"]
    )
    check(
        list(result.columns) == OUTPUT_COLUMNS + ["age", "age_at_menopause", "site"],
        "requested columns are added in snake_case after the standard columns",
    )
    ages = result.groupby("sample_label")["age"].unique().map(list).to_dict()
    check(
        ages == {"6264509100_R01C01": [54], "6264509100_R01C02": [61]},
        "every row has the age of its sample",
    )
    check(pd.api.types.is_numeric_dtype(result["age"]), "numeric sample sheet column becomes numeric")
    menopause = result.drop_duplicates("sample_label").set_index("sample_label")["age_at_menopause"]
    check(
        menopause["6264509100_R01C01"] == 50.5 and pd.isna(menopause["6264509100_R01C02"]),
        "'NA' becomes a missing value; the sample is kept",
    )
    check(result["site"].dtype == object, "text sample sheet column stays text")

# ---------------------------------------------------------------------------
# 4. Sample sheet variants and errors (temporary folders with empty IDAT files)
# ---------------------------------------------------------------------------
print("\n[4] Sample sheet variants and errors")


def make_folder(root: Path, name: str, labels: list[str], sheet_name: str | None, sheet: str | None) -> Path:
    folder = root / name
    folder.mkdir()
    for label in labels:
        for channel in ("Grn", "Red"):
            (folder / f"{label}_{channel}.idat").touch()
    if sheet_name:
        (folder / sheet_name).write_text(sheet)
    return folder


LABELS = ["200000000001_R01C01", "200000000001_R02C01"]

with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)

    folder = make_folder(
        root, "pylluminator_name", LABELS, "samplesheet.csv",
        "sample_id,cohort\n200000000001_R01C01,c1\n200000000001_R02C01,c2\n",
    )
    df = sample_metadata(folder)
    check(list(df["cohort"]) == ["c1", "c2"], "'samplesheet.csv' (pylluminator's file name) is read")

    folder = make_folder(
        root, "illumina", LABELS, "SampleSheet.csv",
        "[Header],,,,\nInvestigator Name,x,,,\n[Data],,,,\n"
        "Sample_ID,Sample_Name,Sentrix_ID,Sentrix_Position,Study Group\n"
        "LAB-001,P1,200000000001,R01C01,c1\nLAB-002,P2,200000000001,R02C01,c2\n",
    )
    df = sample_metadata(folder, cohort_column="Study Group", sample_sheet_name="SampleSheet.csv")
    check(
        list(df["sample_label"]) == LABELS and list(df["cohort"]) == ["c1", "c2"],
        "Illumina sheet with [Data] section, custom cohort column and Sentrix matching",
    )

    folder = make_folder(root, "no_sheet", LABELS, None, None)
    expect_error(lambda: sample_metadata(folder), "No sample sheet", "missing sample sheet raises")

    folder = make_folder(
        root, "no_cohort", LABELS, "sample_sheet.csv",
        "Sample_ID\n200000000001_R01C01\n200000000001_R02C01\n",
    )
    expect_error(lambda: sample_metadata(folder), "Cohort column 'cohort' not found", "missing cohort column raises")

    folder = make_folder(
        root, "unlisted_sample", LABELS, "sample_sheet.csv",
        "Sample_ID,Cohort\n200000000001_R01C01,c1\n",
    )
    expect_error(lambda: sample_metadata(folder), "1 of 2 IDAT sample(s)", "IDAT sample missing from the sheet raises")

    folder = make_folder(
        root, "empty_cohort", LABELS, "sample_sheet.csv",
        "Sample_ID,Cohort\n200000000001_R01C01,c1\n200000000001_R02C01,\n",
    )
    expect_error(lambda: sample_metadata(folder), "1 of 2 IDAT sample(s)", "empty cohort value raises")

    folder = make_folder(
        root, "extra_columns", LABELS, "sample_sheet.csv",
        "Sample_ID,Cohort,Age\n200000000001_R01C01,c1,50\n200000000001_R02C01,c2,51\n",
    )
    expect_error(
        lambda: sample_metadata(folder, sample_columns=["Weight"]),
        "sample_columns ['Weight'] not found",
        "unknown extra column raises",
    )
    expect_error(
        lambda: sample_metadata(folder, sample_columns=["Beta"]),
        "clash with the output columns",
        "extra column with an output column name raises",
    )

# ---------------------------------------------------------------------------
# 5. No silent fallback to other data
# ---------------------------------------------------------------------------
print("\n[5] Missing data raises instead of falling back")
expect_error(lambda: extraction_impl({}), "No IDAT folder given", "no database URI raises")
expect_error(
    lambda: extraction_impl({"uri": str(current_path / "does_not_exist")}),
    "does not exist",
    "missing IDAT folder raises",
)
with tempfile.TemporaryDirectory() as tmp:
    expect_error(lambda: extraction_impl({"uri": tmp}), "No IDAT files", "folder without IDATs raises")

print("\nAll checks passed.")
