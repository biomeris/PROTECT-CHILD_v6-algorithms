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
import contextlib
import io
import json
import re
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
# 2b. QC summary: counts from the Beta table, samples flagged (not removed)
# ---------------------------------------------------------------------------
print("\n[2b] QC summary")
from extractionIDAT.extract import quality_summary  # noqa: E402
toy = pd.DataFrame({
    "s1": [0.1, np.nan, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.2],
    "s2": [0.1, np.nan, np.nan, np.nan, 0.5, 0.6, 0.7, 0.8, 0.9, 0.2],
}, index=[f"cg{i}" for i in range(10)])
qc = quality_summary(toy, flag_fraction=0.10)
check(
    qc["n_probes"] == 10 and qc["masked_all"] == 1 and qc["dropped"] == 2 and qc["kept"] == 7,
    "QC counts: 1 probe masked in all samples, 2 failed in one sample (left out), 7 kept",
)
check(
    qc["failed_fraction"] == {"s1": 0.0, "s2": 2 / 9} and list(qc["flagged"]) == ["s2"],
    "failed share per sample excludes probes masked everywhere; above 10% is flagged",
)
check(quality_summary(toy, flag_fraction=0.5)["flagged"] == {}, "the flag threshold is respected")

with contextlib.redirect_stdout(io.StringIO()) as log:
    qc_run = extraction_impl({}, idat_dir=str(current_path / "Hospital A"), qc_flag_fraction=0.0)
log = log.getvalue()
n_samples = qc_run["sample_label"].nunique()
kept = int(re.search(r"([\d,]+) kept", log).group(1).replace(",", ""))
values = ["probe_id", "sample_label", "cohort", "beta", "m_value", "qc_failed_fraction"]
check(kept * n_samples == len(qc_run) and qc_run[values].equals(extracted["Hospital A"][values]),
      "the QC summary matches the extracted table, which is unchanged by the summary")
check(f"QC flag: {n_samples} of {n_samples} samples" in log and qc_run["qc_flagged"].all(),
      "every sample above the threshold is flagged (count in the log, qc_flagged column), and all are kept")
check(not any(label in log for label in qc_run["sample_label"].unique()),
      "the log never names a sample (vantage6 sends node logs to HQ)")
per_sample = extracted["Hospital A"].drop_duplicates("sample_label")
check(per_sample["qc_failed_fraction"].between(0, 1).all()
      and (per_sample["qc_flagged"] == (per_sample["qc_failed_fraction"] > 0.10)).all()
      and extracted["Hospital A"].groupby("sample_label")["qc_flagged"].nunique().eq(1).all(),
      "qc_failed_fraction in [0, 1] and qc_flagged = above 10%, the same on every row of a sample")

# Type I channel inference: optional; logs the switched probes and keeps valid values
with contextlib.redirect_stdout(io.StringIO()) as log:
    inferred = extraction_impl({}, idat_dir=str(current_path / "Hospital A"), infer_type1_channel=True)
log = log.getvalue()
switched = int(re.search(r"([\d,]+) probe\(s\) switched", log).group(1).replace(",", ""))
common = inferred.merge(extracted["Hospital A"], on=["probe_id", "sample_label"], suffixes=("", "_default"))
check(switched > 0 and inferred["beta"].between(0, 1).all() and list(inferred.columns) == OUTPUT_COLUMNS,
      f"type I channel inference runs ({switched:,} probes switched) and gives valid Beta values")
check(not np.allclose(common["beta"], common["beta_default"]) and len(common) > 0.95 * len(inferred),
      "with channel inference the values change slightly; without it they are as before")

# ---------------------------------------------------------------------------
# 3. Extra sample sheet columns (e.g. age) are added to every row
# ---------------------------------------------------------------------------
print("\n[2c] Sample QC summary and exclusion of flagged samples")
from extractionIDAT.qc import NEEDS_NEW_EXTRACTION, exclude_flagged  # noqa: E402


def qc_run_summary(flag_fraction, min_samples=5):
    """Extract with a flag threshold through the MockNetwork and run the QC summary."""
    net = MockNetwork(
        datasets=[{DATABASE_LABEL: {"database": str(current_path / h), "db_type": "folder"}} for h in HOSPITALS],
        module_name="extractionIDAT",
    )
    c = net.user_client
    with contextlib.redirect_stdout(io.StringIO()):
        frame = c.dataframe.create(label=DATABASE_LABEL, method="data_extraction_function",
                                   arguments={"qc_flag_fraction": flag_fraction}, name=DATABASE_LABEL)
        task = c.task.create(method="qc_summary", arguments={"min_samples": min_samples},
                             organizations=[net.organization_ids[0]],
                             databases=[{"type": "dataframe", "dataframe_id": frame["id"]}])
        return c.wait_for_results(task["id"])[0]


expected_counts = {}
for h, hospital in enumerate(HOSPITALS):
    cohorts = extracted[hospital].drop_duplicates("sample_label")["cohort"].astype(str).value_counts()
    expected_counts[h] = cohorts.to_dict()
none_flagged = qc_run_summary(0.99)
check(none_flagged["n_flagged"] == 0 and none_flagged["n_samples"] == sum(sum(c.values()) for c in expected_counts.values())
      and all(r["n_after_exclusion"] == r["n_samples"] for r in none_flagged["qc_summary"]),
      "QC summary: counts per hospital and cohort; nothing flagged at a 99% threshold")
all_flagged = qc_run_summary(0.0, min_samples=1)
above_zero = sum(int((f.drop_duplicates("sample_label")["qc_failed_fraction"] > 0).sum()) for f in extracted.values())
check(all_flagged["n_flagged"] == above_zero
      and all(r["below_minimum_after_exclusion"] == (r["n_after_exclusion"] < 1) for r in all_flagged["qc_summary"])
      and "would bring" in all_flagged["summary"],
      f"QC summary at a 0% threshold: {above_zero} samples with any failed probe flagged; cohorts left "
      "with no sample are marked below the minimum, and the summary says so")
check(not any(label in json.dumps(all_flagged) for frame in extracted.values() for label in frame["sample_label"].unique()),
      "the QC summary contains no sample IDs")

frame = extracted["Hospital A"].copy()
one = frame["sample_label"].unique()[0]
frame["qc_flagged"] = frame["sample_label"] == one
with contextlib.redirect_stdout(io.StringIO()):
    kept = exclude_flagged(frame)
check(one not in set(kept["sample_label"]) and kept["sample_label"].nunique() == frame["sample_label"].nunique() - 1
      and kept.equals(frame[frame["sample_label"] != one].reset_index(drop=True)),
      "exclusion removes exactly the flagged sample's rows and keeps the rest unchanged")
expect_error(lambda: exclude_flagged(frame.drop(columns=["qc_flagged"])), NEEDS_NEW_EXTRACTION[:40],
             "exclusion on a dataframe from an older extraction explains that it must be extracted again")

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

    automatic = extraction_impl({}, idat_dir=str(folder))
    check(list(automatic.columns) == OUTPUT_COLUMNS + ["age"]
          and automatic.groupby("sample_label")["age"].first().to_dict()
          == {"6264509100_R01C01": 54, "6264509100_R01C02": 61},
          "an 'Age' column in the sample sheet is added automatically, without requesting it")
    only_site = extraction_impl({}, idat_dir=str(folder), sample_columns=["Site"])
    check(list(only_site.columns) == OUTPUT_COLUMNS + ["site", "age"],
          "requested columns first, then the automatic age")
    (folder / "sample_sheet.csv").write_text(
        "Sample_ID,Cohort,AGE\n6264509100_R01C01,cohort_A,54\n6264509100_R01C02,cohort_B,61\n")
    check("age" in extraction_impl({}, idat_dir=str(folder)).columns, "any capitalisation of Age is found")
check(list(result.columns).count("age") == 1, "a requested Age is not added a second time")

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


print("\n[OMOP] Samples of an OMOP cohort (linked through SPECIMEN)")
import extractionIDAT.extract as extract_module  # noqa: E402
from extractionIDAT import omop_link  # noqa: E402

settings = {"OMOP_URI": "jdbc:postgresql://omop-postgres.omop.svc.cluster.local:5433",
            "OMOP_DATABASE": "epi_node1", "OMOP_USER": "reader", "OMOP_PASSWORD": "secret",
            "OMOP_CDM_SCHEMA": "cdm", "OMOP_RESULTS_SCHEMA": "results"}
parsed = omop_link.omop_settings(settings)
check(parsed["host"] == "omop-postgres.omop.svc.cluster.local" and parsed["port"] == 5433
      and parsed["dbname"] == "epi_node1", "the JDBC address gives host, port and database")
expect_error(lambda: omop_link.omop_settings({"uri": "/data/idat"}), "omop_uri",
             "without the OMOP settings the error says what the node administrator must add")


class _FakeCursor:
    executed = []
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, query, params): _FakeCursor.executed.append((query, params))
    def fetchall(self): return [("6264509100_R01C02",), ("not_here",)]


class _FakeConnection:
    def __init__(self, **kwargs): self.kwargs = kwargs
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def cursor(self): return _FakeCursor()


labels = omop_link.cohort_sample_labels(settings, 7, connect=_FakeConnection)
query, params = _FakeCursor.executed[-1]
text = query.as_string(None) if hasattr(query, "as_string") else str(query)
check(labels == {"6264509100_R01C02", "not_here"} and params == (7,)
      and '"results".cohort' in text and '"cdm".specimen' in text and "specimen_source_id" in text,
      "the cohort's samples come from results.cohort joined to cdm.specimen (cohort id as a parameter)")

original = extract_module.cohort_sample_labels
extract_module.cohort_sample_labels = lambda details, cohort_id: {"6264509100_R01C02", "elsewhere_1"}
try:
    out, log = None, io.StringIO()
    with contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
        out = extraction_impl({}, idat_dir=str(current_path / "Hospital A"), omop_cohort_id=7)
    check(sorted(out["sample_label"].unique()) == ["6264509100_R01C02"],
          "omop_cohort_id: only the cohort's sample is extracted")
    # QC removes probes failing in any extracted sample, so compare the shared probes
    both = out.merge(extracted["Hospital A"].query("sample_label == '6264509100_R01C02'"),
                     on=["probe_id", "sample_label"], suffixes=("", "_all"))
    check(len(both) > 0.9 * len(out) and np.allclose(both["beta"].astype(float), both["beta_all"].astype(float),
                                                     equal_nan=True, atol=1e-6),
          f"its values equal those of the full extraction on the shared probes ({len(both):,})")
    check("1 of 2 samples" in log.getvalue() and "1 of its samples have no IDAT" in log.getvalue()
          and "elsewhere_1" not in log.getvalue(), "the log has counts only, no sample labels")
    extract_module.cohort_sample_labels = lambda details, cohort_id: set()
    expect_error(lambda: extraction_impl({}, idat_dir=str(current_path / "Hospital A"), omop_cohort_id=8),
                 "No sample at this hospital", "a cohort without samples here fails with the reason")
    extract_module.cohort_sample_labels = lambda details, cohort_id: (_ for _ in ()).throw(AssertionError("called"))
    zero = extraction_impl({}, idat_dir=str(current_path / "Hospital A"), omop_cohort_id=0)
    check(zero["sample_label"].nunique() == 2, "omop_cohort_id 0 (an empty field) extracts every sample, without OMOP")
finally:
    extract_module.cohort_sample_labels = original

print("\nAll checks passed.")
