"""
Checks for running the SVM on vantage6 nodes with EPIC v2 data extracted by
v6-extractionIDAT-py (see also test_compute.py, which validates the method).

Run as (from the algorithm folder):

    python test/test_sandbox.py

[1] long input (one row per sample and probe) gives the same result as wide
[2] the batched leave-one-hospital-out models equal a separate discovery on
    the training hospitals only
[3] a hospital lacking some of the CpGs is left out and reported in `skipped`
[4] only what a real vantage6 node accepts
[5] the summary: 95% interval of the balanced accuracy, neutral wording and
    the caveat on how the CpGs were selected
"""
import ast
import contextlib
import importlib
import inspect
import io
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from vantage6.algorithm.client import AlgorithmClient
from vantage6.algorithm.mock.network import MockNetwork

ROOT = Path(__file__).parent.parent
DATA = ROOT / "test" / "data"
MODULE = "federated_svm"
CPGS = (DATA / "cpgs.txt").read_text().split()
CLASSES = ["no_rejection", "late_rejection"]
WIDE = [pd.read_csv(f) for f in sorted(DATA.glob("hospital_*.csv"))]

failures = []


def check(condition, message):
    print(("  PASS " if condition else "  FAIL ") + message)
    if not condition:
        failures.append(message)


def to_long(frame):
    """Wide -> long, as v6-extractionIDAT-py stores it."""
    return frame.melt(id_vars=["sample_id", "cohort"], value_vars=CPGS, var_name="probe_id",
                      value_name="beta").rename(columns={"sample_id": "sample_label"})


def run(frames, **arguments):
    network = MockNetwork(module_name=MODULE,
                          datasets=[{"db": {"database": f, "db_type": "csv"}} for f in frames])
    client = network.user_client
    with contextlib.redirect_stdout(io.StringIO()):
        task = client.task.create(
            method="central",
            organizations=[network.organization_ids[0]],
            arguments={"features": CPGS, "outcome_column": "cohort", "classes": CLASSES,
                       **arguments},
            databases=[{"type": "dataframe", "dataframe_id": client.dataframe.list()[0]["id"]}],
        )
        result = client.result.from_task(task["id"])[0]
    json.dumps(result)
    return result


print("\n[1] Long input")
wide = run(WIDE)
long = run([to_long(f) for f in WIDE])
check(json.dumps(long, sort_keys=True) == json.dumps(wide, sort_keys=True),
      "long input (probe_id, sample_label, beta) gives exactly the wide result")
check(wide["skipped"] == [], "no hospital skipped")

print("\n[2] Leave-one-hospital-out")
for held_out, entry in zip(range(len(WIDE)), wide["validation"]["loho"]["results"]):
    alone = run([f for i, f in enumerate(WIDE) if i != held_out], run_loho=False)
    check(entry["status"] == "success" and entry["selected_C"] == alone["model"]["C"],
          f"held-out hospital {held_out + 1}: same C as a discovery on the other hospitals only")
    check(np.isclose(entry["training_cv_balanced_accuracy"],
                     alone["validation"]["cv"]["metrics"]["balanced_accuracy"]),
          f"held-out hospital {held_out + 1}: same cross-validated balanced accuracy")

print("\n[3] CpGs missing at one hospital")
frames = [to_long(f) for f in WIDE]
frames[2] = frames[2][~frames[2]["probe_id"].isin(CPGS[:2])]  # QC removed 2 CpGs at hospital 3
partial = run(frames, run_loho=False)
check([s["organization_id"] for s in partial["skipped"] if s["step"] == "partial_cohorts"] == [3]
      and "2 of the" in partial["skipped"][0]["reason"],
      "hospital 3 is reported in `skipped`, with the number of missing CpGs")
check("error" not in partial, "the other hospitals still run the analysis")
complete = [f[CPGS + ["cohort"]].dropna() for f in WIDE[:2]]
check(partial["n_total"] == sum(f["cohort"].isin(CLASSES).sum() for f in complete),
      "only the (complete) samples of hospitals 1 and 2 are used")

print("\n[4] Real vantage6 node")
real_params = set(inspect.signature(AlgorithmClient.Task.create).parameters) - {"self"}
tree = ast.parse((ROOT / MODULE / "central.py").read_text())
used = {kw.arg for node in ast.walk(tree) if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute) and node.func.attr == "create"
        and ast.unparse(node.func.value) == "client.task" for kw in node.keywords}
check(used <= real_params, f"client.task.create only uses arguments of the real client {sorted(used)}")
pkg_name = re.search(r'ARG PKG_NAME="([^"]+)"', (ROOT / "Dockerfile").read_text()).group(1)
module = importlib.import_module(pkg_name)
check(all(hasattr(module, f) for f in ("central", "partial_cohorts", "partial_summary",
                                       "partial_svm", "partial_evaluate")),
      f"the Dockerfile's PKG_NAME ('{pkg_name}') is the importable module")
store = json.loads((ROOT / "algorithm_store.json").read_text())
for function in store["functions"]:
    code = set(inspect.signature(getattr(module, function["name"])).parameters)
    names = {a["name"] for a in function["arguments"]}
    check(names <= code, f"algorithm_store.json: {function['name']} arguments exist in the code")

print("\n[5] Summary wording and interval")
from scipy.stats import beta  # noqa: E402
central_module = sys.modules[f"{MODULE}.central"]
check(np.allclose(central_module._balanced_accuracy_interval([14], [15]),
                  beta.ppf([0.025, 0.975], 15, 2), atol=2e-3),
      "one class: the interval is the Beta(correct + 1, wrong + 1) 95% interval")
low, high = central_module._balanced_accuracy_interval([5, 6], [10, 12])
check(low < 0.5 < high, "chance-level counts give an interval that includes 0.5")
cv = wide["validation"]["cv"]["metrics"]
check(cv["balanced_accuracy_interval"][0] <= cv["balanced_accuracy"] + 1e-9
      and cv["balanced_accuracy_interval"][1] <= 1.0, "the CV metrics carry their interval")
text = " ".join(wide["evidence_summary"])
check("discovered" not in text.lower() and "The SVM separated" in text and "(95% CI " in text,
      "neutral wording: 'separated ... (95% CI ...)', never 'discovered'")
check("was not specified" in text, "default: says that how the CpGs were selected is unknown")
same = " ".join(run(WIDE, run_loho=False, features_selected_on_same_samples=True)["evidence_summary"])
check("selected on these same samples" in same and "optimistic" in same,
      "features_selected_on_same_samples=True: the accuracies are called optimistic")
independent = run(WIDE, run_loho=False, features_selected_on_same_samples=False)
check("selected independently" in " ".join(independent["evidence_summary"])
      and independent["validation"]["features_selected_on_same_samples"] is False,
      "features_selected_on_same_samples=False: stated, and kept in the output")

tables = wide["tables"]
check(len(tables["interpretation"]) == len(wide["comparisons"])
      and tables["performance"][0]["cv_balanced_accuracy"] == wide["comparisons"][0]["cv_metrics"]["balanced_accuracy"]
      and len(tables["cpgs"]) == len(CPGS) * len(wide["comparisons"])
      and all(not isinstance(v, (dict, list)) for row in tables["cpgs"] + tables["performance"] for v in row.values()),
      "flat tables (interpretation, performance, cpgs) repeat the results with one value per cell")
binary_text = " ".join(wide["evidence_summary"])
check(f"C = {wide['model']['C']:g}, selected by cross-validation from 0.001-100" in binary_text,
      "the summary states the selected C and the grid it came from")
check("Contributing towards" in binary_text and "Higher methylation in" not in binary_text,
      "two cohorts: key CpGs grouped by the cohort they push towards")
ovr = run(WIDE, classes=None, run_loho=False)
ovr_text = " ".join(ovr["evidence_summary"])
check("Higher methylation in" in ovr_text and "Lower methylation in" in ovr_text
      and "Contributing towards" not in ovr_text,
      "one-vs-rest: key CpGs grouped by higher / lower methylation in the target cohort")

print(f"\n{'All checks passed' if not failures else f'{len(failures)} check(s) failed'}")
sys.exit(1 if failures else 0)
