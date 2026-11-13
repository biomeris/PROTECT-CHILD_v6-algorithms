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
    names = {a["name"] for a in function["arguments"] if not a.get("is_frontend_only")}
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
blank = run(WIDE, classes=[], C_grid=[], run_loho=False)
detected = run(WIDE, classes=None, run_loho=False)
check("error" not in blank and blank["classes"] == detected["classes"]
      and blank["model_selection"]["best_C"] == detected["model_selection"]["best_C"],
      "classes=[] and C_grid=[] (blank fields in the web interface) act as not given")
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

print("\n[6] Hospitals to include, inputs from other tasks, applying a trained model")


def run_in(network, method, arguments):
    """Run `method` as a central task in an existing mock network."""
    client = network.user_client
    with contextlib.redirect_stdout(io.StringIO()):
        task = client.task.create(
            method=method,
            organizations=[network.organization_ids[0]],
            arguments=arguments,
            databases=[{"type": "dataframe", "dataframe_id": client.dataframe.list()[0]["id"]}],
        )
        result = client.result.from_task(task["id"])[0]
    json.dumps(result)
    return task["id"], result


network = MockNetwork(module_name=MODULE,
                      datasets=[{"db": {"database": f, "db_type": "csv"}} for f in WIDE])
orgs = network.organization_ids
common_args = {"outcome_column": "cohort", "classes": CLASSES}
_, full = run_in(network, "central", {**common_args, "features": CPGS})
check(full["input"]["source"] == "list" and full["organizations"] == orgs,
      "the result states its input (a given list) and the hospitals used")
train_id, trained = run_in(network, "central", {**common_args, "features": CPGS, "run_loho": False,
                                                "organizations_to_include": orgs[:2]})
check(trained["organizations"] == orgs[:2], "organizations_to_include trains on those hospitals only")
_, applied = run_in(network, "apply_model", {"outcome_column": "cohort", "model_task_id": train_id,
                                             "organizations_to_include": [orgs[2]]})
loho_3 = [r for r in full["validation"]["loho"]["results"] if r["held_out_hospital"] == orgs[2]][0]
check(applied["input"]["task_id"] == train_id
      and applied["per_hospital"][0]["confusion_matrix"] == loho_3["confusion_matrix"],
      "a model trained on hospitals 1-2 and applied to hospital 3 gives the LOHO result for hospital 3")
check(applied["tables"]["performance"] and applied["tables"]["performance"][-1]["hospitals"] == "pooled",
      "apply_model returns flat tables (per hospital and pooled)")
_, everywhere = run_in(network, "apply_model", {"outcome_column": "cohort", "model_task_id": train_id})
check(everywhere["pooled"]["n"] == sum(h["n"] for h in everywhere["per_hospital"] if h["status"] == "evaluated"),
      "pooled counts are the sum of the hospitals' counts")
model = {"features": trained["features"], "classes": trained["classes"],
         "weights": [c["model"]["weights"] for c in trained["comparisons"]],
         "biases": [c["model"]["bias"] for c in trained["comparisons"]],
         "mean": trained["model"]["standardization"]["mean"],
         "std": trained["model"]["standardization"]["std"]}
_, from_json = run_in(network, "apply_model", {"outcome_column": "cohort", "model": model,
                                               "organizations_to_include": [orgs[2]]})
check(from_json["per_hospital"] == applied["per_hospital"], "a model given as JSON gives the same result")
_, not_svm = run_in(network, "apply_model", {"outcome_column": "cohort", "model_task_id": train_id, "model": model})
check("error" in not_svm, "model and model_task_id together are rejected")

# A test hospital whose quality control removed some of the model's CpGs
check(everywhere["per_hospital"][0]["n_missing_cpgs"] == 0 and applied["tables"]["performance"][0]["missing_cpgs"] == 0,
      "with every CpG present, no CpG is reported missing")
lost = trained["features"][0]
partial = [WIDE[0], WIDE[1], WIDE[2].drop(columns=[lost])]
net_missing = MockNetwork(module_name=MODULE,
                          datasets=[{"db": {"database": f, "db_type": "csv"}} for f in partial])
_, missing_one = run_in(net_missing, "apply_model", {"outcome_column": "cohort", "model": model,
                                                     "organizations_to_include": [orgs[2]]})
h3 = missing_one["per_hospital"][0]
test_rows = WIDE[2][WIDE[2]["cohort"].isin(model["classes"])]
# complete samples over the CpGs still present (as the hospital does)
test_rows = test_rows.dropna(subset=[c for c in model["features"] if c != lost])
z = (test_rows[model["features"]].to_numpy(float) - np.asarray(model["mean"])) / np.asarray(model["std"])
z[:, model["features"].index(lost)] = 0.0  # the missing CpG at its training mean
score = z @ np.asarray(model["weights"][0]) + model["biases"][0]
predicted = np.where(score >= 0, model["classes"][1], model["classes"][0])
expected = [[int(((test_rows["cohort"] == t) & (predicted == q)).sum()) for q in model["classes"]]
            for t in model["classes"]]
check(h3["status"] == "evaluated" and h3["n_missing_cpgs"] == 1 and h3["missing_cpgs"] == [lost]
      and h3["confusion_matrix"] == expected,
      f"1 of {len(model['features'])} CpGs missing at the test hospital: applied with it at its training mean "
      "(equals a hand calculation) and reported")
check(missing_one["tables"]["performance"][0]["missing_cpgs"] == 1, "the Performance table shows the missing CpGs")
_, strict = run_in(net_missing, "apply_model", {"outcome_column": "cohort", "model": model,
                                                "organizations_to_include": [orgs[2]], "max_missing_fraction": 0})
check(strict["per_hospital"][0]["status"] == "not evaluated"
      and "more than the maximum of 0%" in strict["per_hospital"][0]["reason"]
      and lost in strict["per_hospital"][0]["reason"],
      "max_missing_fraction=0: not evaluated, with the hospital's own reason naming the missing CpG")
many = model["features"][: int(len(model["features"]) * 0.5)]
net_many = MockNetwork(module_name=MODULE, datasets=[{"db": {"database": f, "db_type": "csv"}}
                                                     for f in [WIDE[0], WIDE[1], WIDE[2].drop(columns=many)]])
_, too_many = run_in(net_many, "apply_model", {"outcome_column": "cohort", "model": model,
                                               "organizations_to_include": [orgs[2]]})
check(too_many["per_hospital"][0]["status"] == "not evaluated" and too_many["pooled"] is None
      and "more than the maximum of 20%" in too_many["per_hospital"][0]["reason"],
      f"{len(many)} of {len(model['features'])} CpGs missing (above the default 20%): not evaluated, reason given")

# Privacy warning when a hospital has at least as many CpGs as samples (a warning only)
central_module = sys.modules[f"{MODULE}.central"]
warn_text = central_module._privacy_warning(23, [6, 10, 30])
check(warn_text == "Warning: more CpGs than patients in 2 of 3 hospitals",
      "privacy warning: short message with how many hospitals")
check(central_module._privacy_warning(5, [6, 10]) is None, "no privacy warning when every hospital has more samples than CpGs")
# complete samples of the analysed cohorts, as the SVM counts them
sizes = [int(f[f["cohort"].isin(CLASSES)].dropna(subset=CPGS).shape[0]) for f in WIDE]
expected_warning = central_module._privacy_warning(len(CPGS), sizes)
check(full["input"].get("privacy_warning") == expected_warning
      and (expected_warning is None or expected_warning in full["evidence_summary"]),
      f"the SVM result shows the privacy warning in its Input table (hospital sizes {sizes}, {len(CPGS)} CpGs)")
few = run_in(network, "central", {**common_args, "features": CPGS[:3], "run_loho": False})[1]
check(few["input"].get("privacy_warning") is None, "3 CpGs: no privacy warning")

central_module = sys.modules[f"{MODULE}.central"]


class _Fake:
    def __init__(self, tasks):
        outer = self
        self._tasks = tasks

        class task:  # noqa: N801
            @staticmethod
            def get(task_id):
                t = outer._tasks.get(task_id)
                return {"id": task_id, "name": t[0]} if t else {"msg": f"Task id={task_id} does not exist!"}

            @staticmethod
            def create(**kwargs):
                return {"msg": "The dataframe you selected is not present for the following organizations: 2, 3."}

        class result:  # noqa: N801
            @staticmethod
            def from_task(task_id):
                t = outer._tasks.get(task_id)
                return [t[1]] if t else []

        class organization:  # noqa: N801
            @staticmethod
            def list():
                return [{"id": 1}, {"id": 2}, {"id": 3}]

        self.task, self.result, self.organization = task, result, organization


fake = _Fake({5: ("en", {"selected_cpgs": ["cgA", "cgB"], "cohorts": ["lipid", "highcardio"]}),
              6: ("dmp", {"global_dmps": []})})
feats, cohorts, prov = central_module._features_from_task(fake, 5)
check(feats == ["cgA", "cgB"] and cohorts == ["lipid", "highcardio"] and prov["task_id"] == 5,
      "features_task_id: the Elastic Net task's CpGs and cohorts")
try:
    central_module._features_from_task(fake, 6)
    check(False, "a task of another algorithm is rejected")
except ValueError as exc:
    check("not an Elastic Net result" in str(exc), "a task of another algorithm is rejected")
try:
    central_module._features_from_task(fake, 99)
    check(False, "an unknown task is rejected")
except ValueError as exc:
    check("could not be read" in str(exc), "an unknown task is rejected")
check(central_module._select_organizations(fake, []) == [1, 2, 3]
      and central_module._select_organizations(fake, [3]) == [3], "organizations_to_include: empty = all")
try:
    central_module._run_task(fake, "partial_cohorts", [1, 2, 3], {}, "test")
    check(False, "a refused subtask stops the task")
except RuntimeError as exc:
    check("not present" in str(exc), "a refused subtask stops the task with HQ's message (instead of waiting forever)")



class _Session:
    """A session with finished tasks (id -> (name, image, result)), like the real client."""
    def __init__(self, tasks, session_id=7):
        outer = self
        self._tasks, self.session_id = tasks, session_id

        class task:  # noqa: N801
            @staticmethod
            def get(task_id):
                t = outer._tasks.get(task_id)
                return {"id": task_id, "name": t[0]} if t else {"msg": f"Task id={task_id} does not exist!"}

        class result:  # noqa: N801
            @staticmethod
            def from_task(task_id):
                t = outer._tasks.get(task_id)
                return [t[2]] if t else []

        self.task, self.result = task, result

    def request(self, endpoint, params=None):
        assert endpoint == "task" and params["session_id"] == self.session_id and "status" not in params
        if self.session_id == "broken":
            return {"msg": "An unexpected error occurred!"}
        return {"data": [{"id": i, "name": t[0], "image": t[1]} for i, t in sorted(self._tasks.items(), reverse=True)]}


session = _Session({30: ("en old", "ghcr.io/x/elastic-net:1.2.0", {"selected_cpgs": ["cgA"], "cohorts": ["a", "b"]}),
                    31: ("svm", "ghcr.io/x/svm:1.4.0", {"algorithm": "federated_linear_svm"}),
                    32: ("en new", "ghcr.io/x/elastic-net:1.2.0", {"selected_cpgs": ["cgB"], "cohorts": ["a", "b"]}),
                    33: ("apply", "ghcr.io/x/svm:1.4.0", {"algorithm": "federated_linear_svm_apply"})})
check(central_module._latest_task(session, central_module._is_elastic_net_result, ("elastic-net",), "Elastic Net") == 32,
      "empty Elastic Net field: the most recent Elastic Net task of the session")
check(central_module._latest_task(session, central_module._is_svm_model_result, ("svm",), "Federated linear SVM") == 31,
      "empty SVM field in Apply: the most recent SVM training task (not an earlier Apply)")

print(f"\n{'All checks passed' if not failures else f'{len(failures)} check(s) failed'}")
sys.exit(1 if failures else 0)
