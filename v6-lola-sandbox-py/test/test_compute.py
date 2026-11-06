"""
Run this script to test the LOLA enrichment inside the Docker image (R, LOLA and
LOLACore hg38 are only installed there):

    docker build -t lola-sandbox:dev .
    docker run --rm -v "$PWD/test:/app/test" -v <folder with the DMP/DMR result>:/results \
        lola-sandbox:dev python /app/test/test_compute.py [/results/<dmp result>.json]

[1] builds a tiny region database (BED files, as LOLA reads them) and checks every
count of the 2x2 tables and every p-value against values computed by hand in
Python. [2] runs on a real DMP/DMR result (DMRs + universe) against LOLACore hg38,
if one is given. [3] checks the arguments, [4] what a real vantage6 node accepts.
"""
import importlib
import inspect
import json
import re
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import false_discovery_control, fisher_exact

current_path = Path(__file__).parent
sys.path.insert(0, str(current_path.parent))

from lola_sandbox.central import COLLECTIONS, REGION_DB, decode_table, lola_enrichment  # noqa: E402

failures = []


def check(condition, message):
    print(("  OK   " if condition else "  FAIL ") + message)
    if not condition:
        failures.append(message)


# ---------------------------------------------------------------------------
# 1. Counts and p-values on a tiny database, checked by hand
# ---------------------------------------------------------------------------
print("\n[1] Tiny region database")
universe = pd.DataFrame({"chromosome": ["chr1"] * 100 + ["chr2"] * 50,
                         "position": list(range(100, 10_100, 100)) + list(range(100, 5_100, 100))})
dmrs = pd.DataFrame({"chromosome": ["chr1", "chr1", "chr2"],
                     "start": [150, 4_950, 1_000], "end": [450, 5_250, 1_000]})
# DMR CpGs: chr1 200, 300, 400, 5000, 5100, 5200 and chr2 1000
dmr_cpgs = {("chr1", 200), ("chr1", 300), ("chr1", 400), ("chr1", 5_000), ("chr1", 5_100),
            ("chr1", 5_200), ("chr2", 1_000)}
region_sets = {
    "promoters.bed": [("chr1", 100, 1_000), ("chr2", 900, 1_100)],  # all of the DMRs but 3
    "enhancers.bed": [("chr1", 5_050, 5_150), ("chr1", 9_000, 9_950)],
    "far.bed": [("chr2", 3_000, 4_000)],
}

with tempfile.TemporaryDirectory() as db:
    regions = Path(db, "toy", "regions")
    regions.mkdir(parents=True)
    for name, rows in region_sets.items():
        pd.DataFrame(rows).to_csv(regions / name, sep="\t", header=False, index=False)
    pd.DataFrame({"filename": list(region_sets), "description": ["Promoters", "Enhancers", "Far"]}).to_csv(
        Path(db, "toy", "index.txt"), sep="\t", index=False)

    out = lola_enrichment(dmrs, universe, collections=("toy",), top_n=10, region_db=db, known=("toy",))
    result = pd.DataFrame(out["results"]).set_index("filename")

    check(out["summary"]["n_dmr_cpgs"] == len(dmr_cpgs), f"the 3 DMRs cover {len(dmr_cpgs)} universe CpGs")
    pvalues = {}
    for name, rows in region_sets.items():
        in_set = {(c, p) for c, p in zip(universe["chromosome"], universe["position"])
                  if any(c == rc and s < p <= e for rc, s, e in rows)}  # BED starts are 0-based
        a = len(in_set & dmr_cpgs)
        b = len(in_set) - a
        c = len(dmr_cpgs) - a
        d = len(universe) - a - b - c
        row = result.loc[name]
        check([row["support"], row["b"], row["c"], row["d"]] == [a, b, c, d],
              f"{name}: support, b, c, d = {a}, {b}, {c}, {d}")
        odds, p = fisher_exact([[a, b], [c, d]], alternative="greater")
        pvalues[name] = p
        check(np.isclose(row["pValue"], p) and np.isclose(row["oddsRatio"], odds),
              f"{name}: one-sided Fisher p = {p:.3g}")
    names = list(pvalues)
    q = dict(zip(names, false_discovery_control([pvalues[n] for n in names], method="bh")))
    check(all(np.isclose(result.loc[n, "qValue"], q[n]) for n in names), "qValue is Benjamini-Hochberg over the region sets")
    check(list(result.index) == sorted(names, key=pvalues.get), "region sets are sorted by p-value")
    check(result.loc["promoters.bed", "description"] == "Promoters", "the database annotation is merged")

    depletion = pd.DataFrame(lola_enrichment(dmrs, universe, ("toy",), "depletion", region_db=db,
                                             known=("toy",))["results"]).set_index("filename")
    far = depletion.loc["far.bed"]
    check(np.isclose(far["pValue"], fisher_exact([[far["support"], far["b"]], [far["c"], far["d"]]], alternative="less")[1])
          and far["pValue"] > 0.5 and depletion.loc["promoters.bed", "pValue"] > 0.9,
          "direction='depletion' tests the lower tail")
    check(lola_enrichment(dmrs.iloc[:0], universe, ("toy",), region_db=db, known=("toy",))["warnings"]
          == ["No DMRs given: nothing to test"], "no DMRs gives a warning and no results")

# ---------------------------------------------------------------------------
# 2. A real DMP/DMR result against LOLACore hg38
# ---------------------------------------------------------------------------
print("\n[2] Real DMP/DMR result against LOLACore hg38")
if len(sys.argv) > 1:
    dmp = json.loads(Path(sys.argv[1]).read_text())
    real_dmrs, real_universe = decode_table(dmp["global_dmrs"]), decode_table(dmp["universe"])
    out = lola_enrichment(real_dmrs, real_universe, top_n=10_000)
    summary = out["summary"]
    print("  ", json.dumps(summary))
    real = pd.DataFrame(out["results"])
    check(summary["n_region_sets"] > 1000 and len(real) == summary["n_region_sets"], "all LOLACore region sets are tested")
    check(((real["support"] + real["b"] + real["c"] + real["d"]) == summary["n_universe"]).all(),
          "every 2x2 table adds up to the universe")
    check(((real["support"] + real["c"]) == summary["n_dmr_cpgs"]).all(), "support + c = the DMR CpGs")
    check(set(real["collection"]) <= set(COLLECTIONS), "only the requested collections")
    print(real.head(10)[["collection", "description", "cellType", "antibody", "support", "b", "oddsRatio", "pValue", "qValue"]]
          .to_string(index=False))
else:
    print("   skipped: pass a DMP/DMR result JSON (with 'global_dmrs' and 'universe')")

# ---------------------------------------------------------------------------
# 3. Arguments
# ---------------------------------------------------------------------------
print("\n[3] Arguments")
for kwargs, message in [
    ({"collections": ("nope",)}, "Unknown collection"),
    ({"direction": "up"}, "direction must be"),
    ({"top_n": 0}, "top_n must be"),
    ({"top_n": 10_001}, "top_n must be"),
]:
    try:
        lola_enrichment(dmrs, universe, **kwargs)
        check(False, f"{kwargs} is rejected")
    except ValueError as error:
        check(message in str(error), f"{kwargs} is rejected: {error}")
try:
    lola_enrichment(dmrs.drop(columns="end"), universe)
    check(False, "DMRs without 'end' are rejected")
except ValueError as error:
    check("missing ['end']" in str(error), "DMRs without 'end' are rejected")

# ---------------------------------------------------------------------------
# 4. Only what a real vantage6 node accepts
# ---------------------------------------------------------------------------
print("\n[4] Docker image")
pkg_name = re.search(r'ARG PKG_NAME="([^"]+)"', (current_path.parent / "Dockerfile").read_text()).group(1)
module = importlib.import_module(pkg_name)
check(hasattr(module, "central_function"), f"the Dockerfile's PKG_NAME ('{pkg_name}') is the importable module")
check(Path(REGION_DB).is_dir() and all(Path(REGION_DB, c).is_dir() for c in COLLECTIONS),
      f"LOLACore hg38 is in the image at {REGION_DB}")
store = json.loads((current_path.parent / "algorithm_store.json").read_text())
store_args = {a["name"] for f in store["functions"] for a in f["arguments"] if not a.get("is_frontend_only")}
code_args = set(inspect.signature(module.central_function).parameters)
check(store_args <= code_args, f"algorithm_store.json arguments exist in central_function {sorted(store_args)}")

print("\n[5] DMRs and universe from a DMP/DMR task")
from lola_sandbox.central import inputs_from_task  # noqa: E402


class _Fake:
    def __init__(self, tasks):
        outer = self
        self._tasks = tasks

        class task:  # noqa: N801
            @staticmethod
            def get(task_id):
                t = outer._tasks.get(task_id)
                return {"id": task_id, "name": t[0]} if t else {"msg": f"Task id={task_id} does not exist!"}

        class result:  # noqa: N801
            @staticmethod
            def from_task(task_id):
                t = outer._tasks.get(task_id)
                return [t[1]] if t else []

        self.task, self.result = task, result


dmp21 = {"comparison": {"cohort_a": "lipid", "cohort_b": "highcardio"}, "summary": {"n_universe": 3},
         "global_dmrs": [{"chromosome": "chr1", "start": 1, "end": 9}], "universe": {"format": "x", "data": ""}}
dmp20 = {k: v for k, v in dmp21.items() if k != "universe"}
fake = _Fake({1: ("dmp", dmp21), 2: ("old dmp", dmp20), 3: ("svm", {"algorithm": "svm"}), 4: ("running", None)})
got_dmrs, got_universe, prov = inputs_from_task(fake, 1)
check(got_dmrs == dmp21["global_dmrs"] and got_universe == dmp21["universe"] and prov["task_id"] == 1
      and "highcardio vs lipid" in prov["description"], "a DMP/DMR 2.1 task gives its DMRs, universe and provenance")
for bad, text in ((2, "before 2.1.0"), (3, "not a DMP/DMR result"), (4, "no result yet"), (9, "could not be read")):
    try:
        inputs_from_task(fake, bad)
        check(False, f"task {bad} is rejected")
    except ValueError as exc:
        check(text in str(exc), f"task {bad} is rejected: {exc}")



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
        assert endpoint == "task" and params["session_id"] == self.session_id and params["status"] == "completed"
        return {"data": [{"id": i, "name": t[0], "image": t[1]} for i, t in sorted(self._tasks.items(), reverse=True)]}


from lola_sandbox.central import is_dmp_result, latest_task  # noqa: E402
session = _Session({170: ("dmp old", "ghcr.io/x/dmp-dmr:2.2.0", dmp21), 172: ("dmp 2.0", "ghcr.io/x/dmp-dmr:2.0.0", dmp20),
                    173: ("dmp new", "ghcr.io/x/dmp-dmr:2.2.0", dmp21), 175: ("svm", "ghcr.io/x/svm:1.4.0", {"algorithm": "svm"})})
check(latest_task(session, is_dmp_result, ("dmp-dmr",), "DMP/DMR") == 173,
      "empty task field: the most recent DMP/DMR task (with universe) of the session; newer other tasks are skipped")
for client, text in ((_Session({175: ("svm", "svm", {"algorithm": "svm"})}), "No finished DMP/DMR task in this session"),
                     (_Session({}, session_id=None), "No session to look in")):
    try:
        latest_task(client, is_dmp_result, ("dmp-dmr",), "DMP/DMR")
        check(False, "no suitable task gives a clear message")
    except ValueError as exc:
        check(text in str(exc), f"no suitable task gives a clear message: {exc}")

print(f"\n{'All checks passed' if not failures else f'{len(failures)} check(s) failed'}")
sys.exit(1 if failures else 0)
