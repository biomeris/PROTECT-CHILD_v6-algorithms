"""
Test del Elastic Net federado con MockNetwork (vantage6 v5).

Reparte el CSV toy en 3 "nodos" (hospitales), ejecuta la funcion central
end-to-end y valida que los coeficientes federados coinciden con
sklearn.ElasticNet centralizado sobre los datos completos (exacto), para:

  [1] center_by="global"           (comportamiento original)
  [2] entrada en formato LARGO      (salida de v6-preprocessIDAT-py) == ANCHO
  [3] center_by="hospital"          (intercepto por hospital)
  [4] center_by="hospital_cohort"   (intercepto por hospital x cohorte), con
      un grupo pequeño descartado por min_samples
  [5] filtro `cohorts`
  [6] errores de entrada
  [7] outcome de texto (cohorte) con positive_value
  [8] CpG ausente en un hospital: se excluye y el hospital sigue participando
  [9] limite de features, Gram en binario, resultado ordenado
  [10] solo lo que acepta un nodo real de vantage6

A cada hospital se le añade un desplazamiento (batch) en X e y para que los
modos de centrado den resultados distintos.

Ejecutar (con el package instalado: pip install -e .):
    python test/test.py
"""

import json
import os

import numpy as np
import pandas as pd
from sklearn.linear_model import ElasticNet
from vantage6.algorithm.mock.network import MockNetwork

DATA = os.path.join(os.path.dirname(__file__), "test_data.csv")
FEATURES = [f"cg{i:08d}" for i in range(12)]
OUTCOME = "outcome"
ALPHA, L1_RATIO = 0.01, 0.5
MIN_SAMPLES = 3

# --- Datos: 3 hospitales, cohortes, batch por hospital ---
df = pd.read_csv(DATA)
df["hospital"] = np.repeat([0, 1, 2], [14, 13, 13])
df["cohort"] = np.where(np.arange(len(df)) % 2 == 0, "cohort_A", "cohort_B")
df.loc[[26, 25], "cohort"] = "cohort_C"          # hospital 1: cohort_C con 2 muestras
batch = df["hospital"].map({0: 0.0, 1: 0.04, 2: -0.03})
df[FEATURES] = df[FEATURES].add(batch, axis=0)
df[OUTCOME] = df[OUTCOME] + df["hospital"].map({0: 0.0, 1: 0.3, 2: -0.2})
nodes = [d.drop(columns="hospital").reset_index(drop=True) for _, d in df.groupby("hospital")]


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
    print(f"  PASS: {message}")


def run_central(datasets: list, **arguments) -> dict:
    network = MockNetwork(
        module_name="v6-elastic-net-py",
        datasets=[{"methylation": {"database": d, "db_type": "csv"}} for d in datasets],
    )
    client = network.user_client
    task = client.task.create(
        method="central",
        organizations=[network.organization_ids[0]],
        arguments={"features": FEATURES, "outcome_column": OUTCOME, "alpha": ALPHA,
                   "l1_ratio": L1_RATIO, "min_samples": MIN_SAMPLES, **arguments},
        databases=[{"type": "dataframe", "dataframe_id": client.dataframe.list()[0]["id"]}],
    )
    result = client.result.from_task(task["id"])[0]
    json.dumps(result)  # debe ser JSON-serializable
    return result


def reference(data: pd.DataFrame, groups: pd.Series | None) -> np.ndarray:
    """sklearn sobre los datos completos; centrado global o dentro de `groups`."""
    X = data[FEATURES].astype(float)
    y = data[OUTCOME].astype(float)
    if groups is None:
        Xc, yc = X - X.mean(), y - y.mean()
    else:
        Xc = X - X.groupby(groups).transform("mean")
        yc = y - y.groupby(groups).transform("mean")
    std = np.sqrt((Xc ** 2).mean())
    Xs = (Xc / np.where(std < 1e-12, 1.0, std)).to_numpy()
    return ElasticNet(alpha=ALPHA, l1_ratio=L1_RATIO, fit_intercept=False,
                      max_iter=100000, tol=1e-10).fit(Xs, yc.to_numpy()).coef_


def compare(result: dict, expected: np.ndarray, label: str) -> None:
    w = np.asarray(result["coefficients"], dtype=float)
    diff = float(np.max(np.abs(w - expected)))
    print(f"  {label}: max |dif| = {diff:.2e}, selected = {len(result['selected_cpgs'])}/{len(FEATURES)}")
    check(diff < 1e-6, f"{label}: federado == centralizado (exacto)")


def to_long(d: pd.DataFrame) -> pd.DataFrame:
    """Ancho -> largo, como la salida de v6-preprocessIDAT-py."""
    return d.melt(id_vars=["patient_id", OUTCOME, "cohort"], value_vars=FEATURES,
                  var_name="probe_id", value_name="beta").rename(columns={"patient_id": "sample_label"})


# ---------------------------------------------------------------------------
print("\n[1] center_by='global' (original)")
r_global = run_central(nodes, center_by="global")
compare(r_global, reference(df, None), "global")

# ---------------------------------------------------------------------------
print("\n[2] Long-format input")
r_long = run_central([to_long(d) for d in nodes], center_by="global")
check(np.allclose(r_long["coefficients"], r_global["coefficients"], atol=1e-12),
      "formato largo da los mismos coeficientes que el ancho")

# ---------------------------------------------------------------------------
print("\n[3] center_by='hospital' (default)")
r_hosp = run_central(nodes)
check(r_hosp["center_by"] == "hospital", "el modo por defecto es 'hospital'")
compare(r_hosp, reference(df, df["hospital"]), "hospital")
check(not np.allclose(r_hosp["coefficients"], r_global["coefficients"], atol=1e-3),
      "con batch entre hospitales, 'hospital' y 'global' difieren (test discriminante)")
r_hosp_long = run_central([to_long(d) for d in nodes])
check(np.allclose(r_hosp_long["coefficients"], r_hosp["coefficients"], atol=1e-12),
      "formato largo == ancho tambien en modo 'hospital'")

# ---------------------------------------------------------------------------
print("\n[4] center_by='hospital_cohort'")
r_hc = run_central(nodes, center_by="hospital_cohort", cohort_column="cohort")
kept = df[~((df["hospital"] == 1) & (df["cohort"] == "cohort_C"))]
compare(r_hc, reference(kept, kept["hospital"].astype(str) + "_" + kept["cohort"]), "hospital_cohort")
check(r_hc["n_total"] == len(kept) and r_hc["n_groups"] == 6,
      "cohort_C (2 muestras en el hospital 1) se descarta; 6 grupos hospital x cohorte")

# ---------------------------------------------------------------------------
print("\n[5] cohorts filter")
r_sub = run_central(nodes, cohort_column="cohort", cohorts=["cohort_A"])
sub = df[df["cohort"] == "cohort_A"]
compare(r_sub, reference(sub, sub["hospital"]), "cohorts=['cohort_A'], center_by='hospital'")
check(r_sub["n_total"] == len(sub) and r_sub["cohorts"] == ["cohort_A"], "solo se usan muestras de cohort_A")

# ---------------------------------------------------------------------------
print("\n[6] Input errors")
check("error" in run_central(nodes, center_by="hospital_cohort"),
      "center_by='hospital_cohort' sin cohort_column -> error")
check("error" in run_central(nodes, center_by="other"), "center_by desconocido -> error")
check("error" in run_central([d.drop(columns="cohort") for d in nodes], center_by="hospital_cohort",
                             cohort_column="cohort"), "cohort_column ausente -> error")

# ---------------------------------------------------------------------------
def reference_y(data: pd.DataFrame, features: list, y: pd.Series, groups: pd.Series) -> np.ndarray:
    """Como `reference`, con features y outcome dados."""
    X = data[features].astype(float)
    Xc = X - X.groupby(groups).transform("mean")
    yc = y - y.groupby(groups).transform("mean")
    Xs = (Xc / np.sqrt((Xc ** 2).mean())).to_numpy()
    return ElasticNet(alpha=ALPHA, l1_ratio=L1_RATIO, fit_intercept=False,
                      max_iter=100000, tol=1e-10).fit(Xs, yc.to_numpy()).coef_


print("\n[7] Text outcome (cohort) with positive_value")
two = df[df["cohort"].isin(["cohort_A", "cohort_B"])]
r_cohort = run_central([to_long(d) for d in nodes], outcome_column="cohort", cohort_column="cohort",
                       cohorts=["cohort_A", "cohort_B"], positive_value="cohort_B")
compare(r_cohort, reference_y(two, FEATURES, (two["cohort"] == "cohort_B").astype(float), two["hospital"]),
        "outcome = (cohort == 'cohort_B')")
check(r_cohort["n_total"] == len(two) and r_cohort["outcome"]["positive_value"] == "cohort_B",
      "solo cohort_A y cohort_B; outcome 0/1 por cohorte")
r_text = run_central(nodes, outcome_column="cohort")
check("error" in r_text and len(r_text["skipped"]) == 3 and "not numeric" in r_text["skipped"][0],
      "outcome de texto sin positive_value -> error que explica la causa")

# ---------------------------------------------------------------------------
print("\n[8] CpG missing in one hospital")
missing_cpg = FEATURES[3]
long_nodes = [to_long(d) for d in nodes]
long_nodes[1] = long_nodes[1][long_nodes[1]["probe_id"] != missing_cpg]  # QC la quito en el hospital 1
r_missing = run_central(long_nodes)
rest = [f for f in FEATURES if f != missing_cpg]
expected = reference_y(df, rest, df[OUTCOME].astype(float), df["hospital"])
check(np.allclose(r_missing["coefficients"], expected, atol=1e-6) and r_missing["features"] == rest,
      "el modelo usa las CpGs disponibles en todos los hospitales (== centralizado)")
check(r_missing["excluded_features"] == [{"feature": missing_cpg, "n_nodes_available": 2}],
      "la CpG excluida se lista, con en cuantos hospitales estaba")
check(r_missing["n_nodes"] == 3 and r_missing["n_total"] == len(df), "los tres hospitales siguen participando")

# ---------------------------------------------------------------------------
print("\n[9] Limits and result")
from importlib import import_module  # noqa: E402
module = import_module("v6-elastic-net-py.federated")
check("error" in run_central(nodes, features=[f"cg{i}" for i in range(module.MAX_FEATURES + 1)]),
      f"mas de {module.MAX_FEATURES} features -> error")
G = np.arange(6, dtype=float).reshape(2, 3)
check(np.array_equal(module._decode_array(module._encode_array(G)), G), "la Gram viaja en binario sin perdidas")
coefs = [abs(s["coefficient"]) for s in r_hosp["selected"]]
check(coefs == sorted(coefs, reverse=True) and [s["feature"] for s in r_hosp["selected"]] == r_hosp["selected_cpgs"],
      "selected: CpGs con su coeficiente, de mayor a menor |coeficiente|")
check(r_hosp["privacy_warnings"] == [], "sin aviso de privacidad cuando hay mas muestras que features")
check(len(r_sub["privacy_warnings"]) == 1, "aviso de privacidad cuando un nodo tiene menos muestras que features")

# ---------------------------------------------------------------------------
print("\n[10] Real vantage6 node")
import ast  # noqa: E402
import inspect  # noqa: E402
import re  # noqa: E402
from pathlib import Path  # noqa: E402
from vantage6.algorithm.client import AlgorithmClient  # noqa: E402

root = Path(__file__).parent.parent
real_params = set(inspect.signature(AlgorithmClient.Task.create).parameters) - {"self"}
tree = ast.parse((root / "v6-elastic-net-py" / "federated.py").read_text())
used = {kw.arg for node in ast.walk(tree) if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute) and node.func.attr == "create"
        and ast.unparse(node.func.value) == "client.task" for kw in node.keywords}
check(used <= real_params, f"client.task.create solo usa argumentos del cliente real {sorted(used)}")
pkg_name = re.search(r'ARG PKG_NAME="([^"]+)"', (root / "Dockerfile").read_text()).group(1)
check(all(hasattr(import_module(pkg_name), f) for f in ("central", "partial_moments", "partial_gram")),
      f"PKG_NAME del Dockerfile ('{pkg_name}') es el modulo importable")

# ---------------------------------------------------------------------------
print("\n[11] Candidates from another task, one-vs-rest, hospitals, refused subtasks")
from importlib import import_module as _im  # noqa: E402
fed = _im("v6-elastic-net-py.federated")


class _FakeClient:
    """Lo minimo del AlgorithmClient para leer otra tarea."""
    def __init__(self, tasks):
        self._tasks = tasks
        outer = self

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


dmp_result = {"comparison": {"cohort_a": "cohort_A", "cohort_b": "cohort_B"},
              "global_dmps": [{"probe_id": f, "n_nodes": 3 if i != 1 else 2, "p_value": i / 100}
                              for i, f in enumerate(FEATURES)]}
beta_result = ([{"cohort": "cohort_A", "probe_id": f, "beta_mean_global": 0.1, "coverage": 1.0} for f in FEATURES]
               + [{"cohort": "cohort_B", "probe_id": f, "beta_mean_global": 0.1 + i / 100,
                   "coverage": 1.0 if i != 11 else 0.5} for i, f in enumerate(FEATURES)])
fake = _FakeClient({10: ("dmp run", dmp_result), 11: ("beta run", beta_result), 12: ("running", None)})
feats, prov = fed.candidates_from_task(fake, 10, 5, ["cohort_A", "cohort_B"])
check(feats == [FEATURES[0]] + FEATURES[2:6] and prov["task_id"] == 10 and "DMP/DMR" in prov["description"],
      "DMP/DMR task: the top DMPs present in all hospitals, smallest p-value first")
feats, prov = fed.candidates_from_task(fake, 11, 3, ["cohort_A", "cohort_B"])
check(feats == [FEATURES[10], FEATURES[9], FEATURES[8]] and "Beta and M values" in prov["description"],
      "Beta and M values task: the largest difference with full coverage")
for bad, text in ((99, "could not be read"), (12, "no result yet")):
    try:
        fed.candidates_from_task(fake, bad, 5, None)
        check(False, f"task {bad} is rejected")
    except ValueError as exc:
        check(text in str(exc), f"task {bad}: {exc}")
fake2 = _FakeClient({13: ("other", {"something": 1})})
try:
    fed.candidates_from_task(fake2, 13, 5, None)
    check(False, "a task of another algorithm is rejected")
except ValueError as exc:
    check("is not a DMP/DMR or Beta and M values result" in str(exc), "a task of another algorithm is rejected")
check(fed.select_organizations(fake, []) == [1, 2, 3] and fed.select_organizations(fake, [3, 1]) == [1, 3],
      "organizations_to_include: empty = all, otherwise those hospitals")
try:
    fed.check_subtask(fake.task.create(), "x")
    check(False, "a refused subtask stops the task")
except RuntimeError as exc:
    check("not present" in str(exc), "a refused subtask stops the task with HQ's message (instead of waiting forever)")

both = run_central(nodes, candidates_task_id=10)
check("error" in both and "not both" in both["error"], "features and candidates_task_id together are rejected")
neither = run_central(nodes, features=[])
check("error" in neither and "Give the candidate CpGs" in neither["error"], "no candidates at all is rejected")

ab = dict(outcome_column="cohort", cohort_column="cohort")
r_default = run_central([to_long(d) for d in nodes], cohorts=["cohort_A", "cohort_B"], **ab)
r_explicit = run_central([to_long(d) for d in nodes], cohorts=["cohort_A", "cohort_B"], positive_value="cohort_B", **ab)
check(r_default["outcome"]["positive_value"] == "cohort_B"
      and np.allclose(r_default["coefficients"], r_explicit["coefficients"]),
      "two cohorts without positive value: the second cohort is the positive one")
check(r_explicit["input"]["source"] == "list", "the result says where its candidates came from")

three = ["cohort_A", "cohort_B", "cohort_C"]
ovr = run_central([to_long(d) for d in nodes], cohorts=three, min_samples=2, **ab)
check(ovr.get("one_vs_rest") and sorted(ovr["per_cohort"]) == three, "three cohorts: one model per cohort")
single = run_central([to_long(d) for d in nodes], cohorts=three, positive_value="cohort_C", min_samples=2, **ab)
check(np.allclose(ovr["per_cohort"]["cohort_C"]["coefficients"], single["coefficients"]),
      "each one-vs-rest model equals a separate run with that positive cohort")
union = set().union(*(r["selected_cpgs"] for r in ovr["per_cohort"].values()))
check(set(ovr["selected_cpgs"]) == union and all(e["cohorts"] for e in ovr["selected"]),
      "selected_cpgs is the union, each with the cohort(s) that selected it")



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


session = _Session({20: ("beta", "ghcr.io/x/betavalues:2.2.0", beta_result), 21: ("dmp", "ghcr.io/x/dmp-dmr:2.2.0", dmp_result),
                    22: ("svm", "ghcr.io/x/svm:1.4.0", {"algorithm": "federated_linear_svm"})})
check(fed.latest_task(session, fed.is_candidates_result, ("dmp-dmr", "betavalues"), "DMP/DMR or Beta and M values") == 21,
      "empty candidates field: the most recent DMP/DMR or Beta and M values task of the session")
only_beta = _Session({20: ("beta", "ghcr.io/x/betavalues:2.2.0", beta_result)})
check(fed.latest_task(only_beta, fed.is_candidates_result, ("dmp-dmr", "betavalues"), "x") == 20,
      "a Beta and M values task is found too")

print("\nAll checks passed.")
