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

print("\nAll checks passed.")
