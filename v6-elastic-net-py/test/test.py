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

print("\nAll checks passed.")
