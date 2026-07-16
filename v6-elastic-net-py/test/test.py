"""
Test del Elastic Net federado con MockNetwork (vantage6 v5).

Reparte el CSV toy en 3 "nodos" (DataFrames distintos -> data extraction
automatica), ejecuta la funcion central end-to-end y valida que los
coeficientes federados coinciden con sklearn.ElasticNet centralizado sobre
los datos completos (criterio Fase 0: max dif ~0, exacto).

Ejecutar (con el package instalado: pip install -e .):
    python test/test.py
"""

import os
import numpy as np
import pandas as pd
from sklearn.linear_model import ElasticNet
from vantage6.algorithm.mock.network import MockNetwork

DATA = os.path.join(os.path.dirname(__file__), "test_data.csv")
FEATURES = [f"cg{i:08d}" for i in range(12)]
OUTCOME = "outcome"
ALPHA, L1_RATIO = 0.01, 0.5

# --- Datos: repartir en 3 nodos (DataFrames -> extraccion automatica) ---
df = pd.read_csv(DATA)
splits = [s.reset_index(drop=True) for s in np.array_split(df, 3)]

network = MockNetwork(
    module_name="v6-elastic-net-py",
    datasets=[
        {"methylation": {"database": splits[0], "db_type": "csv"}},
        {"methylation": {"database": splits[1], "db_type": "csv"}},
        {"methylation": {"database": splits[2], "db_type": "csv"}},
    ],
)
client = network.user_client

# El dataframe se auto-crea al pasar DataFrames directos; recuperamos su id.
df_id = client.dataframe.list()[0]["id"]

# --- Central end-to-end ---
task = client.task.create(
    method="central",
    organizations=[network.organization_ids[0]],
    arguments={"features": FEATURES, "outcome_column": OUTCOME,
               "alpha": ALPHA, "l1_ratio": L1_RATIO},
    databases=[{"type": "dataframe", "dataframe_id": df_id}],
)
result = client.result.from_task(task["id"])[0]

print("\n=== Central result ===")
print("  n_total       :", result.get("n_total"))
print("  n_nodes       :", result.get("n_nodes"))
print("  selected_cpgs :", result.get("selected_cpgs"))

# --- Validacion: federado == centralizado (sklearn sobre datos completos) ---
w_fed = np.asarray(result["coefficients"], dtype=float)
X = df[FEATURES].to_numpy(float)
Xs = (X - X.mean(0)) / np.where(X.std(0) < 1e-12, 1.0, X.std(0))
y = df[OUTCOME].to_numpy(float) - df[OUTCOME].mean()
w_sk = ElasticNet(alpha=ALPHA, l1_ratio=L1_RATIO, fit_intercept=False,
                  max_iter=100000, tol=1e-10).fit(Xs, y).coef_

max_diff = float(np.max(np.abs(w_fed - w_sk)))
corr = float(np.corrcoef(w_fed, w_sk)[0, 1])
print("\n=== Validacion federado == centralizado ===")
print(f"  corr coeficientes     : {corr:.6f}")
print(f"  max |dif| coeficiente : {max_diff:.2e}")
assert max_diff < 1e-6, f"FALLA: federado != centralizado (dif {max_diff:.2e})"
print("\n  PASA: federado == centralizado (exacto).")
