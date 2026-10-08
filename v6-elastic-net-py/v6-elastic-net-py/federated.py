"""
Elastic Net federado — funciones de COMPUTE (vantage6 v5 "Uluru").

Enfoque EXACTO via estadisticos suficientes (Gram matrix). Validado en
notebooks/00_elasticnet_phase0.py: federado == centralizado (max dif 2.5e-9).

Estructura v5 (sessions): la extraccion de datos va en extract.py; aqui solo
compute. Dos funciones federadas (corren en cada nodo) + una central (orquesta):

  partial_moments -> media/std/ysum locales (para estandarizar igual en todos)
  partial_gram    -> G_k = Xs^T Xs y b_k = Xs^T yc (estadisticos suficientes)
  central         -> agrega G, b de todos los nodos y resuelve coordinate descent

Convencion BIOMERIS: errores como {"error": msg}, no raise. Outputs
JSON-serializables (.tolist()).

Entrada (independiente del modelo de datos): cada nodo acepta
  - formato ANCHO: una fila por paciente, una columna por CpG + outcome
    (+ cohorte opcional), o
  - formato LARGO (salida de v6-preprocessIDAT-py): una fila por
    (muestra, sonda) con probe_id, sample_column, value_column y las columnas
    por muestra outcome (+ cohorte). Se pivota en el nodo solo para `features`.
Los nombres de columna se pasan como argumentos.

Centrado (`center_by`):
  - "global"          : media/std global (comportamiento original).
  - "hospital"        : cada nodo centra X e y con sus propias medias.
  - "hospital_cohort" : centra dentro de cada (nodo, cohorte).
Centrar dentro de grupos equivale EXACTAMENTE a añadir un intercepto no
penalizado por grupo (Frisch-Waugh-Lovell): elimina diferencias entre hospitales
(batch) o entre cohortes sin salir de la solucion exacta. La escala es la std
intra-grupo agregada. Ojo: "hospital_cohort" elimina tambien el efecto de la
cohorte; no usarlo si el outcome depende de la cohorte que se quiere estudiar.
"""

from typing import Any
import numpy as np
import pandas as pd

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.action import federated, central
from vantage6.algorithm.decorator.data import dataframe
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.client import AlgorithmClient


# --------------------------------------------------------------------------
# Helpers (corren donde toque; no son funciones de algoritmo)
# --------------------------------------------------------------------------
CENTER_MODES = ("global", "hospital", "hospital_cohort")


def _to_wide(df: pd.DataFrame, features: list, outcome_column: str,
             cohort_column: str | None, sample_column: str, value_column: str) -> Any:
    """Formato largo -> ancho (una fila por muestra) solo para `features`."""
    needed = ["probe_id", sample_column, value_column, outcome_column]
    if cohort_column:
        needed.append(cohort_column)
    missing = [c for c in needed if c not in df.columns]
    if missing:
        return {"error": f"missing columns: {missing}"}

    long = df[df["probe_id"].isin(features)]
    wide = long.pivot(index=sample_column, columns="probe_id", values=value_column)
    wide = wide.reindex(columns=list(features))

    per_sample = [outcome_column] + ([cohort_column] if cohort_column else [])
    meta = df[[sample_column] + per_sample].drop_duplicates()
    if meta[sample_column].duplicated().any():
        return {"error": "outcome/cohort values differ between rows of the same sample"}
    return wide.join(meta.set_index(sample_column), how="left").reset_index(drop=True)


def _prepare(df: pd.DataFrame, features: list, outcome_column: str,
             min_samples: int, center_by: str = "global",
             cohort_column: str | None = None, cohorts: list | None = None,
             sample_column: str = "sample_label", value_column: str = "beta") -> Any:
    """Subset limpio (ancho) con columna `_group`, o dict de error.

    Privacy guard K=min_samples: el nodo debe tener >= min_samples muestras y, al
    centrar por grupo, los grupos con < min_samples se descartan.
    """
    if center_by not in CENTER_MODES:
        return {"error": f"center_by must be one of {CENTER_MODES}"}
    needs_cohort = center_by == "hospital_cohort" or bool(cohorts)
    if needs_cohort and not cohort_column:
        return {"error": "cohort_column is required for center_by='hospital_cohort' or cohorts"}
    if not needs_cohort:
        cohort_column = None

    if "probe_id" in df.columns:
        df = _to_wide(df, features, outcome_column, cohort_column, sample_column, value_column)
        if isinstance(df, dict):
            warn(df["error"])
            return df

    cols = list(features) + [outcome_column] + ([cohort_column] if cohort_column else [])
    missing = [c for c in cols if c not in df.columns]
    if missing:
        warn(f"Missing columns: {missing}")
        return {"error": f"missing columns: {missing}"}
    sub = df[cols].dropna()

    if cohorts:
        sub = sub[sub[cohort_column].isin(cohorts)]

    if center_by == "hospital_cohort":
        sub = sub.assign(_group=sub[cohort_column].astype(str))
        sizes = sub["_group"].value_counts()
        small = sizes[sizes < min_samples].index
        for g in small:
            info(f"Dropping cohort '{g}': fewer than min_samples={min_samples} samples on this node")
        sub = sub[~sub["_group"].isin(small)]
    else:
        sub = sub.assign(_group="node")

    if len(sub) < min_samples:
        warn(f"Node has {len(sub)} < {min_samples} samples — skipped (privacy guard)")
        return {"error": f"insufficient samples (< {min_samples})"}
    return sub


def _center_within_groups(sub: pd.DataFrame, features: list, outcome_column: str):
    """X e y centrados con la media de su grupo (nodo o nodo×cohorte)."""
    X = sub[list(features)].astype(float)
    y = sub[outcome_column].astype(float)
    Xc = X - X.groupby(sub["_group"]).transform("mean")
    yc = y - y.groupby(sub["_group"]).transform("mean")
    return Xc.to_numpy(), yc.to_numpy()


def _coordinate_descent_gram(G, b, n, alpha, l1_ratio, max_iter, tol):
    """CD de Elastic Net usando solo G=X^T X y b=X^T y (corre en el central).

    Update por coordenada j (soft-thresholding):
      rho_j = (b_j - (G·w)_j + G_jj·w_j) / n        # = (1/n) X_j^T (y - X w_{-j})
      w_j   = sign(rho_j)·max(|rho_j|-alpha·l1, 0) / (1 + alpha·(1-l1))
    """
    p = G.shape[0]
    w = np.zeros(p)
    Gw = np.zeros(p)                       # cache de G·w (incremental)
    gamma = alpha * l1_ratio
    denom = 1.0 + alpha * (1.0 - l1_ratio)
    for _ in range(max_iter):
        w_old = w.copy()
        for j in range(p):
            rho = (b[j] - Gw[j] + G[j, j] * w[j]) / n
            new = np.sign(rho) * max(abs(rho) - gamma, 0.0) / denom
            if new != w[j]:
                Gw += G[:, j] * (new - w[j])
                w[j] = new
        if np.linalg.norm(w - w_old) < tol:
            break
    return w


def _aggregate(results: list, key: str) -> np.ndarray:
    return np.sum([np.asarray(r[key], dtype=float) for r in results], axis=0)


def _format_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}"
        n /= 1024


def _check_dimensionality(n_features: int, warn_gb: float = 1.0) -> None:
    """Solo LEE el numero de features y avisa del coste en memoria.

    El metodo exacto construye una matriz de Gram p×p (float64) en cada nodo y
    en el central. Esto NO toca los datos; solo estima el tamaño y avisa si es
    grande (p.ej. con ~850K CpGs serian TB -> inviable sin pre-screening).
    """
    gram_bytes = n_features * n_features * 8  # p×p float64
    info(f"Dimensionalidad: {n_features} features -> matriz Gram "
         f"{n_features}×{n_features} ≈ {_format_bytes(gram_bytes)}")
    if gram_bytes > warn_gb * 1024 ** 3:
        warn(f"ALERTA MEMORIA: la matriz Gram (p×p) ocupara ~{_format_bytes(gram_bytes)} "
             f"por nodo y en el central. Con p={n_features} features esto puede agotar "
             f"la memoria del nodo. Recomendado: reducir features (pre-screening) antes "
             f"de ejecutar el Elastic Net exacto.")


# --------------------------------------------------------------------------
# Funciones FEDERADAS (corren en cada nodo, ven datos locales)
# --------------------------------------------------------------------------
@federated
@dataframe(1)
def partial_moments(
    df1: pd.DataFrame, features: list, outcome_column: str, min_samples: int = 5,
    center_by: str = "global", cohort_column: str | None = None,
    cohorts: list | None = None, sample_column: str = "sample_label",
    value_column: str = "beta",
) -> dict:
    """Ronda 1: momentos locales para la estandarizacion.

    Devuelve sumas globales (modo "global") y la suma de cuadrados intra-grupo
    `ss_within` (modos centrados por grupo).
    """
    info("Elastic Net — partial_moments")
    sub = _prepare(df1, features, outcome_column, min_samples, center_by,
                   cohort_column, cohorts, sample_column, value_column)
    if isinstance(sub, dict):
        return sub
    X = sub[list(features)].to_numpy(dtype=float)
    Xc, _ = _center_within_groups(sub, features, outcome_column)
    return {
        "n": int(len(sub)),
        "n_groups": int(sub["_group"].nunique()),
        "sum": X.sum(axis=0).tolist(),
        "sumsq": (X ** 2).sum(axis=0).tolist(),
        "ss_within": (Xc ** 2).sum(axis=0).tolist(),
        "ysum": float(sub[outcome_column].sum()),
    }


@federated
@dataframe(1)
def partial_gram(
    df1: pd.DataFrame, features: list, outcome_column: str,
    mean: list, std: list, ymean: float, min_samples: int = 5,
    center_by: str = "global", cohort_column: str | None = None,
    cohorts: list | None = None, sample_column: str = "sample_label",
    value_column: str = "beta",
) -> dict:
    """Ronda 2: estadisticos suficientes G = Xs^T Xs, b = Xs^T yc.

    Estandariza con media/std GLOBAL (del central) para que el agregado de G
    entre nodos sea exacto. Solo devuelve agregados; nunca filas individuales.

    NOTA privacidad/escala: G es p×p. Para p grande (~850K CpGs) es inviable y
    podria filtrar covarianzas -> usar solo sobre un subset pre-filtrado.
    """
    info("Elastic Net — partial_gram")
    sub = _prepare(df1, features, outcome_column, min_samples, center_by,
                   cohort_column, cohorts, sample_column, value_column)
    if isinstance(sub, dict):
        return sub
    # Aviso de memoria en el nodo (construye una matriz Gram p×p local)
    _check_dimensionality(len(features))
    if len(features) >= len(sub):
        warn(f"p={len(features)} features >= n={len(sub)} samples on this node: the "
             "shared Gram matrix carries a lot of information about each patient. "
             "Use fewer features than patients per node.")
    std_arr = np.asarray(std, dtype=float)
    if center_by == "global":
        mean_arr = np.asarray(mean, dtype=float)
        Xs = (sub[list(features)].to_numpy(dtype=float) - mean_arr) / std_arr
        yc = sub[outcome_column].to_numpy(dtype=float) - ymean
    else:
        Xc, yc = _center_within_groups(sub, features, outcome_column)
        Xs = Xc / std_arr
    return {"G": (Xs.T @ Xs).tolist(), "b": (Xs.T @ yc).tolist()}


# --------------------------------------------------------------------------
# Funcion CENTRAL (orquesta, NO ve datos)
# --------------------------------------------------------------------------
@central
@algorithm_client
def central(
    client: AlgorithmClient,
    features: list,
    outcome_column: str,
    alpha: float = 0.01,
    l1_ratio: float = 0.5,
    max_iter: int = 1000,
    tol: float = 1e-8,
    min_samples: int = 5,
    warn_gram_gb: float = 1.0,
    center_by: str = "hospital",
    cohort_column: str | None = None,
    cohorts: list | None = None,
    sample_column: str = "sample_label",
    value_column: str = "beta",
) -> dict:
    """Coordinador del Elastic Net federado (estadisticos suficientes, 2 rondas).

    center_by: "hospital" (defecto, intercepto por hospital: elimina batch entre
    hospitales), "hospital_cohort" (intercepto por hospital×cohorte) o "global".
    cohort_column/cohorts: columna de cohorte y, opcionalmente, cohortes a incluir.
    sample_column/value_column: solo para entrada en formato largo.
    """
    if center_by not in CENTER_MODES:
        return {"error": f"center_by must be one of {CENTER_MODES}"}
    org_ids = [o["id"] for o in client.organization.list()]
    info(f"Federated Elastic Net | {len(org_ids)} nodos | {len(features)} features "
         f"| center_by={center_by}")
    data_args = {"features": features, "outcome_column": outcome_column,
                 "min_samples": min_samples, "center_by": center_by,
                 "cohort_column": cohort_column, "cohorts": cohorts,
                 "sample_column": sample_column, "value_column": value_column}

    # Aviso de coste en memoria (solo lee el nº de features, no toca datos)
    _check_dimensionality(len(features), warn_gb=warn_gram_gb)

    # --- Ronda 1: momentos -> estandarizacion global ---
    task1 = client.task.create(
        method="partial_moments",
        arguments=data_args,
        organizations=org_ids,
        name="ElasticNet round 1 (moments)",
    )
    r1 = [r for r in client.wait_for_results(task_id=task1.get("id")) if "error" not in r]
    if not r1:
        return {"error": "no valid nodes in round 1"}
    total_n = sum(r["n"] for r in r1)
    mean = _aggregate(r1, "sum") / total_n
    if center_by == "global":
        var = _aggregate(r1, "sumsq") / total_n - mean ** 2
    else:
        var = _aggregate(r1, "ss_within") / total_n   # varianza intra-grupo agregada
    std = np.sqrt(np.maximum(var, 1e-12))
    ymean = sum(r["ysum"] for r in r1) / total_n

    # --- Ronda 2: estadisticos suficientes G, b ---
    task2 = client.task.create(
        method="partial_gram",
        arguments={**data_args, "mean": mean.tolist(), "std": std.tolist(),
                   "ymean": ymean},
        organizations=org_ids,
        name="ElasticNet round 2 (gram)",
    )
    r2 = [r for r in client.wait_for_results(task_id=task2.get("id")) if "error" not in r]
    if not r2:
        return {"error": "no valid nodes in round 2"}
    if len(r2) < len(r1):
        warn(f"Round 2: {len(r1) - len(r2)} nodos cayeron entre rondas")

    # --- Central resuelve el descent sobre los agregados ---
    G = _aggregate(r2, "G")
    b = _aggregate(r2, "b")
    w = _coordinate_descent_gram(G, b, total_n, alpha, l1_ratio, max_iter, tol)

    selected = [f for f, wj in zip(features, w) if abs(wj) > 1e-6]
    info(f"Elastic Net done | {len(selected)}/{len(features)} CpGs seleccionadas")
    return {
        "coefficients": w.tolist(),
        "features": list(features),
        "selected_cpgs": selected,
        "n_total": int(total_n),
        "n_nodes": len(r2),
        "n_groups": int(sum(r["n_groups"] for r in r1)),
        "nodes_skipped": len(org_ids) - len(r2),
        "center_by": center_by,
        "cohorts": list(cohorts) if cohorts else None,
        "hyperparams": {"alpha": alpha, "l1_ratio": l1_ratio},
    }
