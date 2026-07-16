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
def _validate(df: pd.DataFrame, features: list, outcome_column: str,
              min_samples: int) -> Any:
    """Subset limpio o dict de error (privacy guard K=min_samples)."""
    missing = [c for c in list(features) + [outcome_column] if c not in df.columns]
    if missing:
        warn(f"Missing columns: {missing}")
        return {"error": f"missing columns: {missing}"}
    sub = df[list(features) + [outcome_column]].dropna()
    if len(sub) < min_samples:
        warn(f"Node has {len(sub)} < {min_samples} samples — skipped (privacy guard)")
        return {"error": f"insufficient samples (< {min_samples})"}
    return sub


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
    df1: pd.DataFrame, features: list, outcome_column: str, min_samples: int = 5
) -> dict:
    """Ronda 1: momentos locales para estandarizacion global."""
    info("Elastic Net — partial_moments")
    sub = _validate(df1, features, outcome_column, min_samples)
    if isinstance(sub, dict):
        return sub
    X = sub[list(features)].to_numpy(dtype=float)
    return {
        "n": int(len(sub)),
        "sum": X.sum(axis=0).tolist(),
        "sumsq": (X ** 2).sum(axis=0).tolist(),
        "ysum": float(sub[outcome_column].sum()),
    }


@federated
@dataframe(1)
def partial_gram(
    df1: pd.DataFrame, features: list, outcome_column: str,
    mean: list, std: list, ymean: float, min_samples: int = 5
) -> dict:
    """Ronda 2: estadisticos suficientes G = Xs^T Xs, b = Xs^T yc.

    Estandariza con media/std GLOBAL (del central) para que el agregado de G
    entre nodos sea exacto. Solo devuelve agregados; nunca filas individuales.

    NOTA privacidad/escala: G es p×p. Para p grande (~850K CpGs) es inviable y
    podria filtrar covarianzas -> usar solo sobre un subset pre-filtrado.
    """
    info("Elastic Net — partial_gram")
    sub = _validate(df1, features, outcome_column, min_samples)
    if isinstance(sub, dict):
        return sub
    # Aviso de memoria en el nodo (construye una matriz Gram p×p local)
    _check_dimensionality(len(features))
    mean_arr = np.asarray(mean, dtype=float)
    std_arr = np.asarray(std, dtype=float)
    Xs = (sub[list(features)].to_numpy(dtype=float) - mean_arr) / std_arr
    yc = sub[outcome_column].to_numpy(dtype=float) - ymean
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
) -> dict:
    """Coordinador del Elastic Net federado (estadisticos suficientes, 2 rondas)."""
    org_ids = [o["id"] for o in client.organization.list()]
    info(f"Federated Elastic Net | {len(org_ids)} nodos | {len(features)} features")

    # Aviso de coste en memoria (solo lee el nº de features, no toca datos)
    _check_dimensionality(len(features), warn_gb=warn_gram_gb)

    # --- Ronda 1: momentos -> estandarizacion global ---
    task1 = client.task.create(
        method="partial_moments",
        arguments={"features": features, "outcome_column": outcome_column,
                   "min_samples": min_samples},
        organizations=org_ids,
        name="ElasticNet round 1 (moments)",
    )
    r1 = [r for r in client.wait_for_results(task_id=task1.get("id")) if "error" not in r]
    if not r1:
        return {"error": "no valid nodes in round 1"}
    total_n = sum(r["n"] for r in r1)
    mean = _aggregate(r1, "sum") / total_n
    std = np.sqrt(np.maximum(_aggregate(r1, "sumsq") / total_n - mean ** 2, 1e-12))
    ymean = sum(r["ysum"] for r in r1) / total_n

    # --- Ronda 2: estadisticos suficientes G, b ---
    task2 = client.task.create(
        method="partial_gram",
        arguments={"features": features, "outcome_column": outcome_column,
                   "mean": mean.tolist(), "std": std.tolist(), "ymean": ymean,
                   "min_samples": min_samples},
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
        "hyperparams": {"alpha": alpha, "l1_ratio": l1_ratio},
    }
