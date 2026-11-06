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

Outcome: numerico (p.ej. edad), o una columna de texto (p.ej. la cohorte) con
`positive_value`: y = 1 si el valor es `positive_value`, 0 si no (junto con
`cohorts` para comparar dos cohortes).

CpGs disponibles: tras el control de calidad cada hospital conserva un conjunto
de sondas algo distinto. En la ronda 1 cada nodo indica que features tiene sin
valores ausentes; el modelo usa solo las disponibles en todos los nodos validos
y el resultado lista las excluidas (con en cuantos nodos estaban).

Tamaño: la matriz de Gram (p×p float64) viaja en binario (base64); p esta
limitado a MAX_FEATURES para no superar el limite de 1 GB por resultado de
vantage6. Las CpGs deben venir pre-seleccionadas (p.ej. top DMPs de v6-dmp-dmr-py).

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

import base64
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
# p×p float64 en base64: 2,000 features ≈ 43 MB por nodo
MAX_FEATURES = 2000


def _encode_array(a: np.ndarray) -> dict:
    a = np.ascontiguousarray(a, dtype="<f8")
    return {"shape": list(a.shape), "data": base64.b64encode(a.tobytes()).decode("ascii")}


def _decode_array(d: dict) -> np.ndarray:
    return np.frombuffer(base64.b64decode(d["data"]), dtype="<f8").reshape(d["shape"])


def _to_wide(df: pd.DataFrame, features: list, outcome_column: str,
             cohort_column: str | None, sample_column: str, value_column: str) -> Any:
    """Formato largo -> ancho (una fila por muestra) solo para `features`."""
    per_sample = list(dict.fromkeys([outcome_column] + ([cohort_column] if cohort_column else [])))
    needed = ["probe_id", sample_column, value_column] + per_sample
    missing = [c for c in needed if c not in df.columns]
    if missing:
        return {"error": f"missing columns: {missing}"}

    long = df[df["probe_id"].isin(features)]
    wide = long.pivot(index=sample_column, columns="probe_id", values=value_column)
    wide = wide.reindex(columns=list(features))

    meta = df[[sample_column] + per_sample].drop_duplicates()
    if meta[sample_column].duplicated().any():
        return {"error": "outcome/cohort values differ between rows of the same sample"}
    return wide.join(meta.set_index(sample_column), how="left").reset_index(drop=True)


def _prepare(df: pd.DataFrame, features: list, outcome_column: str,
             min_samples: int, center_by: str = "global",
             cohort_column: str | None = None, cohorts: list | None = None,
             sample_column: str = "sample_label", value_column: str = "beta",
             positive_value: str | None = None) -> Any:
    """(subset ancho con columnas `_group` e `_y`, features disponibles), o dict de error.

    `_y` es el outcome numerico (0/1 con `positive_value`). Una feature esta
    disponible si no tiene valores ausentes en las muestras del nodo.

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

    per_sample = list(dict.fromkeys([outcome_column] + ([cohort_column] if cohort_column else [])))
    missing = [c for c in per_sample if c not in df.columns]
    if missing:
        warn(f"Missing columns: {missing}")
        return {"error": f"missing columns: {missing}"}
    # Features ausentes en este nodo: columnas vacias (se excluyen, no se aborta)
    df = df.reindex(columns=list(dict.fromkeys(list(features) + per_sample)))
    sub = df.dropna(subset=per_sample)

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

    if positive_value is not None:
        y = (sub[outcome_column].astype(str) == str(positive_value)).astype(float)
    else:
        y = pd.to_numeric(sub[outcome_column], errors="coerce")
        if y.isna().any():
            return {"error": f"outcome column '{outcome_column}' is not numeric; "
                             "give positive_value (e.g. a cohort) to use it as 0/1"}
    sub = sub.assign(_y=y)

    if len(sub) < min_samples:
        warn(f"Node has {len(sub)} < {min_samples} samples — skipped (privacy guard)")
        return {"error": f"insufficient samples (< {min_samples})"}
    available = [f for f in features if sub[f].notna().all()]
    return sub, available


def _center_within_groups(sub: pd.DataFrame, features: list):
    """X e y (`_y`) centrados con la media de su grupo (nodo o nodo×cohorte)."""
    X = sub[list(features)].astype(float)
    y = sub["_y"]
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
    value_column: str = "beta", positive_value: str | None = None,
) -> dict:
    """Ronda 1: features disponibles y momentos locales para la estandarizacion.

    Devuelve, para las features disponibles en el nodo (`available`), sumas
    globales (modo "global") y la suma de cuadrados intra-grupo `ss_within`
    (modos centrados por grupo).
    """
    info("Elastic Net — partial_moments")
    prepared = _prepare(df1, features, outcome_column, min_samples, center_by,
                        cohort_column, cohorts, sample_column, value_column, positive_value)
    if isinstance(prepared, dict):
        return prepared
    sub, available = prepared
    info(f"{len(sub)} samples; {len(available)}/{len(features)} features available")
    X = sub[available].to_numpy(dtype=float)
    Xc, _ = _center_within_groups(sub, available)
    return {
        "n": int(len(sub)),
        "n_groups": int(sub["_group"].nunique()),
        "available": available,
        "sum": X.sum(axis=0).tolist(),
        "sumsq": (X ** 2).sum(axis=0).tolist(),
        "ss_within": (Xc ** 2).sum(axis=0).tolist(),
        "ysum": float(sub["_y"].sum()),
    }


@federated
@dataframe(1)
def partial_gram(
    df1: pd.DataFrame, features: list, outcome_column: str,
    mean: list, std: list, ymean: float, min_samples: int = 5,
    center_by: str = "global", cohort_column: str | None = None,
    cohorts: list | None = None, sample_column: str = "sample_label",
    value_column: str = "beta", positive_value: str | None = None,
) -> dict:
    """Ronda 2: estadisticos suficientes G = Xs^T Xs, b = Xs^T yc.

    `features` son las disponibles en todos los nodos (decididas en la ronda 1).
    G viaja en binario (base64, float64).

    Estandariza con media/std GLOBAL (del central) para que el agregado de G
    entre nodos sea exacto. Solo devuelve agregados; nunca filas individuales.

    NOTA privacidad/escala: G es p×p. Para p grande (~850K CpGs) es inviable y
    podria filtrar covarianzas -> usar solo sobre un subset pre-filtrado.
    """
    info("Elastic Net — partial_gram")
    prepared = _prepare(df1, features, outcome_column, min_samples, center_by,
                        cohort_column, cohorts, sample_column, value_column, positive_value)
    if isinstance(prepared, dict):
        return prepared
    sub, available = prepared
    if len(available) < len(features):
        return {"error": f"{len(features) - len(available)} features are no longer available on this node"}
    # Aviso de memoria en el nodo (construye una matriz Gram p×p local)
    _check_dimensionality(len(features))
    p_exceeds_n = len(features) >= len(sub)
    if p_exceeds_n:
        warn(f"p={len(features)} features >= n={len(sub)} samples on this node: the "
             "shared Gram matrix carries a lot of information about each patient. "
             "Use fewer features than patients per node.")
    std_arr = np.asarray(std, dtype=float)
    if center_by == "global":
        mean_arr = np.asarray(mean, dtype=float)
        Xs = (sub[list(features)].to_numpy(dtype=float) - mean_arr) / std_arr
        yc = sub["_y"].to_numpy(dtype=float) - ymean
    else:
        Xc, yc = _center_within_groups(sub, features)
        Xs = Xc / std_arr
    return {"G": _encode_array(Xs.T @ Xs), "b": (Xs.T @ yc).tolist(),
            "n": int(len(sub)), "p_exceeds_n": p_exceeds_n}


# --------------------------------------------------------------------------
# Helpers del central: nodos, subtareas, CpGs candidatas de otra tarea
# --------------------------------------------------------------------------
def select_organizations(client: AlgorithmClient, organizations_to_include: list | None) -> list:
    """Ids de las organizaciones de la colaboracion, o solo las de `organizations_to_include`."""
    org_ids = [o["id"] for o in client.organization.list()]
    if not organizations_to_include:
        return org_ids
    wanted = [int(o) for o in organizations_to_include]
    unknown = sorted(set(wanted) - set(org_ids))
    if unknown:
        raise ValueError(f"organizations_to_include: {unknown} are not in this collaboration ({org_ids})")
    return [o for o in org_ids if o in wanted]


def check_subtask(task: Any, name: str) -> int:
    """Id de una subtarea creada. HQ responde con un mensaje en vez de una tarea cuando
    la rechaza (p.ej. el dataframe no existe en un nodo); esperarla no terminaria nunca."""
    if not isinstance(task, dict) or task.get("id") is None:
        message = task.get("msg") if isinstance(task, dict) else task
        raise RuntimeError(f"HQ did not create the subtask '{name}': {message}")
    return task["id"]


def latest_task(client, accepts, image_hints, label: str, limit: int = 50) -> int:
    """Id of the most recent finished task of this session whose result `accepts`.

    Lists the session's finished top-level tasks, newest first, and checks their
    results; tasks whose image contains one of `image_hints` are checked first, to
    avoid reading large results of other analyses.
    """
    session_id = getattr(client, "session_id", None)
    if session_id is None or not hasattr(client, "request"):
        raise ValueError(f"No session to look in: give the {label} task number")
    # No status filter: HQ 5.0.3 fails on it (status is computed from the runs).
    # Unfinished tasks have no result yet and are skipped below.
    response = client.request("task", params={
        "session_id": session_id, "is_user_created": 1, "per_page": limit})
    if not isinstance(response, dict) or "data" not in response:
        message = response.get("msg") if isinstance(response, dict) else response
        raise ValueError(f"The tasks of this session could not be listed ({message}): "
                         f"give the {label} task number")
    tasks = response["data"]
    tasks = sorted(tasks, key=lambda t: (not any(h in str(t.get("image", "")) for h in image_hints),
                                         -int(t["id"])))
    for task in tasks:
        results = [r for r in client.result.from_task(int(task["id"])) if r is not None]
        if results and accepts(results[0]):
            return int(task["id"])
    raise ValueError(f"No finished {label} task in this session: run {label} first in this session, "
                     "or give a task number")


def is_candidates_result(result: Any) -> bool:
    """Un resultado de DMP/DMR o de Beta y M por cohorte."""
    if isinstance(result, dict):
        return "global_dmps" in result
    return (isinstance(result, list) and bool(result) and isinstance(result[0], dict)
            and {"probe_id", "cohort", "beta_mean_global"} <= set(result[0]))


def task_result(client: AlgorithmClient, task_id: int) -> tuple:
    """(tarea, resultado) de una tarea terminada de la colaboracion, o ValueError."""
    task = client.task.get(int(task_id))
    # Un cliente real devuelve la tarea o el mensaje de HQ; la red simulada de los
    # tests no tiene detalles de tareas (None)
    if task is None:
        task = {"id": int(task_id), "name": None}
    elif not isinstance(task, dict) or task.get("id") is None:
        message = task.get("msg") if isinstance(task, dict) else task
        raise ValueError(f"Task {task_id} could not be read: {message}")
    results = [r for r in client.result.from_task(int(task_id)) if r is not None]
    if not results:
        raise ValueError(f"Task {task_id} ('{task.get('name')}') has no result yet: "
                         "is it finished and did it succeed?")
    return task, results[0]


def candidates_from_task(client: AlgorithmClient, task_id: int, n_candidates: int,
                         cohorts: list | None) -> tuple:
    """CpGs candidatas leidas del resultado de otra tarea, y de donde vienen.

    - DMP/DMR (v6-dmp-dmr-py): los `n_candidates` DMPs de menor p-valor presentes en
      todos los hospitales.
    - Beta y M por cohorte (v6-betavalues-py): las sondas con cobertura completa en
      las cohortes elegidas y mayor diferencia de Beta medio entre ellas.
    """
    task, result = task_result(client, task_id)
    name = task.get("name")
    if isinstance(result, dict) and "global_dmps" in result:
        dmps = pd.DataFrame(result["global_dmps"])
        if dmps.empty:
            raise ValueError(f"Task {task_id} ('{name}') has no DMPs")
        everywhere = dmps[dmps["n_nodes"] == dmps["n_nodes"].max()].sort_values("p_value")
        features = everywhere["probe_id"].head(n_candidates).tolist()
        comparison = result.get("comparison", {})
        contrast = f"{comparison.get('cohort_b')} vs {comparison.get('cohort_a')}"
        description = (f"the {len(features)} top DMPs found in all hospitals of task {task_id} "
                       f"'{name}' (DMP/DMR, {contrast})")
        if cohorts and set(cohorts) != {comparison.get("cohort_a"), comparison.get("cohort_b")}:
            warn(f"The DMP/DMR task compared {contrast}, not {', '.join(map(str, cohorts))}")
        return features, {"source": "task", "task_id": int(task_id), "task_name": name,
                          "algorithm": "DMP/DMR", "description": description}
    rows = result if isinstance(result, list) else None
    if rows and isinstance(rows[0], dict) and {"probe_id", "cohort", "beta_mean_global"} <= set(rows[0]):
        means = pd.DataFrame(rows)
        chosen = list(cohorts) if cohorts else sorted(means["cohort"].unique())
        means = means[means["cohort"].isin(chosen)]
        full = means.groupby("probe_id").filter(
            lambda g: len(g) == len(chosen) and (g["coverage"] >= 1).all())
        spread = full.groupby("probe_id")["beta_mean_global"].agg(lambda b: b.max() - b.min())
        features = spread.sort_values(ascending=False).head(n_candidates).index.tolist()
        description = (f"the {len(features)} probes with the largest difference in mean Beta "
                       f"between {', '.join(map(str, chosen))} (full coverage) of task {task_id} "
                       f"'{name}' (Beta and M values per cohort)")
        return features, {"source": "task", "task_id": int(task_id), "task_name": name,
                          "algorithm": "Beta and M values per cohort", "description": description}
    raise ValueError(f"Task {task_id} ('{name}') is not a DMP/DMR or Beta and M values result")


# --------------------------------------------------------------------------
# Funcion CENTRAL (orquesta, NO ve datos)
# --------------------------------------------------------------------------
@central
@algorithm_client
def central(
    client: AlgorithmClient,
    outcome_column: str,
    features: list | None = None,
    candidates_task_id: int | None = None,
    n_candidates: int = 100,
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
    positive_value: str | None = None,
    organizations_to_include: list | None = None,
) -> dict:
    """Coordinador del Elastic Net federado (estadisticos suficientes, 2 rondas).

    CpGs candidatas: `features` (lista, como mucho MAX_FEATURES) o `candidates_task_id`:
    una tarea DMP/DMR o Beta y M por cohorte terminada, de la que se toman
    `n_candidates` CpGs (ver candidates_from_task). Sin ninguno de los dos, la tarea
    DMP/DMR o Beta y M terminada mas reciente de la sesion. El resultado indica de
    donde vienen (`input`).
    outcome_column/positive_value: outcome numerico, o 0/1 = (valor == positive_value).
    Con `cohorts` y sin positive_value: dos cohortes -> la segunda es la positiva;
    tres o mas -> un modelo por cohorte frente a las demas (one-vs-rest) y la union de
    las CpGs seleccionadas.
    center_by: "hospital" (defecto, intercepto por hospital: elimina batch entre
    hospitales), "hospital_cohort" (intercepto por hospital×cohorte) o "global".
    cohort_column/cohorts: columna de cohorte y, opcionalmente, cohortes a incluir.
    sample_column/value_column: solo para entrada en formato largo.
    organizations_to_include: hospitales a usar; todos si no se indica.
    """
    if center_by not in CENTER_MODES:
        return {"error": f"center_by must be one of {CENTER_MODES}"}
    # La interfaz web envia los campos opcionales en blanco como listas vacias
    cohorts = list(cohorts) if cohorts else None
    if features and candidates_task_id not in (None, ""):
        return {"error": "Give either features or candidates_task_id, not both"}
    if candidates_task_id not in (None, ""):
        features, provenance = candidates_from_task(client, candidates_task_id, int(n_candidates), cohorts)
    elif features:
        provenance = {"source": "list", "description": f"a given list of {len(features)} CpGs"}
    else:
        # Ni lista ni tarea: la tarea DMP/DMR o Beta y M mas reciente de la sesion
        try:
            task_id = latest_task(client, is_candidates_result, ("dmp-dmr", "betavalues"),
                                  "DMP/DMR or Beta and M values")
        except ValueError as exc:
            return {"error": f"Give the candidate CpGs: {exc}"}
        features, provenance = candidates_from_task(client, task_id, int(n_candidates), cohorts)
        provenance["selected"] = "automatically"
        provenance["description"] = (f"Automatically selected the most recent {provenance['algorithm']} "
                                     f"task in this session (task {task_id}): " + provenance["description"])
    info(f"Input: {provenance['description']}")
    features = list(dict.fromkeys(features))
    if not features or len(features) > MAX_FEATURES:
        return {"error": f"give between 1 and {MAX_FEATURES:,} features (got {len(features):,}); "
                         "pre-select CpGs, e.g. the top DMPs of v6-dmp-dmr-py"}
    org_ids = select_organizations(client, organizations_to_include)
    info(f"Federated Elastic Net | {len(org_ids)} nodos | {len(features)} features "
         f"| center_by={center_by}")
    settings = dict(alpha=alpha, l1_ratio=l1_ratio, max_iter=max_iter, tol=tol,
                    warn_gram_gb=warn_gram_gb, center_by=center_by)

    def data_args(positive):
        return {"outcome_column": outcome_column,
                "min_samples": min_samples, "center_by": center_by,
                "cohort_column": cohort_column, "cohorts": cohorts,
                "sample_column": sample_column, "value_column": value_column,
                "positive_value": positive}

    if positive_value in (None, "") and cohorts and len(cohorts) == 2:
        positive_value = cohorts[1]
        info(f"Two cohorts and no positive value: {positive_value} is the positive cohort")
    if positive_value in (None, "") and cohorts and len(cohorts) >= 3:
        # One-vs-rest: un modelo por cohorte frente a las demas
        per_cohort = {}
        for positive in cohorts:
            info(f"One-vs-rest: {positive} vs the other cohorts")
            per_cohort[str(positive)] = _fit(client, org_ids, features, data_args(positive), **settings)
        failed = {c: r["error"] for c, r in per_cohort.items() if "error" in r}
        if failed:
            return {"error": f"One-vs-rest failed for {failed}", "input": provenance}
        union = {}
        for c, r in per_cohort.items():
            for s in r["selected"]:
                entry = union.setdefault(s["feature"], {"feature": s["feature"], "cohorts": [],
                                                        "max_abs_coefficient": 0.0})
                entry["cohorts"].append(c)
                entry["max_abs_coefficient"] = max(entry["max_abs_coefficient"], abs(s["coefficient"]))
        selected = sorted(union.values(), key=lambda e: -e["max_abs_coefficient"])
        for e in selected:
            e["cohorts"] = ", ".join(e["cohorts"])
        return {
            "input": provenance,
            "one_vs_rest": True,
            "cohorts": cohorts,
            "selected": selected,
            "selected_cpgs": [e["feature"] for e in selected],
            "per_cohort": per_cohort,
            "n_features_requested": len(features),
            "skipped": sorted({s for r in per_cohort.values() for s in r["skipped"]}),
            "privacy_warnings": sorted({w for r in per_cohort.values() for w in r["privacy_warnings"]}),
            "hyperparams": {"alpha": alpha, "l1_ratio": l1_ratio},
        }

    result = _fit(client, org_ids, features, data_args(positive_value), **settings)
    return {"input": provenance, **result}


def _fit(client: AlgorithmClient, org_ids: list, features: list, data_args: dict,
         alpha: float, l1_ratio: float, max_iter: int, tol: float, warn_gram_gb: float,
         center_by: str) -> dict:
    """Un Elastic Net federado (2 rondas) para un outcome."""
    # Aviso de coste en memoria (solo lee el nº de features, no toca datos)
    _check_dimensionality(len(features), warn_gb=warn_gram_gb)

    # --- Ronda 1: features disponibles y momentos -> estandarizacion global ---
    task1 = client.task.create(
        method="partial_moments",
        arguments={**data_args, "features": features},
        organizations=org_ids,
        name="ElasticNet round 1 (moments)",
    )
    results1 = client.wait_for_results(task_id=check_subtask(task1, "ElasticNet round 1 (moments)"))
    skipped = [r["error"] for r in results1 if "error" in r]
    r1 = [r for r in results1 if "error" not in r]
    if not r1:
        return {"error": "no valid nodes in round 1", "skipped": skipped}

    # Solo las features disponibles en todos los nodos validos
    n_available = {f: sum(f in set(r["available"]) for r in r1) for f in features}
    common = [f for f in features if n_available[f] == len(r1)]
    excluded = [{"feature": f, "n_nodes_available": n_available[f]}
                for f in features if n_available[f] < len(r1)]
    if excluded:
        warn(f"{len(excluded)} features not available in every node are excluded")
    if not common:
        return {"error": "no feature is available in every node", "excluded_features": excluded,
                "skipped": skipped}

    def stack(key: str) -> np.ndarray:
        """Suma sobre nodos de un estadistico por feature, en el orden de `common`."""
        total = np.zeros(len(common))
        for r in r1:
            index = {f: i for i, f in enumerate(r["available"])}
            total += np.asarray(r[key], dtype=float)[[index[f] for f in common]]
        return total

    total_n = sum(r["n"] for r in r1)
    mean = stack("sum") / total_n
    if center_by == "global":
        var = stack("sumsq") / total_n - mean ** 2
    else:
        var = stack("ss_within") / total_n   # varianza intra-grupo agregada
    std = np.sqrt(np.maximum(var, 1e-12))
    ymean = sum(r["ysum"] for r in r1) / total_n

    # --- Ronda 2: estadisticos suficientes G, b ---
    task2 = client.task.create(
        method="partial_gram",
        arguments={**data_args, "features": common, "mean": mean.tolist(),
                   "std": std.tolist(), "ymean": ymean},
        organizations=org_ids,
        name="ElasticNet round 2 (gram)",
    )
    results2 = client.wait_for_results(task_id=check_subtask(task2, "ElasticNet round 2 (gram)"))
    r2 = [r for r in results2 if "error" not in r]
    if not r2:
        return {"error": "no valid nodes in round 2", "skipped": skipped}
    if len(r2) < len(r1):
        warn(f"Round 2: {len(r1) - len(r2)} nodos cayeron entre rondas")
        skipped += [r["error"] for r in results2 if "error" in r]
    # n de los nodos que aportan G (puede ser menor si alguno cayo)
    total_n = sum(r["n"] for r in r2)

    # --- Central resuelve el descent sobre los agregados ---
    G = np.sum([_decode_array(r["G"]) for r in r2], axis=0)
    b = _aggregate(r2, "b")
    w = _coordinate_descent_gram(G, b, total_n, alpha, l1_ratio, max_iter, tol)

    selected = sorted(
        ({"feature": f, "coefficient": float(wj)} for f, wj in zip(common, w) if abs(wj) > 1e-6),
        key=lambda s: -abs(s["coefficient"]),
    )
    info(f"Elastic Net done | {len(selected)}/{len(common)} CpGs seleccionadas")
    privacy_warnings = []
    n_p_exceeds_n = sum(r["p_exceeds_n"] for r in r2)
    if n_p_exceeds_n:
        privacy_warnings.append(
            f"In {n_p_exceeds_n} of {len(r2)} nodes there are at least as many features "
            f"({len(common)}) as samples: the shared Gram matrix reveals much about each patient")
    return {
        "selected": selected,
        "selected_cpgs": [s["feature"] for s in selected],
        "coefficients": w.tolist(),
        "features": common,
        "n_features_requested": len(features),
        "excluded_features": excluded,
        "n_total": int(total_n),
        "n_nodes": len(r2),
        "n_groups": int(sum(r["n_groups"] for r in r1)),
        "nodes_skipped": len(org_ids) - len(r2),
        "skipped": skipped,
        "privacy_warnings": privacy_warnings,
        "center_by": center_by,
        "cohorts": list(data_args["cohorts"]) if data_args["cohorts"] else None,
        "outcome": {"column": data_args["outcome_column"],
                    "positive_value": data_args["positive_value"]},
        "hyperparams": {"alpha": alpha, "l1_ratio": l1_ratio},
    }
