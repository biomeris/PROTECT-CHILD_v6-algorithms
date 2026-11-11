"""
Federated PCA of DNA methylation — central function.

Quality control and batch check: does the main variation in the methylation
data follow the hospital (batch, pre-analytical or population differences)
or the cohort (biology)?

1. ``probe_moments`` at each hospital: per-CpG sums. The central function
   keeps the CpGs measured in every hospital and picks the ``n_features``
   with the highest pooled variance (or the most variable of a given list).
2. ``partial_scatter``: Z^T Z of those CpGs, centred on the global mean.
   Their sum gives the exact global covariance matrix; its eigenvectors are
   the principal components (as in v6-pca-py).
3. ``partial_scores``: each hospital projects its samples and returns only
   count, sum and sum of squares of each component, for the hospital and
   each cohort with at least ``min_samples`` samples.

From these summaries the result gives, for each component, the share of its
variance explained by hospital and by cohort (R squared, one-way ANOVA), and
by hospital beyond cohort (two-way: cohort and hospital-cohort cells), with
F-test p-values. No sample score or covariance matrix is returned.

Subtasks are created per hospital so that each result is known to belong to
its hospital.
"""

import numpy as np
from scipy import stats

from vantage6.algorithm.tools.util import info, warn
from vantage6.algorithm.decorator.action import central as central_action
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.client import AlgorithmClient

from .federated import decode_array


# p×p float64 per hospital: 2,000 CpGs ≈ 43 MB in base64
MAX_FEATURES = 2000
DEFAULT_FEATURES = 1000
SIGNIFICANCE = 0.05
# A group of one sample would return that sample's own scores
MIN_SAMPLES_FLOOR = 2

VARIANCE_COLUMNS = [
    "component", "explained_variance_ratio", "cumulative_ratio",
    "r2_hospital", "p_hospital", "r2_cohort", "p_cohort",
    "r2_hospital_beyond_cohort", "p_hospital_beyond_cohort",
    "r2_cohort_beyond_hospital", "main_factor", "hospital_effect",
    "separable", "samples_with_cohort",
]
GROUP_COLUMNS = ["organization_id", "cohort", "component", "n", "mean", "sd"]
LOADING_COLUMNS = ["component", "rank", "probe_id", "weight", "correlation"]
HOSPITAL_COLUMNS = [
    "organization_id", "status", "n_samples", "n_cpgs_complete",
    "n_outliers", "suppressed_cohorts", "p_exceeds_n",
]


# ---------------------------------------------------------------------
# SUBTASKS
# ---------------------------------------------------------------------

def _select_organizations(client, organizations_to_include):
    """
    Organization ids of the collaboration, or only those in
    ``organizations_to_include`` (an empty list from the web interface
    means all).
    """

    org_ids = [organization["id"] for organization in client.organization.list()]

    if not organizations_to_include:
        return org_ids

    wanted = [int(org_id) for org_id in organizations_to_include]
    unknown = sorted(set(wanted) - set(org_ids))

    if unknown:
        raise ValueError(
            f"organizations_to_include: {unknown} are not in this "
            f"collaboration ({org_ids})"
        )

    return [org_id for org_id in org_ids if org_id in wanted]


def _run_per_hospital(client, method, org_ids, arguments, name, status):
    """
    One subtask per hospital (created first, then awaited). Returns
    ``{org_id: result}`` for the hospitals without an error; the others get
    the error in ``status``.
    """

    tasks = {}

    for org_id in org_ids:
        task = client.task.create(
            method=method,
            arguments=arguments,
            organizations=[org_id],
            name=f"{name} (organization {org_id})",
        )
        # HQ answers with a message instead of a task when it refuses one
        # (e.g. the dataframe is missing at a node); waiting would never end.
        if not isinstance(task, dict) or task.get("id") is None:
            message = task.get("msg") if isinstance(task, dict) else task
            raise RuntimeError(f"HQ did not create the subtask '{name}': {message}")
        tasks[org_id] = task["id"]

    results = {}

    for org_id, task_id in tasks.items():
        replies = client.wait_for_results(task_id=task_id)
        reply = replies[0] if isinstance(replies, list) and replies else replies
        if not isinstance(reply, dict):
            status[org_id] = f"{method}: no result"
        elif "error" in reply:
            warn(f"organization {org_id}, {method}: {reply['error']}")
            status[org_id] = f"{method}: {reply['error']}"
        else:
            results[org_id] = reply

    return results


# ---------------------------------------------------------------------
# CpG SELECTION
# ---------------------------------------------------------------------

def select_features(moments, n_features):
    """
    Pooled mean and variance of the CpGs measured in every hospital, and
    the ``n_features`` CpGs with the highest variance.

    ``moments``: ``{org_id: probe_moments result}``.
    Returns ``(features, mean, variance, n_total, n_common)``.
    """

    common = None

    for reply in moments.values():
        probes = set(reply["probes"])
        common = probes if common is None else common & probes

    common = sorted(common or [])

    if not common:
        return [], np.array([]), np.array([]), 0, 0

    n_total = 0
    total = np.zeros(len(common))
    total_sq = np.zeros(len(common))

    for reply in moments.values():
        index = {probe: i for i, probe in enumerate(reply["probes"])}
        rows = np.fromiter((index[probe] for probe in common), dtype=int,
                           count=len(common))
        total += decode_array(reply["sum"])[rows]
        total_sq += decode_array(reply["sumsq"])[rows]
        n_total += int(reply["n"])

    mean = total / n_total
    variance = (total_sq - n_total * mean ** 2) / (n_total - 1)

    # Constant CpGs carry no information (and cannot be scaled)
    order = [i for i in np.argsort(-variance, kind="stable") if variance[i] > 1e-12]
    chosen = sorted(order[:n_features], key=lambda i: common[i])

    return (
        [common[i] for i in chosen],
        mean[chosen],
        variance[chosen],
        n_total,
        len(common),
    )


# ---------------------------------------------------------------------
# PCA AND VARIANCE DECOMPOSITION
# ---------------------------------------------------------------------

def principal_components(scatter, n_total, n_components):
    """
    Eigenvalues (variances) and eigenvectors of the covariance matrix
    ``scatter / (n_total - 1)``, largest first. Each component's sign makes
    its largest loading positive, so that results are reproducible.
    """

    covariance = (scatter + scatter.T) / (2.0 * (n_total - 1))
    values, vectors = np.linalg.eigh(covariance)
    order = np.argsort(values)[::-1]
    values = np.maximum(values[order], 0.0)
    vectors = vectors[:, order]

    signs = np.sign(vectors[np.abs(vectors).argmax(axis=0), np.arange(vectors.shape[1])])
    vectors = vectors * np.where(signs == 0, 1.0, signs)

    return values, vectors[:, :n_components], float(np.trace(covariance))


def _between(groups, overall_sum, overall_n):
    """
    Between-group sum of squares from per-group (n, sum) of each component.
    """

    ss = -(overall_sum ** 2) / overall_n

    for n, total in groups:
        ss = ss + total ** 2 / n

    return ss


def _f_test(ss_effect, ss_residual, df_effect, df_residual):

    if df_effect <= 0 or df_residual <= 0:
        return np.full_like(ss_effect, np.nan)

    with np.errstate(divide="ignore", invalid="ignore"):
        f = (ss_effect / df_effect) / (ss_residual / df_residual)

    return np.where(np.isfinite(f), stats.f.sf(f, df_effect, df_residual), np.nan)


def decompose(scores, k, with_cohort):
    """
    Variance of each component explained by hospital and cohort.

    ``scores``: ``{org_id: partial_scores result}``. Returns a dictionary of
    arrays (one value per component).
    """

    totals = [(r["total"]["n"], np.asarray(r["total"]["sum"]),
               np.asarray(r["total"]["sumsq"])) for r in scores.values()]

    n_all = sum(n for n, _, _ in totals)
    sum_all = sum(s for _, s, _ in totals)
    ss_total = sum(q for _, _, q in totals) - sum_all ** 2 / n_all
    ss_hospital = _between([(n, s) for n, s, _ in totals], sum_all, n_all)
    n_hospitals = len(totals)

    out = {
        "r2_hospital": ss_hospital / ss_total,
        "p_hospital": _f_test(ss_hospital, ss_total - ss_hospital,
                              n_hospitals - 1, n_all - n_hospitals),
        "samples_with_cohort": np.full(k, np.nan),
        "separable": False,
    }

    nan = np.full(k, np.nan)

    for key in ("r2_cohort", "p_cohort", "r2_hospital_beyond_cohort",
                "p_hospital_beyond_cohort", "r2_cohort_beyond_hospital"):
        out[key] = nan

    if not with_cohort:
        return out

    # Hospital × cohort cells reported (cohorts with >= min_samples there)
    cells = [
        (org_id, group["cohort"], group["n"], np.asarray(group["sum"]),
         np.asarray(group["sumsq"]))
        for org_id, reply in scores.items()
        for group in reply["groups"]
    ]

    if not cells:
        return out

    n_rep = sum(cell[2] for cell in cells)
    sum_rep = sum(cell[3] for cell in cells)
    ss_rep = sum(cell[4] for cell in cells) - sum_rep ** 2 / n_rep

    def pooled(position):
        keys = sorted({cell[position] for cell in cells}, key=str)
        return [
            (sum(c[2] for c in cells if c[position] == key),
             sum(c[3] for c in cells if c[position] == key))
            for key in keys
        ]

    by_cohort = pooled(1)
    by_hospital = pooled(0)
    ss_cohort = _between(by_cohort, sum_rep, n_rep)
    ss_hosp_rep = _between(by_hospital, sum_rep, n_rep)
    ss_cells = _between([(c[2], c[3]) for c in cells], sum_rep, n_rep)

    n_cohorts, n_cells, n_hosp_rep = len(by_cohort), len(cells), len(by_hospital)

    out.update({
        "r2_cohort": ss_cohort / ss_rep,
        "p_cohort": _f_test(ss_cohort, ss_rep - ss_cohort,
                            n_cohorts - 1, n_rep - n_cohorts),
        "r2_hospital_beyond_cohort": (
            (ss_cells - ss_cohort) / ss_rep if n_cells > n_cohorts else nan
        ),
        "p_hospital_beyond_cohort": _f_test(
            ss_cells - ss_cohort, ss_rep - ss_cells,
            n_cells - n_cohorts, n_rep - n_cells,
        ),
        "r2_cohort_beyond_hospital": (
            (ss_cells - ss_hosp_rep) / ss_rep if n_cells > n_hosp_rep else nan
        ),
        "samples_with_cohort": np.full(k, n_rep / n_all),
        # Hospital and cohort can only be told apart if some cohort is
        # reported at two or more hospitals
        "separable": n_cells > n_cohorts,
    })

    return out


def _main_factor(row, with_cohort):
    """Plain reading of one component."""

    def significant(key):
        value = row[key]
        return value is not None and np.isfinite(value) and value < SIGNIFICANCE

    if not with_cohort:
        return "hospital" if significant("p_hospital") else "neither"

    if not row["separable"]:
        return "not separable (each cohort reported at one hospital)"

    cohort = significant("p_cohort")
    hospital_beyond = significant("p_hospital_beyond_cohort")

    if cohort and hospital_beyond:
        return "cohort and hospital"
    if cohort:
        return "cohort"
    if hospital_beyond or significant("p_hospital"):
        return "hospital"

    return "neither"


def _round(value, digits=6):

    if value is None or not np.isfinite(value):
        return None

    return round(float(value), digits)


# ---------------------------------------------------------------------
# CENTRAL FUNCTION
# ---------------------------------------------------------------------

@central_action
@algorithm_client
def central(
    client: AlgorithmClient,
    n_features: int = DEFAULT_FEATURES,
    features: list | None = None,
    n_components: int = 5,
    scale: bool = False,
    cohort_column: str | None = "cohort",
    cohorts: list | None = None,
    sample_column: str = "sample_label",
    value_column: str = "beta",
    min_samples: int = 5,
    outlier_sd: float = 3.0,
    top_cpgs: int = 10,
    organizations_to_include: list | None = None,
) -> dict:
    """
    Federated PCA of methylation values, for quality control and to check
    whether the main variation follows hospital or cohort.

    Parameters
    ----------
    n_features:
        Number of CpGs used: the most variable over all hospitals among the
        CpGs measured in every hospital (at most 2,000).
    features:
        Optional list of CpGs to choose from (e.g. DMPs); the most variable
        ``n_features`` of them are used.
    n_components:
        Number of components reported.
    scale:
        False: PCA of the covariance (usual for beta values). True: each CpG
        divided by its SD (correlation PCA).
    cohort_column:
        Per-sample cohort column; empty for a PCA without cohorts.
    cohorts:
        Optional cohorts to include (default all).
    min_samples:
        Minimum samples a hospital, and a cohort at a hospital, needs before
        any summary of it is returned.
    outlier_sd:
        A sample further than this many SDs from the global mean on any
        reported component is counted as an outlier at its hospital.
    top_cpgs:
        CpGs with the largest loadings listed per component.
    """

    cohort_column = cohort_column or None
    with_cohort = cohort_column is not None
    n_features = int(n_features or DEFAULT_FEATURES)
    n_components = int(n_components or 5)

    if not 2 <= n_features <= MAX_FEATURES:
        return {"error": f"n_features must be between 2 and {MAX_FEATURES}"}
    if n_components < 1:
        return {"error": "n_components must be at least 1"}
    if int(min_samples) < MIN_SAMPLES_FLOOR:
        return {
            "error": (
                f"min_samples must be at least {MIN_SAMPLES_FLOOR}: with 1, the "
                "summaries of a group of one sample are that sample's scores"
            )
        }

    org_ids = _select_organizations(client, organizations_to_include)

    if not org_ids:
        return {"error": "No organizations available."}

    info(f"PCA methylation on {len(org_ids)} organizations")

    data_args = {
        "cohort_column": cohort_column,
        "cohorts": cohorts or None,
        "sample_column": sample_column,
        "value_column": value_column,
        "min_samples": int(min_samples),
    }
    status = {}

    # Round 1: CpGs measured everywhere, pooled variance, selection
    moments = _run_per_hospital(
        client, "probe_moments", org_ids,
        {**data_args, "features": features or None},
        "PCA: CpG moments", status,
    )

    if not moments:
        return {"error": "No hospital returned data.", "hospitals": _hospital_rows(org_ids, status, {}, {}, {})}

    chosen, mean, variance, n_total, n_common = select_features(moments, n_features)

    if len(chosen) < 2:
        return {"error": f"Only {len(chosen)} variable CpG(s) are measured in every hospital."}

    info(f"{n_common} CpGs in every hospital; {len(chosen)} most variable used")

    sd = np.sqrt(variance)
    model_args = {
        **data_args,
        "features": chosen,
        "mean": mean.tolist(),
        "scale": sd.tolist() if scale else None,
    }

    # Round 2: exact covariance
    scatters = _run_per_hospital(
        client, "partial_scatter", list(moments), model_args,
        "PCA: covariance", status,
    )

    if set(scatters) != set(moments):
        return {
            "error": "A hospital failed while computing the covariance.",
            "hospitals": _hospital_rows(org_ids, status, moments, {}, {}),
        }

    scatter = sum(decode_array(reply["scatter"]) for reply in scatters.values())
    n_scatter = sum(int(reply["n"]) for reply in scatters.values())
    values, vectors, total_variance = principal_components(scatter, n_scatter, len(chosen))
    k = min(n_components, len(chosen), n_scatter - 1)
    ratios = values / total_variance if total_variance > 0 else np.zeros_like(values)

    # Round 3: summaries of the samples' scores
    scores = _run_per_hospital(
        client, "partial_scores", list(scatters),
        {
            **model_args,
            "components": vectors[:, :k].tolist(),
            "sd_components": np.sqrt(values[:k]).tolist(),
            "outlier_sd": float(outlier_sd),
        },
        "PCA: scores", status,
    )

    if not scores:
        return {"error": "No hospital returned component scores.",
                "hospitals": _hospital_rows(org_ids, status, moments, scatters, {})}

    names = [f"PC{j + 1}" for j in range(k)]
    effects = decompose(scores, k, with_cohort)

    variance_rows = []

    for j, name in enumerate(names):
        row = {
            "component": name,
            "explained_variance_ratio": _round(ratios[j]),
            "cumulative_ratio": _round(ratios[: j + 1].sum()),
        }
        for key in ("r2_hospital", "p_hospital", "r2_cohort", "p_cohort",
                    "r2_hospital_beyond_cohort", "p_hospital_beyond_cohort",
                    "r2_cohort_beyond_hospital", "samples_with_cohort"):
            row[key] = _round(effects[key][j])
        raw = {key: effects[key][j] for key in effects if key != "separable"}
        raw["separable"] = effects["separable"]
        row["main_factor"] = _main_factor(raw, with_cohort)
        p_batch = raw["p_hospital_beyond_cohort"] if with_cohort else raw["p_hospital"]
        row["hospital_effect"] = bool(np.isfinite(p_batch) and p_batch < SIGNIFICANCE)
        row["separable"] = bool(effects["separable"]) if with_cohort else None
        variance_rows.append({column: row[column] for column in VARIANCE_COLUMNS})

    group_rows = []

    for org_id, reply in scores.items():
        blocks = [("all", reply["total"])] + [(g["cohort"], g) for g in reply["groups"]]
        for cohort, block in blocks:
            n = block["n"]
            sums, sumsq = np.asarray(block["sum"]), np.asarray(block["sumsq"])
            means = sums / n
            sds = np.sqrt(np.maximum(sumsq - n * means ** 2, 0) / (n - 1)) if n > 1 else np.full(k, np.nan)
            for j, name in enumerate(names):
                group_rows.append({
                    "organization_id": org_id, "cohort": cohort, "component": name,
                    "n": n, "mean": _round(means[j]), "sd": _round(sds[j]),
                })

    # weight: the CpG's coefficient in the (unit-length) component.
    # correlation: correlation between the CpG and the component's scores,
    # weight * sqrt(eigenvalue) / SD of the CpG (in the PCA's units).
    cpg_sd = np.ones(len(chosen)) if scale else sd
    loading_rows = []

    for j, name in enumerate(names):
        order = np.argsort(-np.abs(vectors[:, j]), kind="stable")[: int(top_cpgs)]
        for rank, i in enumerate(order, start=1):
            loading_rows.append({
                "component": name, "rank": rank, "probe_id": chosen[i],
                "weight": _round(vectors[i, j]),
                "correlation": _round(vectors[i, j] * np.sqrt(values[j]) / cpg_sd[i]),
            })

    hospitals = _hospital_rows(org_ids, status, moments, scatters, scores)
    batch_components = [r["component"] for r in variance_rows if r["hospital_effect"]]
    n_with_cohort = sum(g["n"] for reply in scores.values() for g in reply["groups"])
    p_exceeds_n = any(reply.get("p_exceeds_n") for reply in scatters.values())

    notes = [
        f"PCA of {len(chosen)} CpGs ({value_column}, "
        f"{'correlation' if scale else 'covariance'}) over {n_scatter} samples "
        f"from {len(scores)} hospital(s); {n_common} CpGs were measured in every hospital.",
        f"The first {k} components explain {ratios[:k].sum():.1%} of the variance of these CpGs.",
    ]

    if batch_components:
        notes.append(
            "Hospital effect"
            + (" beyond cohort" if with_cohort else "")
            + f" (p < {SIGNIFICANCE}) on {', '.join(batch_components)}: check processing "
            "batches, sample handling and population differences between hospitals."
        )
    else:
        notes.append(f"No component shows a hospital effect (p < {SIGNIFICANCE}).")

    if with_cohort and not effects["separable"]:
        notes.append(
            "Hospital and cohort cannot be separated: every reported cohort is at a single "
            "hospital (cohorts with fewer than min_samples samples at a hospital are not "
            "reported), so the cohort R2 is also a comparison of hospitals. Lower "
            "min_samples, if allowed, or add samples."
        )
    elif with_cohort:
        missing = 1 - n_with_cohort / n_scatter
        if missing > 0:
            notes.append(
                f"{missing:.0%} of the samples are in cohorts too small to report at their "
                "hospital and are left out of the cohort comparisons."
            )
    if p_exceeds_n:
        notes.append(
            "At some hospital the number of CpGs is at least the number of samples: "
            "the shared covariance carries much information about each sample."
        )
    n_outliers = sum(reply["n_outliers"] for reply in scores.values())
    if n_outliers:
        notes.append(
            f"{n_outliers} sample(s) lie beyond {outlier_sd} SD on a component; "
            "the hospitals can look at them locally."
        )

    return {
        "summary": " ".join(notes),
        "variance_explained": variance_rows,
        "group_scores": group_rows,
        "top_loadings": loading_rows,
        "hospitals": hospitals,
        "input": [{
            "n_samples": n_scatter,
            "n_hospitals": len(scores),
            "n_cpgs_common": n_common,
            "n_cpgs_used": len(chosen),
            "cpg_source": "given list" if features else "most variable",
            "value_column": value_column,
            "scaled": bool(scale),
            "min_samples": int(min_samples),
        }],
        # For reuse (e.g. projecting new samples); aggregates only
        "pca_model": {
            "features": chosen,
            "mean": mean.tolist(),
            "scale": sd.tolist() if scale else None,
            "components": vectors[:, :k].tolist(),
            "explained_variance": values[:k].tolist(),
            "explained_variance_ratio": ratios[:k].tolist(),
        },
    }


def _hospital_rows(org_ids, status, moments, scatters, scores):

    rows = []

    for org_id in org_ids:
        reply = scores.get(org_id, {})
        rows.append({
            "organization_id": org_id,
            "status": status.get(org_id, "included" if org_id in scores else "not reached"),
            "n_samples": moments[org_id]["n"] if org_id in moments else None,
            "n_cpgs_complete": len(moments[org_id]["probes"]) if org_id in moments else None,
            "n_outliers": reply.get("n_outliers"),
            "suppressed_cohorts": ", ".join(reply.get("suppressed", [])),
            "p_exceeds_n": scatters.get(org_id, {}).get("p_exceeds_n"),
        })

    return rows
