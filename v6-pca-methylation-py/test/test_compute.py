"""
Tests of the federated methylation PCA with the vantage6 MockNetwork.

Run as:

    python test/test_compute.py

Synthetic long data (probe_id, sample_label, cohort, beta) for three
hospitals. Hospital 3 has a shift on some CpGs (a batch effect) in the
"batch" scenario. Every check compares the federated result with the same
computation on the pooled data.
"""

import contextlib
import io
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

current_path = Path(__file__).parent
sys.path.insert(0, str(current_path.parent))
os.chdir(current_path.parent)

from vantage6.algorithm.mock.network import MockNetwork  # noqa: E402

from pca_methylation.central import (  # noqa: E402
    GROUP_COLUMNS, HOSPITAL_COLUMNS, LOADING_COLUMNS, VARIANCE_COLUMNS,
)

N_PROBES = 300
# (cohort, samples) per hospital
LAYOUT = [
    [("case", 8), ("control", 6)],
    [("case", 5), ("control", 9), ("rare", 2)],   # "rare" is below min_samples
    [("case", 7), ("control", 7)],
]
MIN_SAMPLES = 3


def close(actual, expected):
    """Results are rounded to 6 decimals."""
    return np.allclose(np.asarray(actual, dtype=float), expected, atol=2e-6)


def check(condition, message):
    if not condition:
        raise AssertionError(message)
    print(f"  ok  {message}")


def make_nodes(batch_shift=0.0, seed=1):
    """
    Long data per hospital. CpGs 0-19 differ between cohorts; with
    ``batch_shift``, CpGs 20-39 are shifted at hospital 3.
    """

    rng = np.random.default_rng(seed)
    probes = [f"cg{i:08d}_BC11" for i in range(N_PROBES)]
    base = rng.uniform(0.1, 0.9, N_PROBES)
    nodes, wide = [], []

    for h, layout in enumerate(LAYOUT):
        rows = []
        for cohort, n in layout:
            for s in range(n):
                beta = base + rng.normal(0, 0.03, N_PROBES)
                if cohort == "case":
                    beta[:20] += 0.12
                if h == 2:
                    beta[20:40] += batch_shift
                label = f"h{h + 1}_{cohort}_{s}"
                rows.append((label, cohort, np.clip(beta, 0.01, 0.99)))
        long = pd.DataFrame([
            {"probe_id": probe, "sample_label": label, "cohort": cohort,
             "beta": value, "m_value": np.log2(value / (1 - value))}
            for label, cohort, beta in rows
            for probe, value in zip(probes, beta)
        ])
        nodes.append(long)
        wide.append(pd.DataFrame(
            [beta for _, _, beta in rows], columns=probes,
            index=pd.MultiIndex.from_tuples(
                [(h, cohort, label) for label, cohort, _ in rows],
                names=["hospital", "cohort", "sample"]),
        ))

    return nodes, pd.concat(wide)


def run(nodes, **arguments):
    network = MockNetwork(
        datasets=[{"default": {"database": df}} for df in nodes],
        module_name="pca_methylation",
    )
    client = network.user_client
    org_ids = [o["id"] for o in client.organization.list()]
    # The mock runs the task when it is created: its log is hidden
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        task = client.task.create(
            method="central",
            arguments={"min_samples": MIN_SAMPLES, **arguments},
            organizations=[org_ids[0]],
            databases=[{"type": "dataframe", "dataframe_id": client.dataframe.list()[0]["id"]}],
        )
        results = client.wait_for_results(task["id"])
    result = results[0]
    json.dumps(result)
    return result, org_ids


def pooled_pca(wide, features, scale=False):
    X = wide[features].to_numpy()
    Z = X - X.mean(axis=0)
    if scale:
        Z = Z / X.std(axis=0, ddof=1)
    values, vectors = np.linalg.eigh(np.cov(Z, rowvar=False))
    order = np.argsort(values)[::-1]
    return values[order], vectors[:, order], Z


def r2(scores, labels):
    """One-way R squared of each column of ``scores`` by ``labels``."""
    total = ((scores - scores.mean(axis=0)) ** 2).sum(axis=0)
    between = sum(
        (labels == g).sum() * (scores[labels == g].mean(axis=0) - scores.mean(axis=0)) ** 2
        for g in np.unique(labels)
    )
    return between / total


print("\n[1] Batch scenario: federated PCA equals pooled PCA")
nodes, wide = make_nodes(batch_shift=0.08)
result, org_ids = run(nodes, n_features=100, n_components=4)
check("error" not in result, "no error")

variance = pd.DataFrame(result["variance_explained"])
groups = pd.DataFrame(result["group_scores"])
loadings = pd.DataFrame(result["top_loadings"])
hospitals = pd.DataFrame(result["hospitals"])
print(variance.to_string(index=False))

check(list(variance.columns) == VARIANCE_COLUMNS, "variance table columns")
check(list(groups.columns) == GROUP_COLUMNS, "group table columns")
check(list(loadings.columns) == LOADING_COLUMNS, "loading table columns")
check(list(hospitals.columns) == HOSPITAL_COLUMNS, "hospital table columns")

pooled_var = wide.var(axis=0, ddof=1).sort_values(ascending=False, kind="stable")
expected_features = sorted(pooled_var.index[:100])
model = result["pca_model"]
check(model["features"] == expected_features, "the 100 most variable CpGs over all hospitals")
check(np.allclose(model["mean"], wide[expected_features].mean().to_numpy()), "global means")

values, vectors, Z = pooled_pca(wide, expected_features)
check(np.allclose(model["explained_variance"], values[:4], rtol=1e-9), "eigenvalues equal pooled PCA")
check(np.allclose(model["explained_variance_ratio"], values[:4] / values.sum()), "explained variance ratio")
fed_vectors = np.asarray(model["components"])
check(np.allclose(np.abs(fed_vectors.T @ vectors[:, :4]), np.eye(4), atol=1e-8),
      "components equal pooled PCA (up to sign)")

scores = Z @ fed_vectors
hospital = wide.index.get_level_values("hospital").to_numpy()
cohort = wide.index.get_level_values("cohort").to_numpy()
check(close(variance["r2_hospital"], r2(scores, hospital)), "R2 hospital equals pooled scores")

reported = cohort != "rare"
check(close(variance["r2_cohort"], r2(scores[reported], cohort[reported])),
      "R2 cohort equals pooled scores (suppressed cohort left out)")
cells = np.char.add(hospital.astype(str), cohort.astype(str))
expected_beyond = r2(scores[reported], cells[reported]) - r2(scores[reported], cohort[reported])
check(close(variance["r2_hospital_beyond_cohort"], expected_beyond), "R2 hospital beyond cohort")

pc_cohort = variance.loc[variance["main_factor"].str.startswith("cohort")]
check(len(pc_cohort) >= 1, "a component follows the cohort")
check(variance["hospital_effect"].any(), "the batch at hospital 3 is flagged")
check("Hospital effect beyond cohort" in result["summary"], "summary mentions the hospital effect")

h2 = groups[(groups["organization_id"] == org_ids[1]) & (groups["component"] == "PC1")]
check(set(h2["cohort"]) == {"all", "case", "control"}, "cohort below min_samples is not reported")
check(hospitals.loc[1, "suppressed_cohorts"] == "rare", "suppressed cohort listed by name only")
all_pc1 = groups[(groups["cohort"] == "all") & (groups["component"] == "PC1")]
for h, org_id in enumerate(org_ids):
    mean = all_pc1.loc[all_pc1["organization_id"] == org_id, "mean"].iloc[0]
    check(np.isclose(mean, scores[hospital == h, 0].mean(), atol=1e-6),
          f"PC1 mean of hospital {h + 1}")

text = json.dumps(result)
check(not any(label in text for label in wide.index.get_level_values("sample")),
      "no sample label in the result")
check("covariance" not in result and "scatter" not in text, "no covariance matrix in the result")

print("\n[2] No batch: no hospital effect")
nodes_clean, wide_clean = make_nodes(batch_shift=0.0, seed=3)
clean, _ = run(nodes_clean, n_features=100, n_components=4)
clean_variance = pd.DataFrame(clean["variance_explained"])
print(clean_variance[["component", "r2_hospital", "p_hospital_beyond_cohort", "main_factor"]].to_string(index=False))
check(clean_variance.loc[0, "main_factor"] == "cohort", "PC1 follows the cohort")
check(not clean_variance["hospital_effect"].iloc[:2].any(), "no hospital effect on PC1-PC2")

print("\n[3] Correlation PCA (scale=True), M values")
scaled, _ = run(nodes, n_features=50, n_components=3, scale=True, value_column="m_value")
m_wide = np.log2(wide / (1 - wide))
m_features = sorted(m_wide.var(axis=0, ddof=1).sort_values(ascending=False, kind="stable").index[:50])
check(scaled["pca_model"]["features"] == m_features, "most variable M-value CpGs")
values, _, _ = pooled_pca(m_wide, m_features, scale=True)
check(np.allclose(scaled["pca_model"]["explained_variance"], values[:3]), "eigenvalues of the correlation matrix")
check(np.isclose(sum(values), 50), "trace of a correlation matrix is the number of CpGs")

print("\n[4] Given CpG list, CpG missing at a hospital, cohort filter")
missing_probe = "cg00000005_BC11"
nodes_missing = [df.copy() for df in nodes]
nodes_missing[0] = nodes_missing[0][nodes_missing[0]["probe_id"] != missing_probe]
given = [f"cg{i:08d}_BC11" for i in range(30)]
subset, _ = run(nodes_missing, features=given, n_features=1000, n_components=2,
                cohorts=["case", "control"])
check(subset["input"][0]["n_cpgs_common"] == 29, "CpG missing at one hospital is excluded")
check(missing_probe not in subset["pca_model"]["features"], "excluded CpG not used")
check(subset["input"][0]["n_samples"] == sum(n for layout in LAYOUT for c, n in layout if c != "rare"),
      "cohort filter applied")

print("\n[5] Without a cohort column; hospital below min_samples")
no_cohort, _ = run(nodes, n_features=50, n_components=2, cohort_column="")
row = no_cohort["variance_explained"][0]
check(row["r2_cohort"] is None and row["r2_hospital"] is not None, "only hospital effects")
small = [nodes[0], nodes[1][nodes[1]["sample_label"].str.contains("rare")], nodes[2]]
partial, ids = run(small, n_features=50, n_components=2)
status = {r["organization_id"]: r["status"] for r in partial["hospitals"]}
check("insufficient samples" in status[ids[1]], "small hospital skipped with the reason")
check(status[ids[0]] == "included" and status[ids[2]] == "included", "other hospitals still analysed")

print("\n[6] Argument checks")
too_many, _ = run(nodes, n_features=5000)
check("error" in too_many, "n_features above 2,000 is refused")

print("\nPASS: federated methylation PCA")
