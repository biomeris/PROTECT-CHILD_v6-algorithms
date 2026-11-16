# v6-pca-methylation-py — Methylation PCA

Federated PCA of DNA methylation for vantage6 v5, for **quality control and
batch checks**: does the main variation in the data follow the hospital
(processing batch, sample handling, population) or the cohort (biology)?

Adapted from [v6-pca-py](../v6-pca-py) (exact covariance PCA from summed
cross-products, by fra-ncescoo, Vittoria Ramella and AndrewCesc), which is left
unchanged as a general-purpose PCA for tabular data.

## What it does

Input: the long table of the IDAT extraction (`probe_id`, `sample_label`,
`cohort`, `beta`, `m_value`), at each hospital.

1. **CpG selection** (`probe_moments`): each hospital sends per-CpG count,
   sum and sum of squares. The central step keeps the CpGs measured in every
   hospital and picks the `n_features` (default 1,000, at most 2,000) with
   the highest pooled variance, or the most variable of a given list.
2. **Exact PCA** (`partial_scatter`): each hospital sends ZᵀZ of those CpGs,
   centred on the global mean. Their sum is exactly (N − 1) × the global
   covariance matrix, so the components equal those of a pooled PCA.
3. **Score summaries** (`partial_scores`): each hospital projects its own
   samples and sends, per component, count, sum and sum of squares of the
   scores for the hospital and for each cohort with at least `min_samples`
   samples there, plus the number of outlier samples.

Subtasks are created per hospital, so each result is known to belong to its
hospital.

## Result

| Table | Content |
|---|---|
| Variance explained | Per component: share of variance; R² and F-test p-value of hospital, of cohort, of hospital beyond cohort, of cohort beyond hospital; `main_factor` (cohort / hospital / cohort and hospital / not separable / neither); `hospital_effect` flag (hospital beyond cohort, p < 0.05); `separable` (false when every reported cohort is at a single hospital) |
| Scores by hospital and cohort | Mean and SD of each component per hospital (`all`) and per cohort |
| Top CpGs per component | CpGs with the largest `weight` (coefficient in the unit-length component) and their `correlation` with the component's scores |
| Summary | Plain-text reading of the result |
| Scree plot, Cumulative variance | Line charts of the first 20 components (sent in pandas column format, which is what the web interface's line chart reads) |
| Hospitals | Status, samples, CpGs measured in every sample, outliers, suppressed cohorts |
| Input | Data used |

Also `summary` (a plain-text reading) and `pca_model` (CpGs, means, SDs if
scaled, components, explained variance) to project new samples later.

**Reading it.** "Hospital beyond cohort" compares a model with cohort only to
one with each hospital × cohort cell, so a hospital effect is not mistaken
for an unequal mix of cohorts between hospitals. If a cohort is present at a
single hospital only, the two cannot be separated for it. If that holds for
every reported cohort (e.g. `min_samples` hides the smaller cohorts),
`separable` is false, `main_factor` says "not separable" and the cohort R² is
then also a comparison of hospitals.

## Arguments (central)

| Argument | Default | |
|---|---|---|
| `n_features` | 1000 | CpGs used (2–2,000) |
| `features` | – | optional CpG list to choose from (e.g. DMPs) |
| `n_components` | 5 | components reported |
| `scale` | false | correlation PCA (each CpG / its SD) |
| `cohort_column` | `cohort` | empty: PCA without cohorts (hospital effects only) |
| `cohorts` | all | cohorts to include |
| `value_column` | `beta` | or `m_value` |
| `min_samples` | 5 | privacy threshold per hospital and per hospital × cohort; at least 3 (also enforced at the hospitals). If the hidden cohorts at a hospital add up to fewer samples, the next smallest cohort is hidden too |
| `min_cohort_samples` | 5 | a cohort with fewer samples at a hospital is left out of the PCA there (as in the Elastic Net and SVM, so the PCA sees the same samples); listed in the Hospitals table (`cohorts_left_out`) and the summary. Set 1 to keep every sample, e.g. for quality control of all samples |
| `outlier_sd` | 3 | outlier threshold in SDs of a component |
| `top_cpgs` | 10 | CpGs listed per component |
| `organizations_to_include` | all | hospitals |

Sex-chromosome and SNP probes are already masked by the extraction, so the
components are not driven by sex. Cross-reactive probes are not masked yet.

## Test

    python test/test_compute.py

Synthetic data for three hospitals: federated selection, eigenvalues,
components and R² equal the same computation on the pooled data; a batch at
one hospital is flagged and absent without it; correlation PCA on M values;
CpGs missing at a hospital; cohort filter; hospitals and cohorts below
`min_samples`; weights and CpG–component correlations; `min_samples = 1`
refused; hospital and cohort not separable.

See [docs/privacy.md](docs/privacy.md) for what leaves each hospital.
