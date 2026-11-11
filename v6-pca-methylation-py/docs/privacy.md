# Privacy — v6-pca-methylation-py

## What leaves each hospital

Only aggregates over at least `min_samples` samples; never a sample's values,
scores or label.

| Round | Sent | Over |
|---|---|---|
| `probe_moments` | count, sum and sum of squares per CpG (CpGs measured in every local sample) | all local samples |
| `partial_scatter` | ZᵀZ (p × p) of the selected CpGs, centred on the global mean | all local samples |
| `partial_scores` | count, sum and sum of squares of each component's scores, for the hospital and for each cohort with ≥ `min_samples` samples; names of smaller cohorts; number of outlier samples | the hospital / the cohort at the hospital |

The result returns no covariance matrix (unlike v6-pca-py) and no sample
scores: only the tables and the PCA model (CpG means, components).

## Guards

- A hospital with fewer than `min_samples` samples returns an error and is
  left out (with the reason in the Hospitals table).
- A cohort with fewer than `min_samples` samples at a hospital is not
  summarised; only its name is returned.
- At most 2,000 CpGs (size of ZᵀZ, ~43 MB per hospital).

## Residual risks (to decide with the consortium)

- **ZᵀZ with fewer samples than CpGs.** The same concern as the Gram matrix
  of the elastic net: when p ≥ n at a hospital, ZᵀZ carries much information
  about each sample. The hospital logs a warning and the result flags it
  (`p_exceeds_n`). Options: fewer CpGs than samples, a higher
  `min_samples`, or noise on ZᵀZ (differential privacy).
- **Outlier count.** A small number (e.g. 1) says that one sample at that
  hospital is unusual, but not which one.
- **Differencing.** Repeated runs with slightly different cohort or CpG
  selections could in principle isolate small groups; the same applies to
  all aggregate algorithms.
