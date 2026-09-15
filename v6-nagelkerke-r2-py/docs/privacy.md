# Privacy — Federated Nagelkerke R2 (v6-nagelkerke-r2-py)

## What leaves each node

The algorithm never returns patient rows. Only aggregates:

- **`partial_loglik`**: per node, `n` (sample count), `sum_y` (count of
  positive outcomes), and `loglik_full` (a single scalar: the sum of
  per-patient log-likelihood terms under the given fitted model). Three
  scalars per node, total.

The coordinator (`central`) only sums these aggregates; it never receives
raw data.

## Guards implemented

- **Minimum K (`min_samples`, default 5)**: if a node has fewer than K valid
  samples (after `dropna`), it returns `{"error": ...}` and contributes no
  statistics.
- **Only JSON-serializable aggregates**: individual records are never
  serialized.

## Residual risks (to evaluate before production)

- **Disclosure via `n` / `sum_y`**: these two scalars reveal a node's local
  sample size and event rate. This is a smaller surface than, e.g., a Gram
  matrix, but still node-identifying information; raise `min_samples` if the
  consortium considers this sensitive.
- **Model provenance**: this algorithm assumes the intercept/coefficients
  passed in were fit responsibly (e.g., via a separate federated logistic
  regression). It does not audit how the model was obtained.
