# v6-nagelkerke-r2-py

Federated **Nagelkerke's R2** for an already-fitted logistic regression model.

## Purpose

Report how well a logistic regression model (fit elsewhere — this algorithm
does not fit models) explains the outcome across hospitals, without moving
patient-level data between nodes.

## How it works (exact federation)

Log-likelihood is additive over independent observations, so both
log-likelihoods needed for the R2 formulas are simple sums of per-node
partial sums — no iterative fitting, a single round:

- `R2_CS = 1 - exp((2/n) * (logL0 - logL1))` (Cox-Snell)
- `R2_N = R2_CS / (1 - exp((2/n) * logL0))` (Nagelkerke)

where `n` is the total sample size, `logL1` is the fitted model's
log-likelihood (each node computes `sum(y*log(p_hat) + (1-y)*log(1-p_hat))`
using the given intercept/coefficients), and `logL0` is the null
(intercept-only) model's log-likelihood, which has a closed form once the
global `n` and global positive-outcome count `sum(y)` are known:
`logL0 = n * [p_bar*ln(p_bar) + (1-p_bar)*ln(1-p_bar)]` with `p_bar = sum(y)/n`.

Each node reports only 3 scalars (`n`, `sum_y`, `loglik_full`) in one round.

## Functions

| Function | Type | Role |
|---|---|---|
| `central` | central | orchestrates the single round, aggregates, computes R2 |
| `partial_loglik` | federated | local log-likelihood statistics |

## Input

One database per node: predictor columns + a binary `outcome` column (0/1).
Other columns are ignored.

## Arguments (`central` function)

| Argument | Type | Default | Description |
|---|---|---|---|
| `features` | column_list | — | predictor columns, same order as `coefficients` |
| `outcome_column` | column | — | binary outcome column |
| `intercept` | float | — | fitted model intercept |
| `coefficients` | json | — | fitted model coefficients (list of floats) |
| `min_samples` | int | 5 | privacy guard: nodes with fewer samples are skipped |

## Output

```json
{
  "n_total": 200,
  "n_nodes": 4,
  "p_bar": 0.35,
  "loglik_null": -132.7,
  "loglik_full": -101.2,
  "r2_cox_snell": 0.271,
  "r2_nagelkerke": 0.368
}
```

## Privacy

Only aggregates leave each node (`n`, `sum_y`, `loglik_full` — 3 scalars per
node), never patient rows. Privacy guard K (`min_samples`, default 5). See
`docs/privacy.md`.

## Local test

```bash
pip install -e .
python test/test.py   # validates federated == centralized (statsmodels)
```

## Build & deploy

```bash
docker build -t biomerisdocker/nagelkerke-r2:1.0.0 .
docker push biomerisdocker/nagelkerke-r2:1.0.0
```
