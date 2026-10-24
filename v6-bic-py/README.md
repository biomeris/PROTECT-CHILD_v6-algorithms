# Federated BIC — Vantage6 v5

**Model evaluation, not model training.** This algorithm computes BIC
for a *previously fitted common probabilistic model* across horizontally
partitioned data. It does not estimate model coefficients or calculate the
maximum-likelihood solution. Following the design of Vittoria's
[`v6-classification-metrics`](https://github.com/biomeris/PROTECT-CHILD_v6-algorithms/tree/main/v6-classification-metrics),
it separates a pure reusable scoring function in `metrics.py` from the
Vantage6 central and federated execution layers. It does **not** alter or
require the existing classification-metrics library.

## Statistical definition

`BIC = log(sum(n_local))*k - 2*sum(log_likelihood_local)`

- **k / n_parameters**: the number of independently estimated free parameters
  of the same global model, including intercepts and any estimated dispersion
  parameters as required by the likelihood. The caller MUST supply it.
- **n_observations**: the total number of independent observations (or
  appropriately defined observational units for the model); obtained by
  summing the local counts, not guessed from the number of organizations.
- **log-likelihood**: the same **full** likelihood definition and the same
  globally fitted parameters at every organization. For classical AIC/BIC,
  use the likelihood **maximized** for each candidate model. Inputs from an
  arbitrary fixed/prediction-only model can be scored algebraically but are
  **not automatically a valid classical AIC/BIC comparison**.
- Scores from different models are comparable only when likelihood
  conventions, target observations and candidate-model assumptions match.
- Results are exact apart from floating-point arithmetic **if input
  likelihood contributions themselves are exact**. No binning is used.
  The algorithm cannot independently verify that the same coefficients,
  model family or training procedure were used at all nodes.

## Input modes (central_function)

Common arguments: `n_parameters` (required), `input_mode`,
`organizations_to_include` (optional), `model_id_column` (optional).
The optional model ID column checks consistent **identifiers** across nodes,
but does not prove the model coefficients really match.

1. `input_mode="contributions"` (default): one row per observation, with
   `log_likelihood` as its contribution to the fitted-model log-likelihood.
   Missing values in that column are excluded locally. Valid observations
   determine `n`. Use `log_likelihood_column` to rename it.

   ```csv
   log_likelihood,model_id
   -0.10536051565782628,global_model_v1
   -0.2231435513142097,global_model_v1
   ```

2. `input_mode="aggregated"`: one or more pre-aggregated records per
   site, each with `log_likelihood` (sum) and `n_observations` (positive
   integer). This mode accepts the sufficient statistics from another
   federated model algorithm **without exposing per-observation terms**.
   Use `count_column` to rename `n_observations`.

   ```csv
   log_likelihood,n_observations,model_id
   -35.7,50,global_model_v1
   ```

3. `input_mode="binary"`: `y_true` (0/1 labels) and `p_positive`
   (the already-fitted **global** model's predicted probability for class 1).
   Contributions are computed as `log(p)` for positives and
   `log(1-p)` for negatives using numerically stable `log1p`.
   Rows missing either value are excluded. Probabilities of exactly 0
   or 1 are supported when consistent with the observed outcome;
   impossible observed outcomes (zero assigned probability) cause a
   clear error rather than emitting a non-JSON Infinity.
   Use `label_column` / `probability_column` to rename the columns.

   ```csv
   y_true,p_positive,model_id
   1,0.90,global_model_v1
   0,0.20,global_model_v1
   ```

Only aggregate `(log_likelihood, n_observations, model_id)` is sent
to the central task. Even aggregates can be disclosive on very small sites;
apply the project's minimum-group-size/privacy policies before deployment.

## Example central task (Vantage6 v5)

```python
task = client.task.create(
    method="central_function",
    arguments={
        "n_parameters": 4,
        "input_mode": "aggregated",
        "model_id_column": "model_id",
        "organizations_to_include": [1, 2, 3],
    },
    organizations=[1],
    databases=[{"type": "dataframe", "dataframe_id": dataframe_id}],
)
result = client.wait_for_results(task["id"])
```

## Tests

Python >=3.13 and Vantage6 v5:

```shell
python -m pip install -e .
python test/test_metrics.py
python test/test_compute.py
```

`test_metrics.py` checks the pure formula and input validation.
`test_compute.py` runs Vantage6 v5 `MockNetwork` on three heterogeneous
data partitions and asserts agreement with a centralized reference for
all three input modes.

The Dockerfile and `algorithm_store.json` are **deployment templates**:
build/push the image and set the final registry/release tag before
submitting the algorithm to the store.
