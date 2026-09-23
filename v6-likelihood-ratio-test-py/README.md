
# v6-likelihood-ratio-test

Federated likelihood ratio test for logistic regression covariates across
multiple organizations without sharing raw data.

This algorithm is designed to be run with the [vantage6](https://vantage6.ai)
infrastructure for distributed analysis and learning.

## What it does

Fits a logistic regression model of a binary outcome on a set of numeric
covariates, then — for every covariate — refits the model with just that
covariate dropped and reports the likelihood ratio test of its significance:

```
LR = -2 (logL_reduced - logL_full)  ~  chi2(1)
```

This answers "does this covariate improve the model's fit by more than chance,
holding the others fixed?" — a more powerful and more standard test of a single
predictor's contribution than a Wald test, and the standard companion to
[`v6-C-statistics`](../v6-C-statistics) (which measures how well the *resulting*
model discriminates, not whether a given predictor belongs in it).

```python
{
  "method": "central",
  "kwargs": {
    "organizations_to_include": [1, 2, 3],
    "outcome_col": "recurrence",
    "covariates": ["age", "risk_score", "cohort"],
    "positive_label": 1
  }
}
```

Returns, per covariate:

| Field | Meaning |
| --- | --- |
| `lr_statistic` | The likelihood ratio statistic for dropping this covariate. |
| `degrees_of_freedom` | `1` (one coefficient is dropped per test). |
| `p_value` | From `chi2(1)`. |
| `log_likelihood_full`, `log_likelihood_reduced` | Log-likelihood of the full model and of the model with this covariate dropped. |
| `n_total`, `n_event`, `n_non_event` | Record counts in the complete-case sample shared by every model. |
| `n_iterations_full`, `converged_full` | Newton-Raphson diagnostics for the full model fit. |
| `n_iterations_reduced`, `converged_reduced` | The same, for this particular reduced model. |

Covariates must be numeric; encode a categorical predictor as numeric dummy
columns before calling this algorithm (the same convention used throughout this
repository). `positive_label` is required unless `outcome_col` is boolean or
coded `{0, 1}` — see the note in `v6-C-statistics`'s README for why this isn't
guessed.

The full model and every reduced model are fit on the **same** sample: rows
with a non-null outcome and a non-null value in *every* covariate of the full
list, regardless of which covariate a given reduced model happens to drop. This
is what makes the log-likelihoods comparable — the same requirement standard
software like R's `anova(model1, model2, test="LRT")` enforces.

## How raw data is kept at the node

Fitting a logistic regression model exactly needs the full dataset — normally
obtained by centralizing it. This algorithm never does that. Model fitting uses
**federated Newton-Raphson (IRLS)**: at each iteration, the central server sends
the current coefficient vector to every node; each node computes its local
*score* (log-likelihood gradient) and *Fisher information matrix* — both sums
over its own eligible records — and returns only those two aggregates; the
central server sums them across nodes and takes the Newton step. This repeats
until convergence.

This is not an approximation: summing each node's local score/information gives
exactly the value a centralized fit over the pooled data would, because both
quantities are themselves sums over records (`test/test_math.py` checks this —
the fitted coefficients are identical to 1e-8 whether the data is kept on one
node or split across seven). Once the full model has converged, every reduced
model is refit the same way, **warm-started** from the full model's converged
coefficients (with the dropped covariate's entry removed) — since the reduced
optimum is usually close to that starting point, this typically halves the
number of Newton iterations compared to starting from zero.

So the complete payload leaving a node, every round, is a score vector and a
Fisher information matrix — both sums over every eligible record at that node.
No individual record, no per-record score, no raw covariate value.

### Disclosure control

| Setting | Default | Effect |
| --- | --- | --- |
| `LIKELIHOOD_RATIO_TEST_MINIMUM_NUMBER_OF_RECORDS` | 10 | A node whose complete-case sample is this size or smaller refuses to contribute at all. |
| `LIKELIHOOD_RATIO_TEST_MINIMUM_GROUP_SIZE` | 5 | A node contributes only if it has at least this many event *and* non-event records — otherwise its score/information would be dominated by a handful of records. |

Both checks use the **same** complete-case mask (outcome present, every
covariate of the full list present) in every round, so a node that qualifies
for the full model qualifies identically for every reduced model — there is no
risk of a node flipping in or out of the computation partway through.

### Convergence and numerical stability

Covariates are standardized internally (using pooled mean/sd collected in round
0, themselves ordinary aggregates) purely to keep the Newton-Raphson iteration
well-conditioned — logistic regression's log-likelihood is invariant to this
reparameterization, so it never affects the log-likelihoods or the resulting
likelihood ratio statistic, only how many iterations convergence takes.
`max_iterations` (default 25) is a safety cap, not the typical count — a
well-behaved fit converges in well under 10 iterations, and warm-started reduced
models typically need only 2–4. If a fit fails to converge, or the Fisher
information matrix turns out to be singular, the algorithm raises rather than
returning a misleading result — both usually indicate separation (a covariate
that near-perfectly predicts the outcome) or collinearity among the covariates.

### Accuracy of the approximation

Unlike the histogram-based approximations in `v6-benjamini-hochberg` and
`v6-C-statistics`, nothing here is approximated by binning. The only
approximation is the classical one inherent to the likelihood ratio test
itself: its reference distribution, `chi2(1)`, is asymptotic (Wilks' theorem).
`test/test_math.py` checks by simulation that this holds up well in practice —
Type-I error close to (if mildly conservative of) the nominal rate, and high
power to detect a real effect at moderate sample sizes.

## Configuration

```yaml
algorithm_env:
  LIKELIHOOD_RATIO_TEST_MINIMUM_NUMBER_OF_RECORDS: 25
  LIKELIHOOD_RATIO_TEST_MINIMUM_GROUP_SIZE: 10
```

`max_iterations` and `tolerance` are passed directly to `central()` as call
arguments (not environment variables), since they are properties of a specific
analysis rather than a node-level privacy policy.

## Testing

```bash
# statistics only — needs numpy, scipy and scikit-learn, not vantage6
python test/test_math.py

# end-to-end against the vantage6 mock client
python test/test.py
```

## Dockerizing your algorithm

To finally run your algorithm on the vantage6 infrastructure, you need to
create a Docker image of your algorithm.

A Docker image can be created by executing the following command in the root of your
algorithm directory:

```bash
docker build -t [my_docker_image_name] .
```

where you should provide a sensible value for the Docker image name. The
`docker build` command will create a Docker image that contains your algorithm.
You can create an additional tag for it by running

```bash
docker tag [my_docker_image_name] [another_image_name]
```

This way, you can e.g. do
`docker tag local_lrt_algorithm harbor2.vantage6.ai/algorithms/likelihood-ratio-test` to
make the algorithm available on a remote Docker registry (in this case
`harbor2.vantage6.ai`).

Finally, you need to push the image to the Docker registry. This can be done
by running

```bash
docker push [my_docker_image_name]
```

Note that you need to be logged in to the Docker registry before you can push
the image. You can do this by running `docker login` and providing your
credentials. Check [this page](https://docs.docker.com/get-started/04_sharing_app/)
For more details on sharing images on Docker Hub. If you are using a different
Docker registry, check the documentation of that registry and be sure that you
have sufficient permissions.
