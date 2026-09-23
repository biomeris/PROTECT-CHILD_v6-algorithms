
# v6-C-statistics

Federated C-statistic (concordance statistic / AUC) for a binary outcome across
multiple organizations without sharing raw data.

This algorithm is designed to be run with the [vantage6](https://vantage6.ai)
infrastructure for distributed analysis and learning.

## What it does

The C-statistic measures how well a numeric score discriminates a binary
outcome: the probability that a randomly chosen record with the outcome (the
"event") scores higher than a randomly chosen record without it, with ties
counted as half a win. It is the standard measure of discrimination for a risk
score or a fitted logistic model, and is identical to the area under the ROC
curve (AUC).

```python
{
  "method": "central",
  "kwargs": {
    "organizations_to_include": [1, 2, 3],
    "outcome_col": "recurrence",
    "columns": ["risk_score_a", "risk_score_b"],
    "positive_label": 1,
    "alpha": 0.05
  }
}
```

Returns, per score column:

| Field | Meaning |
| --- | --- |
| `c_statistic` | The C-statistic / AUC, in `[0, 1]`. `0.5` means no discrimination; `1.0` means perfect discrimination. |
| `standard_error` | Hanley & McNeil (1982) standard error. |
| `ci_lower`, `ci_upper` | Confidence interval at the requested `alpha`. |
| `z_score`, `p_value` | Two-sided test of `H0: C = 0.5`. |
| `n_event`, `n_non_event`, `n_total` | Record counts contributing to the result. |
| `n_rank_bins` | Number of rank bins the statistic was computed from — see below. |

`positive_label` names the value of `outcome_col` that denotes the event. It is
required unless the column is boolean or coded `{0, 1}`, in which case
`True`/`1` is assumed. Getting this backwards silently flips every result to
`1 - C`, so set it explicitly for anything else (e.g. `"yes"`/`"no"` columns).

## How raw data is kept at the node

Computing the C-statistic exactly needs a *global* ranking of scores, which is
normally obtained by sending every value to the central server — that is what
the C-statistic is mathematically a normalized Mann-Whitney U statistic
(`C = U / (n_event × n_non_event)`), so this algorithm reuses the same
three-round, aggregate-only protocol validated for the federated Mann-Whitney
test in [`v6-benjamini-hochberg`](../v6-benjamini-hochberg).

**Round 1 — `partial_moments`.** Each node returns, per score column, three
numbers: the record count, the sum, and the sum of squares (pooled over both
outcome groups). The central server pools these into a global mean and standard
deviation and lays out a shared reporting grid spanning `mean ± 4 sd`. No
minimum, maximum, quantile or individual value is involved.

**Round 2 — `partial_histogram`.** Each node places its records into that grid
and returns *record counts per bin*, pooled across the event and non-event
groups — outcome-blind. The central server sums those counts and merges
adjacent bins into roughly equal-count **rank bins**, then computes each rank
bin's global mid-rank.

**Round 3 — `partial_rank_scores`.** Each node assigns its records the mid-rank
of the bin they fall into and returns, per outcome group, three numbers: the
sum of those scores, their sum of squares, and the record count — six numbers
total. The C-statistic, its standard error, confidence interval and p-value all
follow from those alone.

So the complete payload leaving a node is a handful of counts, sums and sums of
squares. No score value, no rank of any individual record, no identifier, no
pairwise comparison.

### Disclosure control

| Setting | Default | Effect |
| --- | --- | --- |
| `C_STATISTIC_MINIMUM_NUMBER_OF_RECORDS` | 10 | A node holding this many records or fewer refuses to contribute at all. |
| `C_STATISTIC_MINIMUM_GROUP_SIZE` | 5 | An (column, event/non-event) cell smaller than this is dropped from every round and reported as suppressed. |
| `C_STATISTIC_MINIMUM_CELL_COUNT` | 5 | No individual histogram cell below this count is ever transmitted. |

Before the histogram leaves a node, adjacent bins are accumulated until the
running total reaches `C_STATISTIC_MINIMUM_CELL_COUNT` and reported at the
run's count-weighted centre, so every non-zero cell describes at least five
records while the total record count and the shape of the distribution are
preserved. The central server additionally requires every rank bin to hold at
least that many records per contributing node, so the bins the C-statistic is
computed from are non-disclosive too.

### Accuracy of the approximation

The C-statistic is computed in the **general-scores** form of the Mann-Whitney
statistic rather than the classical rank-sum formula, because it only assumes
the scores are a monotone function of the values, not a perfect permutation of
`1..N` — the binning protocol's scores drift slightly off that ideal, and the
classical formula amplifies that drift into a badly wrong result (see the
equivalent note in `v6-benjamini-hochberg`'s README for why). `test/test_math.py`
verifies that with exact ranks this form reproduces `sklearn.metrics.roc_auc_score`
exactly, and that with the binning protocol the federated result is stable and
close to the exact AUC regardless of how the data happens to be split across
nodes.

The standard error, confidence interval and p-value use the Hanley & McNeil
(1982) closed-form approximation, which needs only the C-statistic and the two
group sizes — no pairwise comparison. `test/test_math.py` checks its confidence
interval coverage and Type-I error rate by simulation. Like any closed-form
approximation it can be somewhat conservative or anti-conservative depending on
the true score distribution; for a definitive interval, DeLong's method (exact,
but requiring pairwise data not available under this protocol) remains the
reference standard in a non-federated setting.

## Configuration

```yaml
algorithm_env:
  C_STATISTIC_MINIMUM_NUMBER_OF_RECORDS: 25
  C_STATISTIC_MINIMUM_GROUP_SIZE: 10
  C_STATISTIC_MINIMUM_CELL_COUNT: 10
```

`C_STATISTIC_HISTOGRAM_BINS` (default 200), `C_STATISTIC_MAXIMUM_RANK_BINS`
(default 50) and `C_STATISTIC_RANGE_SD` (default 4.0) control the reporting grid
resolution, the cap on the number of rank bins, and the span of the grid.

## Correcting for multiple score columns

If you evaluate several score columns in one call, treat each `p_value` as one
hypothesis in a family and correct with
[`v6-benjamini-hochberg`](../v6-benjamini-hochberg)'s `central` function — it
takes a dict of p-values and needs no node contact:

```python
{
  "method": "central",
  "kwargs": {
    "p_values": {col: r["p_value"] for col, r in c_statistic_results.items()},
    "alpha": 0.05
  }
}
```

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
`docker tag local_c_statistic_algorithm harbor2.vantage6.ai/algorithms/c-statistics` to
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
