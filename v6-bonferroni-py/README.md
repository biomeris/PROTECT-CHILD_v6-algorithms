
# v6-bonferroni-py

Bonferroni correction for multiple hypothesis testing, applied centrally to a family
of p-values produced by another federated test.

This algorithm is designed to be run with the [vantage6](https://vantage6.ai)
infrastructure for distributed analysis and learning.

The base code for this algorithm has been created following the
[v6-algorithm-template](https://github.com/vantage6/v6-algorithm-template) layout used
elsewhere in this repository.

## Why this algorithm has no federated/partial function

Bonferroni correction is not a statistic computed over patient-level data - it is
deterministic arithmetic applied to p-values that some other federated test (e.g.
`v6-fisher-exact-test-py`, `v6-t-test-py`, `v6-anova-py`) has already produced. Given
`m` p-values it computes, per p-value `p`:

```
p_adjusted = min(1, p * m)
reject     = p_adjusted < alpha
```

Because this needs only the p-values and `m` (and nothing from the node databases),
the whole correction runs in a single `central()` call with no subtasks dispatched to
nodes, and no `AlgorithmClient` is required.

## Function

### `compute_bonferroni_correction`

| Argument | Type | Description |
| --- | --- | --- |
| `p_values` | `dict[str, float]` \| `list[float]` | The p-values to correct. Prefer a dict mapping a label (e.g. the hypothesis or variable name) to its p-value so results stay traceable. A list is also accepted and labelled by position (`"0"`, `"1"`, ...). |
| `alpha` | `float` | Family-wise significance level to test against. Default `0.05`. |
| `num_tests` | `int \| None` | The number of tests `m` to correct for. Defaults to `len(p_values)`. Override this if the full family of tests is larger than what is passed in this call. |

Returns:

```json
{
  "alpha": 0.05,
  "num_tests": 3,
  "results": {
    "site_a": {"p_value": 0.01, "p_adjusted": 0.03, "reject": true},
    "site_b": {"p_value": 0.20, "p_adjusted": 0.60, "reject": false},
    "site_c": {"p_value": 0.001, "p_adjusted": 0.003, "reject": true}
  }
}
```

### Example: chaining onto `v6-fisher-exact-test-py`

```python
fisher_result = client.wait_for_results(fisher_task.get("id"))[0]

correction_task = client.task.create(
    method="compute_bonferroni_correction",
    arguments={"p_values": {"my_hypothesis": fisher_result["p_value"]}, "alpha": 0.05},
    organizations=[org_ids[0]],
)
corrected = client.wait_for_results(correction_task.get("id"))
```

### Edge cases

- `p_values` must be a `dict` or `list`/`tuple`; anything else (e.g. a string,
  `None`) raises `InputError`.
- An empty `p_values` raises `InputError`.
- An invalid p-value (outside `[0, 1]`, `NaN`, non-numeric, or a `bool`) raises
  `InputError`. Numeric types beyond plain `int`/`float` (e.g. numpy scalars) are
  accepted.
- `num_tests` must be a whole number `>= len(p_values)` (correcting for fewer tests
  than were actually supplied would understate the correction) and `>= 1`; a
  non-integer, `NaN`, or out-of-range value raises `InputError`.
- `alpha` must be strictly between 0 and 1 (exclusive on both ends); `NaN`, `bool`,
  or a value outside that range raises `InputError`.
- `p * m` is clamped to `1.0` rather than left unbounded.
- `reject` is `True` when `p_adjusted <= alpha` (matches
  `statsmodels.stats.multitest.multipletests`'s convention of using `<=`, not a
  strict `<`).

### Dockerizing your algorithm

```bash
docker build -t [my_docker_image_name] .
docker tag [my_docker_image_name] [another_image_name]
docker push [my_docker_image_name]
```

Note that you need to be logged in to the Docker registry before you can push the
image (`docker login`).
