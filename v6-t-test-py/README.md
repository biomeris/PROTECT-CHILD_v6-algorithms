# Federated independent-samples t-test (Vantage6 v5)

Computes a pooled-variance two-sided Student t-test from local count, sum
and sum-of-squares without sharing row-level data.

Modes:

- `group_col="Group"`: compare exactly two labels pooled globally over
  participating organizations.
- `group_col=None`: legacy mode comparing two distinct organizations in
  the specified `organizations_to_include` order.

The test assumes independent samples with equal population variances and
uses two-sided p-values. It is **not** Welch's t-test.

Local nodes must contain more than
`T_TEST_MINIMUM_NUMBER_OF_RECORDS` (default 3) rows.

## Test

Using Python 3.13 and Vantage6 v5:

```shell
pip install -e .
python test/test_compute.py
```
