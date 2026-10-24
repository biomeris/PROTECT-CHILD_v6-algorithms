# Federated PCA (Vantage6 v5)

PCA computes the eigenvectors of the global covariance matrix from
node-local sufficient statistics `n`, `sum(X)` and `X.T @ X`. Results
agree with centralized covariance PCA up to floating-point differences and
arbitrary eigenvector sign choices.

Select `features` to guarantee the same column ordering across nodes.
Rows with missing or non-finite selected values are excluded locally.
`center=True` (default) centers globally; `center=False` uses
the uncentered second-moment matrix divided by `n-1`.

## Test

With Python 3.13 and Vantage6 v5 installed, run:

```shell
pip install -e .
python test/test_compute.py
```
