# Federated Tukey HSD / Tukey-Kramer (Vantage6 v5)

Global pairwise group comparisons after one-way ANOVA. Every station
contributes per-group n, sum and sum-of-squares; the central function
reconstructs the global within-group mean square error, studentized
range q-statistics, multiplicity-adjusted p-values and confidence intervals.

The method is exact up to floating-point error and assumes independent
normally distributed residuals with a common variance. With unequal group
sizes it uses the Tukey-Kramer standard error.

Call `central_function(group_col="Group", features=["age", "Height"], alpha=0.05)`.

## Local test

With Python 3.13 and Vantage6 5 installed:

```shell
pip install -e .
python test/test_compute.py
```
