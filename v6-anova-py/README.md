# Federated one-way ANOVA (Vantage6 v5)

Computes exact global one-way ANOVA F-statistics (apart from floating-point
rounding) without transferring raw rows. Each station sends counts, sums,
and squared sums per observed group and numeric feature.

Features are analyzed independently after dropping invalid/NA values;
the grouping column must be present at each participating station.
Use `group_col="Group"`, or legacy `groups=["Group"]`.
The one-way equal-variance F-test assumes independent observations,
normally distributed within-group errors and common variance.

## Local development

Install using Python 3.13:

```shell
pip install -e .
python test/test_compute.py
```

The local test uses Vantage6 v5 `MockNetwork` and compares the global
F-statistic and p-value with `scipy.stats.f_oneway`.
