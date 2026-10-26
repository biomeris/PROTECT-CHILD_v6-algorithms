# v6-kendall-py

Approximate federated Kendall tau-b correlation for Vantage6 v5.

The algorithm keeps raw observations at the participating nodes. The central function
orchestrates two federated rounds: global range estimation and aggregation of 2D binned
contingency tables. Kendall tau-b is then approximated from the aggregated table.

## Local test

Use Python 3.13 and Vantage6 v5:

    uv sync --group dev
    uv run python test/test_compute.py

The included test compares the federated approximation with scipy.stats.kendalltau on
the concatenated mock data.
