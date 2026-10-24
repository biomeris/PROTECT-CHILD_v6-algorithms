# v6-spearman-py

Approximate federated Spearman rank correlation for Vantage6 v5.

The algorithm keeps raw observations at the participating nodes. The central function
orchestrates three federated rounds: global range estimation, global histogram
aggregation, and pairwise sufficient statistics on approximate global midranks.

## Local test

Use Python 3.13 and Vantage6 v5:

    uv sync --group dev
    uv run python test/test_compute.py

The included test compares the federated approximation with scipy.stats.spearmanr on
the concatenated mock data.
