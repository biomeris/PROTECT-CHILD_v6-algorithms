"""Federated Nagelkerke's R2 (vantage6 v5) — pseudo-R2 for a fitted logistic model."""

# Compute functions (central + federated)
from .federated import central, partial_loglik

# Built-in CSV data extraction (callable when creating the session).
# If a custom function is defined in extract.py, import it here too.
from vantage6.algorithm.data_extraction import read_csv
