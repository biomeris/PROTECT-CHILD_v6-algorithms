"""v6-cohort-builder-py package.

Entry points, in the order a client calls them on vantage6 5:

1. ``generate_cohort`` — data-extraction step; runs at each node with database
   access, writes the cohort to ``results.cohort``, leaves a one-row dataframe
   with the count in the session.
2. ``central_function`` — central step on that dataframe; asks every node's
   ``cohort_count`` for its row and adds the counts up. A client can also call
   ``cohort_count`` (federated) directly and add them up itself.

``generate_cohort_count`` is the pre-v5 entry point. It remains importable so
old task definitions fail with a clear message rather than an obscure crash.
"""

from .central import central_function
from .extract import generate_cohort
from .federated import cohort_count, generate_cohort_count

__all__ = [
    "generate_cohort",
    "cohort_count",
    "central_function",
    "generate_cohort_count",
]
