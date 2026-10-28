"""Federated entrypoint: report the cohort count an extraction step produced.

``generate_cohort`` (extract.py) does the actual work at each node and leaves a
one-row dataframe in the session. A federated compute step is the only kind of
task whose return value reaches the client, so this function exists to read
that row and hand it back. It touches no database.
"""

from __future__ import annotations

from typing import Any, Callable, TypeVar

import pandas as pd

from .cohort import RESULT_FIELDS, NodeConfigError, minimum_count, withhold_small_count

DecoratedFunction = TypeVar("DecoratedFunction", bound=Callable[..., Any])


try:
    from vantage6.algorithm.decorator.action import federated
    from vantage6.algorithm.decorator.data import dataframe
except ValueError as exc:  # rpy2 without R at import time, e.g. in unit tests
    if "r_home is None" not in str(exc):
        raise

    def federated(func: DecoratedFunction) -> DecoratedFunction:
        return func

    def dataframe(_count: int) -> Callable[[DecoratedFunction], DecoratedFunction]:
        def wrapper(func: DecoratedFunction) -> DecoratedFunction:
            return func
        return wrapper


def _run_cohort_count(df: pd.DataFrame) -> dict[str, Any]:
    """Hand back the cohort outcome row, and nothing else.

    The step can be pointed at any dataframe in the session, so it returns
    only the cohort result fields, refuses a dataframe that is not one, and
    applies the node's minimum count again.
    """
    if df is None or df.empty:
        return {"status": "error", "message": "No cohort result found in the session"}
    if not {"status", "count"}.issubset(df.columns):
        return {
            "status": "error",
            "message": "The dataframe is not a cohort result from generate_cohort",
        }
    row = df.iloc[0].to_dict()
    # Parquet round-trips numpy scalars; the client expects plain JSON types.
    clean: dict[str, Any] = {}
    for key in RESULT_FIELDS:
        if key not in row:
            continue
        value = row[key]
        if hasattr(value, "item"):
            value = value.item()
        if value != value:  # NaN
            value = None
        clean[key] = value
    try:
        return withhold_small_count(clean, minimum_count())
    except NodeConfigError as exc:
        return {"status": "error", "cohort_id": clean.get("cohort_id"), "message": str(exc)}


@federated
@dataframe(1)
def cohort_count(df: pd.DataFrame) -> dict[str, Any]:
    """Return the cohort outcome recorded by ``generate_cohort`` at this node."""
    return _run_cohort_count(df)


# Kept so existing callers get a clear message instead of an obscure crash.
@federated
def generate_cohort_count(
    sql: str,
    cohort_name: str,
    cohort_id: int,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Deprecated: cohort generation must run as a data-extraction step.

    On vantage6 5 a federated task receives no database connection, so this
    entrypoint cannot reach OMOP. Call ``generate_cohort`` as an extraction step
    and ``cohort_count`` to read the result.
    """
    del sql, overwrite
    return {
        "status": "error",
        "cohort_id": cohort_id,
        "cohort_name": cohort_name,
        "message": (
            "generate_cohort_count runs as a federated step, which vantage6 5 "
            "starts without database access. Use the data-extraction entrypoint "
            "generate_cohort followed by cohort_count."
        ),
    }
