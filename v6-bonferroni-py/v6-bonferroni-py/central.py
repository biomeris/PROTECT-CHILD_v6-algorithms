"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""

import math
import numbers
from typing import Any

from vantage6.algorithm.tools.util import info
from vantage6.algorithm.tools.exceptions import InputError
from vantage6.algorithm.decorator.action import central


def _bonferroni(
    p_values: dict[str, float], alpha: float, num_tests: int
) -> dict[str, dict[str, float | bool]]:
    """
    Apply the Bonferroni correction to a dict of labelled p-values.

    Pure function (no vantage6 dependencies) so it can be unit tested directly.
    """
    corrected = {}
    for label, p in p_values.items():
        p_adjusted = min(1.0, p * num_tests)
        corrected[label] = {
            "p_value": p,
            "p_adjusted": p_adjusted,
            # Standard convention (matches statsmodels.stats.multitest): reject
            # when the adjusted p-value is at or below alpha, not strictly below.
            "reject": p_adjusted <= alpha,
        }
    return corrected


def _is_number(x: Any) -> bool:
    """True for any real number (int, float, numpy scalar, ...) but not bool."""
    return isinstance(x, numbers.Real) and not isinstance(x, bool)


@central
def compute_bonferroni_correction(
    p_values: dict[str, float] | list[float],
    alpha: float = 0.05,
    num_tests: int | None = None,
) -> Any:
    """
    Apply Bonferroni correction to a family of p-values.

    Unlike the other algorithms in this repository, this central function does
    not dispatch any subtasks to the nodes. Bonferroni correction is a
    deterministic arithmetic operation (``p_adjusted = min(1, p * m)``) applied
    to p-values that have already been produced elsewhere - typically the
    output of another federated hypothesis test in this repository, such as
    ``v6-fisher-exact-test-py`` or ``v6-t-test-py``. As a result, no node-local
    data is required to perform this correction, and no ``AlgorithmClient`` is
    needed.

    Parameters
    ----------
    p_values : dict[str, float] | list[float]
        The p-values to correct. Prefer a ``dict`` mapping a label (e.g. the
        name of the hypothesis or variable that was tested) to its p-value, so
        that results can be traced back to the test they came from. A bare
        list is also accepted and will be labelled by position
        (``"0"``, ``"1"``, ...).
    alpha : float
        The family-wise significance level to test against. Defaults to 0.05.
    num_tests : int | None
        The number of tests ``m`` to correct for. Defaults to
        ``len(p_values)``. Provide this explicitly if the full family of tests
        is larger than the p-values supplied in this call. Must be an integer
        greater than or equal to ``len(p_values)``.

    Returns
    -------
    dict
        A dictionary containing:
        - "alpha": the significance level used
        - "num_tests": the value of ``m`` used for the correction
        - "results": a dict, keyed the same way as the input, of
          ``{"p_value": ..., "p_adjusted": ..., "reject": ...}``
    """
    info("Validating input p-values")
    if isinstance(p_values, (list, tuple)):
        p_values = {str(i): p for i, p in enumerate(p_values)}
    elif not isinstance(p_values, dict):
        raise InputError(
            "'p_values' must be a dict[label, float] or a list/tuple of floats, "
            f"got {type(p_values).__name__}."
        )

    if not p_values:
        raise InputError("'p_values' must not be empty.")

    for label, p in p_values.items():
        if not _is_number(p) or not (0 <= p <= 1):
            raise InputError(
                f"p-value for '{label}' must be a number in [0, 1], got {p!r}."
            )

    if num_tests is None:
        num_tests = len(p_values)
    elif not _is_number(num_tests) or not math.isfinite(num_tests) or num_tests != int(num_tests):
        raise InputError(f"'num_tests' must be a finite integer, got {num_tests!r}.")
    else:
        num_tests = int(num_tests)

    if num_tests < 1:
        raise InputError("'num_tests' must be at least 1.")
    if num_tests < len(p_values):
        raise InputError(
            "'num_tests' must be at least the number of p-values supplied "
            f"({len(p_values)}), got {num_tests}. Correcting for fewer tests "
            "than were actually performed would understate the correction."
        )

    if not _is_number(alpha) or not (0 < alpha < 1):
        raise InputError(
            f"'alpha' must be a number strictly between 0 and 1, got {alpha!r}."
        )

    info(f"Applying Bonferroni correction (alpha={alpha}, num_tests={num_tests})")
    results = _bonferroni(p_values, alpha, num_tests)

    info("Returning results!")
    return {"alpha": alpha, "num_tests": num_tests, "results": results}
