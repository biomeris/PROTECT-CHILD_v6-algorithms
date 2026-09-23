"""
The Benjamini-Hochberg procedure itself.

Pure arithmetic on a family of p-values: no vantage6 imports, no data access.
"""
from __future__ import annotations

from math import isnan
from typing import Dict, List, Optional, Sequence, Tuple, Union

PValues = Union[Dict[str, Optional[float]], Sequence[Optional[float]]]


def normalise_p_values(p_values: PValues) -> Dict[str, Optional[float]]:
    """Accept either ``{name: p}`` or ``[p, p, ...]`` and always return a dict."""
    if isinstance(p_values, dict):
        return {str(k): v for k, v in p_values.items()}
    return {f"hypothesis_{i + 1}": p for i, p in enumerate(p_values)}


def _is_testable(p: Optional[float]) -> bool:
    """A p-value takes part in the correction only if it is a number in [0, 1]."""
    if p is None:
        return False
    try:
        p = float(p)
    except (TypeError, ValueError):
        return False
    return not isnan(p) and 0.0 <= p <= 1.0


def benjamini_hochberg(
    p_values: PValues,
    alpha: float = 0.05,
) -> Dict[str, Dict]:
    """
    Benjamini-Hochberg FDR correction over a family of p-values.

    The step-up procedure orders the m testable p-values ascending and rejects
    every hypothesis up to the largest rank ``k`` with ``p_(k) <= k/m * alpha``.
    The reported adjusted p-value (q-value) is the usual monotone form
    ``q_(i) = min_{j >= i} ( m/j * p_(j) )``, clipped to 1.

    Parameters
    ----------
    p_values
        ``{hypothesis: p}`` or a plain sequence of p-values. Entries that are
        ``None``/``NaN``/out of range are carried through untested.
    alpha
        Target false discovery rate.

    Returns
    -------
    dict
        ``{"alpha", "n_hypotheses", "n_tested", "n_rejected",
        "largest_rejected_rank", "results"}`` where ``results`` maps each
        hypothesis to ``{"p_value", "adjusted_p_value", "reject", "rank",
        "critical_value", "tested"}``.
    """
    if not 0.0 < alpha < 1.0:
        raise ValueError(f"alpha must be in (0, 1), got {alpha}.")

    named = normalise_p_values(p_values)
    if not named:
        raise ValueError("Provide at least one p-value.")

    testable: List[Tuple[str, float]] = [
        (name, float(p)) for name, p in named.items() if _is_testable(p)
    ]
    m = len(testable)

    results: Dict[str, Dict] = {
        name: {
            "p_value": None if named[name] is None else float(named[name]),
            "adjusted_p_value": None,
            "reject": False,
            "rank": None,
            "critical_value": None,
            "tested": False,
        }
        for name in named
    }

    if m == 0:
        return {
            "alpha": alpha,
            "n_hypotheses": len(named),
            "n_tested": 0,
            "n_rejected": 0,
            "largest_rejected_rank": 0,
            "results": results,
        }

    # Ascending by p-value; ties broken by name so the ranking is deterministic.
    order = sorted(range(m), key=lambda i: (testable[i][1], testable[i][0]))

    # Step-up from the largest p-value: q_(i) = min(q_(i+1), m/i * p_(i)).
    adjusted = [1.0] * m
    running = float("inf")
    for rank in range(m, 0, -1):
        idx = order[rank - 1]
        running = min(running, m / rank * testable[idx][1])
        adjusted[idx] = min(running, 1.0)

    # Largest rank whose raw p-value clears its own BH threshold.
    largest_rejected_rank = 0
    for rank in range(m, 0, -1):
        if testable[order[rank - 1]][1] <= rank / m * alpha:
            largest_rejected_rank = rank
            break

    for rank, idx in enumerate(order, start=1):
        name, p = testable[idx]
        results[name] = {
            "p_value": p,
            "adjusted_p_value": adjusted[idx],
            "reject": rank <= largest_rejected_rank,
            "rank": rank,
            "critical_value": rank / m * alpha,
            "tested": True,
        }

    return {
        "alpha": alpha,
        "n_hypotheses": len(named),
        "n_tested": m,
        "n_rejected": largest_rejected_rank,
        "largest_rejected_rank": largest_rejected_rank,
        "results": results,
    }
