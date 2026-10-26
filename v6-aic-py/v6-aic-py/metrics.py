"""Pure, reusable AIC scoring function (independent of Vantage6)."""
import math


def _validate(log_likelihood: float, n_parameters: int) -> tuple[float, int]:
    if isinstance(n_parameters, bool) or not isinstance(n_parameters, int) or n_parameters < 0:
        raise ValueError("n_parameters must be a nonnegative integer")
    try:
        ll = float(log_likelihood)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ValueError("log_likelihood must be a finite real number") from exc
    if not math.isfinite(ll):
        raise ValueError("log_likelihood must be finite")
    return ll, n_parameters


def compute_aic(log_likelihood: float, n_parameters: int) -> float:
    """Compute AIC of a fitted model from its *maximized* log-likelihood.

    Counting of all fitted/free parameters is the caller's responsibility.
    This function does not fit a model.
    """
    ll, k = _validate(log_likelihood, n_parameters)
    return float(2 * k - 2 * ll)
