"""Pure, reusable BIC scoring function (independent of Vantage6)."""
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


def compute_bic(log_likelihood: float, n_parameters: int, n_observations: int) -> float:
    """Compute BIC for a fitted model from maximized log-likelihood."""
    ll, k = _validate(log_likelihood, n_parameters)
    if isinstance(n_observations, bool) or not isinstance(n_observations, int) or n_observations < 1:
        raise ValueError("n_observations must be an integer >= 1")
    return float(math.log(n_observations) * k - 2 * ll)
