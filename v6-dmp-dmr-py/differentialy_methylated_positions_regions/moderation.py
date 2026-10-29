"""
Empirical Bayes moderation of per-probe variances (limma's squeezeVar / eBayes).

With few samples per cohort, a probe's residual variance is very unreliable: by
chance it can be close to zero, which gives a near-zero standard error, a huge
t-statistic and, in an inverse-variance meta-analysis, an almost infinite weight.
Following Smyth (2004), the residual variances of all probes are modelled as
draws from a scaled inverse chi-square prior with ``d0`` degrees of freedom and
scale ``s0^2``, estimated from the data by the method of moments on log variances.
Each probe's variance is then replaced by its posterior mean

    s2_post = (d0 * s0^2 + d * s2) / (d0 + d)

which pulls extreme variances towards the typical one; the moderated statistic
has ``d + d0`` degrees of freedom.

Reference: Smyth GK (2004). Linear models and empirical Bayes methods for
assessing differential expression in microarray experiments. Stat Appl Genet Mol
Biol 3:3.
"""
from __future__ import annotations

import numpy as np
from scipy.special import digamma, polygamma


def trigamma(x):
    return polygamma(1, x)


def trigamma_inverse(y: float) -> float:
    """Solve trigamma(x) = y for x > 0 (Newton's method, as in limma)."""
    if y > 1e7:
        return 1 / np.sqrt(y)
    if y < 1e-6:
        return 1 / y
    x = 0.5 + 1 / y
    for _ in range(50):
        tri = trigamma(x)
        step = tri * (1 - tri / y) / polygamma(2, x)
        x += step
        if -step / x < 1e-8:
            break
    return float(x)


def fit_prior(s2: np.ndarray, df: np.ndarray) -> tuple[float, float]:
    """Prior degrees of freedom d0 and scale s0^2 of the variances (limma fitFDist).

    Only finite, positive variances with df > 0 are used. d0 is infinite when the
    variances vary no more than chance would explain (then every probe gets s0^2).
    """
    s2 = np.asarray(s2, dtype=float)
    df = np.asarray(df, dtype=float)
    ok = np.isfinite(s2) & (s2 > 0) & np.isfinite(df) & (df > 0)
    if ok.sum() < 3:
        raise ValueError("At least 3 probes with a positive variance are needed to moderate variances")
    s2, df = s2[ok], df[ok]
    z = np.log(s2)
    e = z - digamma(df / 2) + np.log(df / 2)
    n = len(e)
    e_mean = e.mean()
    e_var = ((e - e_mean) ** 2).sum() / (n - 1) - trigamma(df / 2).mean()
    if e_var > 0:
        d0 = 2 * trigamma_inverse(e_var)
        s0_2 = float(np.exp(e_mean + digamma(d0 / 2) - np.log(d0 / 2)))
    else:
        d0 = np.inf
        s0_2 = float(np.exp(e_mean))
    return float(d0), s0_2


def squeeze_variances(s2: np.ndarray, df: np.ndarray, d0: float, s0_2: float) -> np.ndarray:
    """Posterior variances: (d0 * s0^2 + d * s2) / (d0 + d); s0^2 when d0 is infinite."""
    s2 = np.asarray(s2, dtype=float)
    df = np.asarray(df, dtype=float)
    if np.isinf(d0):
        return np.full_like(s2, s0_2)
    return (d0 * s0_2 + df * s2) / (d0 + df)
