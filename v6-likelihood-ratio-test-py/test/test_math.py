"""
Verify the statistical core of the algorithm without needing vantage6 or Docker.

`model.py` deliberately has no vantage6 imports, so its aggregation, Newton
step, and likelihood ratio test can be checked directly against a centralized
Newton-Raphson fit, against scikit-learn's unregularized logistic regression,
and by simulation (Type-I error, power). The node-side score/information
computation in `partial.py` is reproduced by hand below (same formulas, no
vantage6 decorators) so the whole pipeline can be exercised without installing
vantage6. Run as:

    python test_math.py

Requires numpy, scipy and scikit-learn. For the end-to-end federated run
against the vantage6 mock client, see `test.py`.
"""
import importlib.util
import sys
from pathlib import Path

import numpy as np
from scipy.stats import chi2
from sklearn.linear_model import LogisticRegression

PKG = Path(__file__).parent.parent / "v6-likelihood-ratio-test"


def _load(name):
    """Import a module by path so the vantage6-dependent package is bypassed."""
    spec = importlib.util.spec_from_file_location(name, PKG / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


model = _load("model")

failures = []


def check(label, ok, detail=""):
    print(f"{'PASS' if ok else 'FAIL'}  {label}{('  ' + detail) if detail else ''}")
    if not ok:
        failures.append(label)


def node_score_information(X, y, beta):
    """Same formula as `partial.partial_score_information`, without vantage6."""
    p = np.clip(model.sigmoid(X @ beta), 1e-10, 1 - 1e-10)
    score = X.T @ (y - p)
    weights = p * (1 - p)
    information = (X * weights[:, None]).T @ X
    return {"score": score.tolist(), "information": information.tolist(), "n": len(y)}


def node_log_likelihood(X, y, beta):
    """Same formula as `partial.partial_log_likelihood`, without vantage6."""
    p = np.clip(model.sigmoid(X @ beta), 1e-10, 1 - 1e-10)
    return {"log_likelihood": float(np.sum(y * np.log(p) + (1 - y) * np.log(1 - p))), "n": len(y)}


def federated_fit(X, y, node_slices, beta_start, max_iter=25, tol=1e-6):
    """Reproduce `central._fit_model` using only `model.py`'s aggregation."""
    n_params = X.shape[1]
    beta = beta_start.copy()
    converged = False
    for iteration in range(1, max_iter + 1):
        node_results = [node_score_information(X[sl], y[sl], beta) for sl in node_slices]
        aggregated = model.aggregate_score_information(node_results, n_params)
        score, information, n_total, n_contributors = aggregated
        delta = model.newton_step(score, information)
        beta = beta + delta
        if model.has_converged(score, delta, tol_score=tol):
            converged = True
            break
    node_ll = [node_log_likelihood(X[sl], y[sl], beta) for sl in node_slices]
    logl = sum(r["log_likelihood"] for r in node_ll)
    return beta, logl, iteration, converged


rng = np.random.default_rng(3)

# ── Federated fit is exact: matches a centralized Newton-Raphson fit and
#    scikit-learn's unregularized MLE, regardless of node partitioning ────────
n = 3000
x1, x2, x3 = rng.normal(0, 1, n), rng.normal(0, 1, n), rng.normal(0, 1, n)
lin = -0.3 + 1.1 * x1 - 0.6 * x2  # x3 has no true effect
p_true = model.sigmoid(lin)
y = (rng.random(n) < p_true).astype(float)
X = np.column_stack([np.ones(n), x1, x2, x3])

sk = LogisticRegression(penalty="none", solver="lbfgs", max_iter=1000, tol=1e-12)
sk.fit(X[:, 1:], y)
beta_sklearn = np.concatenate([sk.intercept_, sk.coef_[0]])

single_node = [slice(0, n)]
beta_1, logl_1, iters_1, conv_1 = federated_fit(X, y, single_node, np.zeros(4))
check(
    "single-node fit matches sklearn's unregularized MLE",
    conv_1 and np.max(np.abs(beta_1 - beta_sklearn)) < 1e-4,
    f"max|diff|={np.max(np.abs(beta_1 - beta_sklearn)):.2e}, {iters_1} iterations",
)

for k in (2, 3, 7):
    slices = [
        slice(a, b)
        for a, b in zip(
            np.linspace(0, n, k + 1, dtype=int)[:-1], np.linspace(0, n, k + 1, dtype=int)[1:]
        )
    ]
    beta_k, logl_k, iters_k, conv_k = federated_fit(X, y, slices, np.zeros(4))
    check(
        f"fit is identical whether split across 1 or {k} nodes",
        conv_k and np.max(np.abs(beta_k - beta_1)) < 1e-8 and abs(logl_k - logl_1) < 1e-6,
        f"max|beta diff|={np.max(np.abs(beta_k - beta_1)):.2e}, |logL diff|={abs(logl_k-logl_1):.2e}",
    )

# ── Warm-starting a reduced model from the full model's coefficients converges
#    faster than starting from zero, and lands on the same optimum ──────────
X_reduced = X[:, :3]  # drop x3
beta_cold, logl_cold, iters_cold, conv_cold = federated_fit(X_reduced, y, [slice(0, n)], np.zeros(3))
beta_warm_start = model.warm_start(beta_1, dropped_index=3)
beta_warm, logl_warm, iters_warm, conv_warm = federated_fit(
    X_reduced, y, [slice(0, n)], beta_warm_start
)
check(
    "warm start converges to the same optimum as a cold start",
    conv_warm and np.max(np.abs(beta_warm - beta_cold)) < 1e-8,
)
check(
    "warm start takes no more iterations than a cold start",
    iters_warm <= iters_cold,
    f"warm={iters_warm} cold={iters_cold}",
)

# ── The likelihood ratio test itself ──────────────────────────────────────────
result = model.lrt_result(logl_full=logl_1, logl_reduced=logl_cold, df=1)
check(
    "dropping the true-null covariate x3 gives a non-significant LRT here",
    result["p_value"] > 0.05,
    f"LR={result['lr_statistic']:.4f} p={result['p_value']:.4f}",
)
check(
    "LR statistic is never negative, even from finite-precision optimization",
    result["lr_statistic"] >= 0.0,
)

# Dropping a real predictor (x2) must give a tiny p-value.
beta_drop_x2, logl_drop_x2, _, conv_x2 = federated_fit(
    X[:, [0, 1, 3]], y, [slice(0, n)], np.zeros(3)
)
result_x2 = model.lrt_result(logl_full=logl_1, logl_reduced=logl_drop_x2, df=1)
check(
    "dropping a real predictor (x2, true effect -0.6) gives a tiny p-value",
    conv_x2 and result_x2["p_value"] < 1e-10,
    f"LR={result_x2['lr_statistic']:.2f} p={result_x2['p_value']:.2e}",
)

# ── pool_moments reproduces the pooled mean/sd from per-node sums ────────────
node_results = []
for sl in [slice(0, 1000), slice(1000, 2000), slice(2000, 3000)]:
    node_results.append(
        {
            "n": sl.stop - sl.start,
            "n_event": int(y[sl].sum()),
            "n_non_event": int((1 - y[sl]).sum()),
            "sums": {"x1": float(x1[sl].sum()), "x2": float(x2[sl].sum())},
            "sum_sqs": {"x1": float((x1[sl] ** 2).sum()), "x2": float((x2[sl] ** 2).sum())},
        }
    )
pooled = model.pool_moments(node_results, ["x1", "x2"])
check(
    "pool_moments reproduces the true pooled mean and sd",
    abs(pooled["means"]["x1"] - x1.mean()) < 1e-9
    and abs(pooled["sds"]["x1"] - x1.std(ddof=0)) < 1e-9
    and pooled["n_total"] == n
    and pooled["n_event"] + pooled["n_non_event"] == n,
)

# ── newton_step raises a clear error on a singular information matrix ───────
singular_info = np.zeros((3, 3))
try:
    model.newton_step(np.zeros(3), singular_info)
    check("newton_step raises on a singular information matrix", False)
except ValueError as exc:
    check("newton_step raises a clear error on a singular information matrix", True, str(exc)[:60])

# ── Type-I error and power by simulation (matches the pure federated_fit) ────
print()
rng = np.random.default_rng(7)
rejections, trials = 0, 250
for _ in range(trials):
    xa, xb, xc = rng.normal(0, 1, 800), rng.normal(0, 1, 800), rng.normal(0, 1, 800)
    yy = (rng.random(800) < model.sigmoid(-0.2 + 0.9 * xa - 0.5 * xb)).astype(float)
    Xf = np.column_stack([np.ones(800), xa, xb, xc])
    Xr = np.column_stack([np.ones(800), xa, xb])
    _, ll_f, _, cf = federated_fit(Xf, yy, [slice(0, 800)], np.zeros(4))
    _, ll_r, _, cr = federated_fit(Xr, yy, [slice(0, 800)], np.zeros(3))
    if cf and cr and model.lrt_result(ll_f, ll_r, df=1)["p_value"] < 0.05:
        rejections += 1
check(
    "Type-I error for a true-null covariate is close to the nominal 5% "
    "(the classical LRT chi-square approximation is known to run mildly "
    "conservative at finite n; anything far above 5% would be the real concern)",
    rejections / trials <= 0.08,
    f"empirical alpha={rejections/trials:.4f}",
)

rejections, trials = 0, 150
for _ in range(trials):
    xa, xb = rng.normal(0, 1, 800), rng.normal(0, 1, 800)
    yy = (rng.random(800) < model.sigmoid(-0.2 + 0.9 * xa - 0.6 * xb)).astype(float)
    Xf = np.column_stack([np.ones(800), xa, xb])
    Xr = np.column_stack([np.ones(800), xa])
    _, ll_f, _, cf = federated_fit(Xf, yy, [slice(0, 800)], np.zeros(3))
    _, ll_r, _, cr = federated_fit(Xr, yy, [slice(0, 800)], np.zeros(2))
    if cf and cr and model.lrt_result(ll_f, ll_r, df=1)["p_value"] < 0.05:
        rejections += 1
check(
    "power for a real predictor (true effect -0.6, n=800) is high",
    rejections / trials > 0.8,
    f"empirical power={rejections/trials:.4f}",
)

print()
print("FAILURES:", failures if failures else "none")
sys.exit(1 if failures else 0)
