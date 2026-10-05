"""
Plain unit tests for the pure Bonferroni correction math, run without any
vantage6 scaffolding (no MockNetwork, no Docker). Fast to iterate on.

Run as:

    python -m pytest test_bonferroni.py

or, without pytest installed:

    python test_bonferroni.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "v6-bonferroni-py"))

from central import _bonferroni, compute_bonferroni_correction  # noqa: E402
from vantage6.algorithm.tools.exceptions import InputError  # noqa: E402


def approx(a: float, b: float, tol: float = 1e-9) -> bool:
    return abs(a - b) < tol


def expect_input_error(**kwargs):
    try:
        compute_bonferroni_correction.__wrapped__(**kwargs)
    except InputError:
        return
    raise AssertionError(f"expected InputError for kwargs={kwargs!r}")


def test_basic_correction():
    p_values = {"a": 0.01, "b": 0.20, "c": 0.001}
    result = _bonferroni(p_values, alpha=0.05, num_tests=3)

    assert approx(result["a"]["p_adjusted"], 0.03)
    assert approx(result["b"]["p_adjusted"], 0.6)
    assert approx(result["c"]["p_adjusted"], 0.003)

    assert result["a"]["reject"] is True
    assert result["b"]["reject"] is False
    assert result["c"]["reject"] is True


def test_clamps_at_one():
    p_values = {"a": 0.9}
    result = _bonferroni(p_values, alpha=0.05, num_tests=5)
    assert result["a"]["p_adjusted"] == 1.0


def test_num_tests_override_larger_than_input():
    # Correcting for a family of 10 tests when only 2 p-values are supplied.
    p_values = {"a": 0.01, "b": 0.02}
    result = _bonferroni(p_values, alpha=0.05, num_tests=10)
    assert result["a"]["p_adjusted"] == 0.1
    assert result["b"]["p_adjusted"] == 0.2


def test_reject_boundary_uses_less_than_or_equal():
    # p_adjusted exactly equal to alpha should still count as a rejection.
    result = _bonferroni({"a": 0.05}, alpha=0.05, num_tests=1)
    assert result["a"]["reject"] is True


def test_p_value_boundaries_zero_and_one():
    result = _bonferroni({"a": 0.0, "b": 1.0}, alpha=0.05, num_tests=1)
    assert result["a"]["p_adjusted"] == 0.0
    assert result["a"]["reject"] is True
    assert result["b"]["p_adjusted"] == 1.0
    assert result["b"]["reject"] is False


def test_central_accepts_list_input():
    out = compute_bonferroni_correction.__wrapped__(
        p_values=[0.01, 0.2], alpha=0.05
    )
    assert out["num_tests"] == 2
    assert set(out["results"].keys()) == {"0", "1"}


def test_central_accepts_tuple_input():
    out = compute_bonferroni_correction.__wrapped__(p_values=(0.01, 0.2))
    assert out["num_tests"] == 2


def test_central_accepts_numpy_scalars():
    np = __import__("numpy")
    out = compute_bonferroni_correction.__wrapped__(
        p_values={"a": np.float64(0.01)}, num_tests=np.int64(3)
    )
    assert out["num_tests"] == 3
    assert approx(out["results"]["a"]["p_adjusted"], 0.03)


def test_central_rejects_invalid_p_value():
    for bad in [-0.1, 1.1, "not-a-number", float("nan"), None, True, False]:
        expect_input_error(p_values={"a": bad})


def test_central_rejects_empty_input():
    expect_input_error(p_values={})
    expect_input_error(p_values=[])


def test_central_rejects_non_collection_p_values():
    for bad in [None, "0.05", 0.05, 42]:
        expect_input_error(p_values=bad)


def test_central_rejects_bad_num_tests():
    expect_input_error(p_values={"a": 0.1}, num_tests=0)
    expect_input_error(p_values={"a": 0.1}, num_tests=-1)
    expect_input_error(p_values={"a": 0.1}, num_tests=1.5)
    expect_input_error(p_values={"a": 0.1}, num_tests=float("nan"))
    expect_input_error(p_values={"a": 0.1}, num_tests=float("inf"))


def test_central_rejects_num_tests_smaller_than_p_values_supplied():
    # Correcting for fewer tests than were actually reported would understate
    # the correction, and is very likely a caller bug.
    expect_input_error(p_values={"a": 0.1, "b": 0.2}, num_tests=1)


def test_central_rejects_bad_alpha():
    for bad in [0, 1, -0.1, 1.1, float("nan"), True]:
        expect_input_error(p_values={"a": 0.1}, alpha=bad)


if __name__ == "__main__":
    tests = [obj for name, obj in list(globals().items()) if name.startswith("test_")]
    for test in tests:
        test()
        print(f"{test.__name__}: OK")
    print(f"\n{len(tests)} tests passed.")
