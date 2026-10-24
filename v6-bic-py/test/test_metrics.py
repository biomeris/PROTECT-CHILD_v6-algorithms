"""Standalone BIC formula test, without Vantage6."""
from pathlib import Path
import importlib.util
import math

metric_path = Path(__file__).parent.parent / "v6-bic-py" / "metrics.py"
spec = importlib.util.spec_from_file_location("metrics_under_test", metric_path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
assert math.isclose(module.compute_bic(-12.5, 3, 20), 25.0 + 3 * math.log(20))
assert module.compute_bic(0.0, 0, 1) == 0.0
for call in (
    lambda: module.compute_bic(float("nan"), 2, 10),
    lambda: module.compute_bic(-12.5, -1, 10),
    lambda: module.compute_bic(-5.0, 2, 0),
):
    try:
        call()
        raise AssertionError("Expected invalid input to raise ValueError")
    except ValueError:
        pass
print("PASS: BIC metric formula and validation")
