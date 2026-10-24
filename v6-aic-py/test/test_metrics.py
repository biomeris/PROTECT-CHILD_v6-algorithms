"""Standalone AIC formula test: no vantage6 installation is needed."""
from pathlib import Path
import importlib.util
import math

metric_path = Path(__file__).parent.parent / "v6-aic-py" / "metrics.py"
spec = importlib.util.spec_from_file_location("metrics_under_test", metric_path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

assert math.isclose(module.compute_aic(-12.5, 3), 31.0)
assert module.compute_aic(0.0, 0) == 0.0
check = lambda: module.compute_aic(float("nan"), 2)
bad_k = lambda: module.compute_aic(-12.5, -1)
for call in (check, bad_k):
    try:
        call()
        raise AssertionError("Expected ValueError for invalid metric input")
    except ValueError:
        pass
print("PASS: AIC metric formula and validation")
