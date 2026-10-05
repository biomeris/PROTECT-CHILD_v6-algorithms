"""
Run this script to test your compute function locally (without building a
Docker image) using the mock client.

Run as:

    python test_compute.py

Make sure to do so in an environment where `vantage6-algorithm-tools` is
installed. This can be done by running:

    pip install vantage6-algorithm-tools

Beyond the original happy-path smoke test, this file also regression-tests
every edge case the algorithm was hardened against: N-threshold exclusion,
categorical auto-encoding, the small-risk-set privacy warning, perfect
separation, and the upfront argument/column validation errors. Each test
prints its result and then asserts on it, so a broken change fails loudly
instead of just looking different in the printed output.
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

from vantage6.algorithm.mock.network import MockNetwork

current_path = Path(__file__).parent
DATABASE_LABEL = "default"


def _make_network(datasets: list[pd.DataFrame]) -> MockNetwork:
    """Build a MockNetwork with one organization per dataframe in ``datasets``."""
    return MockNetwork(
        datasets=[{DATABASE_LABEL: {"database": d}} for d in datasets],
        module_name="v6-coxph-py",
    )


def _run_coxph(network: MockNetwork, **arguments) -> dict:
    client = network.user_client
    org_ids = [o["id"] for o in client.organization.list()]
    arguments.setdefault("organizations_to_include", org_ids)
    task = client.task.create(
        method="coxph",
        arguments=arguments,
        organizations=[org_ids[0]],
        databases=[
            {"type": "dataframe", "dataframe_id": network.hq.dataframes[0]["id"]}
        ],
    )
    return client.wait_for_results(task.get("id"))[0]


def _expect_validation_error(label: str, dataset: pd.DataFrame | None = None, **arguments) -> None:
    """Run coxph expecting the central function to raise.

    The mock client converts any exception raised inside a task (including
    our InputError validation) into a hard `exit(1)`, so we catch SystemExit
    here rather than the original exception type - this mirrors what a real
    node does with a failing task run.
    """
    print(f"\n=== validation: {label} ===")
    df = dataset if dataset is not None else pd.DataFrame(
        {"time": [5, 10, 15, 20], "event": [1, 0, 1, 1], "x": [1, 2, 3, 4]}
    )
    network = _make_network([df, df])
    try:
        _run_coxph(network, **arguments)
    except SystemExit:
        print("PASS (raised as expected)")
        return
    raise AssertionError(f"Expected '{label}' to raise, but it succeeded.")


def test_happy_path() -> None:
    """The real HEAD-NECK-RADIOMICS dataset, split across 2 nodes: numeric and
    boolean covariates only, no categorical vars, no exclusions expected.

    The expected AIC was cross-checked against a centralized
    lifelines.CoxPHFitter fit on the same pooled data (see conversation
    history / privacy.rst for how this was validated).
    """
    print("\n=== test_happy_path ===")
    d1 = pd.read_csv(current_path / "test_data_1.csv")
    d2 = pd.read_csv(current_path / "test_data_2.csv")
    network = _make_network([d1, d2])
    result = _run_coxph(
        network,
        time_col="overall_survival_in_days",
        outcome_col="event_overall_survival",
        expl_vars=["clin_n_1", "index_tumour_location_oropharynx"],
    )
    print(result)
    assert result["converged"] is True
    assert result["excluded_organizations"] == []
    assert result["warnings"] == []
    assert abs(result["aic"] - 661.152479428255) < 1e-6
    print("PASS")


def _make_survival_df(n: int, seed: int, event_rate: float = 2 / 3) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    x = rng.normal(size=n)
    time = rng.exponential(scale=10, size=n) + 1
    event = np.zeros(n, dtype=int)
    event[rng.choice(n, size=int(n * event_rate), replace=False)] = 1
    return pd.DataFrame({"time": time, "event": event, "x": x})


def test_n_threshold_exclusion() -> None:
    """A node with too few local events (<= COXPH_MINIMUM_EVENTS) is excluded;
    the fit should still succeed on the remaining organization(s)."""
    print("\n=== test_n_threshold_exclusion ===")
    small = _make_survival_df(8, seed=1, event_rate=0.5)  # <=10 events -> excluded
    big = _make_survival_df(60, seed=2)
    network = _make_network([small, big])
    org_ids = [o["id"] for o in network.user_client.organization.list()]
    result = _run_coxph(network, time_col="time", outcome_col="event", expl_vars=["x"])
    print(result)
    assert result["excluded_organizations"] == [org_ids[0]]
    assert result["included_organizations"] == [org_ids[1]]
    assert result["model"] is not None
    print("PASS")


def test_categorical_encoding() -> None:
    """A category-dtype covariate is auto one-hot encoded consistently across
    both nodes, with the alphabetically-first level dropped as reference."""
    print("\n=== test_categorical_encoding ===")

    def make_df(n: int, seed: int) -> pd.DataFrame:
        rng = np.random.default_rng(seed)
        stage = pd.Categorical(
            rng.choice(["I", "II", "III"], size=n), categories=["I", "II", "III"]
        )
        x = rng.normal(size=n)
        time = rng.exponential(scale=10, size=n) + 1
        event = rng.integers(0, 2, size=n)
        return pd.DataFrame({"time": time, "event": event, "stage": stage, "x": x})

    network = _make_network([make_df(60, 1), make_df(60, 2)])
    result = _run_coxph(
        network, time_col="time", outcome_col="event", expl_vars=["stage", "x"]
    )
    print(result)
    model = json.loads(result["model"])
    assert set(model["Coef"].keys()) == {"stage[T.II]", "stage[T.III]", "x"}
    print("PASS")


def test_small_risk_set_warning() -> None:
    """A lone straggler far out in time creates a tiny global risk set at the
    tail, which should trigger the COXPH_MINIMUM_RISK_SET_SIZE warning."""
    print("\n=== test_small_risk_set_warning ===")

    def make_df(n: int, seed: int, tail_boost: bool = False) -> pd.DataFrame:
        rng = np.random.default_rng(seed)
        x = rng.normal(size=n)
        time = rng.exponential(scale=10, size=n) + 1
        if tail_boost:
            time[0] = 500.0
        event = np.ones(n, dtype=int)
        event[rng.choice(n, size=n // 3, replace=False)] = 0
        return pd.DataFrame({"time": time, "event": event, "x": x})

    network = _make_network([make_df(30, 1, tail_boost=True), make_df(30, 2)])
    result = _run_coxph(network, time_col="time", outcome_col="event", expl_vars=["x"])
    print(result)
    assert any("risk set" in w.lower() for w in result["warnings"])
    print("PASS")


def test_perfect_separation_warning() -> None:
    """A covariate that perfectly predicts the event should be caught by the
    perfect-separation / overflow guards rather than crash or silently return
    a misleadingly confident model."""
    print("\n=== test_perfect_separation_warning ===")

    def make_df(n: int, seed: int) -> pd.DataFrame:
        rng = np.random.default_rng(seed)
        time = rng.exponential(scale=10, size=n) + 1
        # x perfectly predicts the event: x=1 -> always event, x=0 -> never.
        x = rng.integers(0, 2, size=n)
        event = x.copy()
        return pd.DataFrame({"time": time, "event": event, "x": x})

    network = _make_network([make_df(40, 1), make_df(40, 2)])
    result = _run_coxph(network, time_col="time", outcome_col="event", expl_vars=["x"])
    print(result)
    # Either the iteration stops early with no model (overflow/singular-Fisher
    # guard) or it "converges" to a suspiciously large coefficient that trips
    # the perfect-separation warning - both are acceptable, silent confident
    # nonsense is not.
    assert (result["model"] is None) or (len(result["warnings"]) > 0)
    print("PASS")


def test_argument_validation_errors() -> None:
    """Upfront argument validation should reject bad expl_vars/time/outcome
    combinations before spending any federated rounds."""
    _expect_validation_error(
        "empty expl_vars", time_col="time", outcome_col="event", expl_vars=[]
    )
    _expect_validation_error(
        "duplicate expl_vars",
        time_col="time",
        outcome_col="event",
        expl_vars=["x", "x"],
    )
    _expect_validation_error(
        "time_col == outcome_col",
        time_col="event",
        outcome_col="event",
        expl_vars=["x"],
    )
    _expect_validation_error(
        "expl_vars contains time_col",
        time_col="time",
        outcome_col="event",
        expl_vars=["time", "x"],
    )
    _expect_validation_error(
        "expl_vars contains outcome_col",
        time_col="time",
        outcome_col="event",
        expl_vars=["event", "x"],
    )


def test_time_column_validation_errors() -> None:
    """time_col must be non-boolean, non-null, and non-negative."""
    bool_df = pd.DataFrame(
        {"time": [True, False, True, True], "event": [1, 0, 1, 1], "x": [1, 2, 3, 4]}
    )
    nan_df = pd.DataFrame(
        {"time": [5, 10, np.nan, 20], "event": [1, 0, 1, 1], "x": [1, 2, 3, 4]}
    )
    negative_df = pd.DataFrame(
        {"time": [5, -10, 15, 20], "event": [1, 0, 1, 1], "x": [1, 2, 3, 4]}
    )

    _expect_validation_error(
        "boolean time_col",
        dataset=bool_df,
        time_col="time",
        outcome_col="event",
        expl_vars=["x"],
    )
    _expect_validation_error(
        "NaN in time_col",
        dataset=nan_df,
        time_col="time",
        outcome_col="event",
        expl_vars=["x"],
    )
    _expect_validation_error(
        "negative time_col",
        dataset=negative_df,
        time_col="time",
        outcome_col="event",
        expl_vars=["x"],
    )


if __name__ == "__main__":
    test_happy_path()
    test_n_threshold_exclusion()
    test_categorical_encoding()
    test_small_risk_set_warning()
    test_perfect_separation_warning()
    test_argument_validation_errors()
    test_time_column_validation_errors()
    print("\nAll v6-coxph-py regression tests passed.")
