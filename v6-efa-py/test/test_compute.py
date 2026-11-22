"""
Test the full algorithm locally (without building a Docker image) using the
vantage6 MockNetwork.

Run as:

    uv run pytest test/test_compute.py

The happy path runs the central ``efa`` function on the three test datasets and
compares the result with ``factor_analyzer`` on the pooled data. The other tests
cover the Kaiser default, node exclusion and argument validation.
"""

import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from factor_analyzer import FactorAnalyzer, calculate_bartlett_sphericity, calculate_kmo

from vantage6.algorithm.mock.network import MockNetwork

current_path = Path(__file__).parent
DATABASE_LABEL = "default"
COLUMNS = [f"x{i}" for i in range(1, 9)]


def _load_test_data() -> list[pd.DataFrame]:
    return [pd.read_csv(current_path / f"test_data_{i}.csv") for i in (1, 2, 3)]


def _make_network(datasets: list[pd.DataFrame]) -> MockNetwork:
    return MockNetwork(
        datasets=[{DATABASE_LABEL: {"database": d}} for d in datasets],
        module_name="v6-efa-py",
    )


def _run_efa(network: MockNetwork, **arguments) -> dict:
    client = network.user_client
    org_ids = [o["id"] for o in client.organization.list()]
    task = client.task.create(
        method="efa",
        arguments=arguments,
        organizations=[org_ids[0]],
        databases=[
            {"type": "dataframe", "dataframe_id": network.hq.dataframes[0]["id"]}
        ],
    )
    return client.wait_for_results(task.get("id"))[0]


def _loadings(result: dict) -> np.ndarray:
    return np.array([result["loadings"][c] for c in result["columns"]])


@pytest.mark.parametrize(
    "method,rotation", [("minres", "promax"), ("ml", "varimax"), ("minres", "oblimin")]
)
def test_happy_path_matches_pooled_factor_analyzer(method, rotation):
    datasets = _load_test_data()
    network = _make_network(datasets)
    result = _run_efa(
        network, columns=COLUMNS, n_factors=2, method=method, rotation=rotation
    )

    pooled = pd.concat(datasets).dropna()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        ref = FactorAnalyzer(n_factors=2, method=method, rotation=rotation).fit(pooled)

    assert result["included_organizations"] == network.organization_ids
    assert result["excluded_organizations"] == []
    assert result["warnings"] == []
    assert result["n_total"] == len(pooled)
    assert result["n_factors_source"] == "user"
    np.testing.assert_allclose(_loadings(result), ref.loadings_, atol=1e-5)
    np.testing.assert_allclose(
        list(result["communalities"].values()), ref.get_communalities(), atol=1e-5
    )
    np.testing.assert_allclose(
        result["correlation_matrix"], pooled.corr().to_numpy(), atol=1e-12
    )
    np.testing.assert_allclose(result["kmo"]["overall"], calculate_kmo(pooled)[1])
    np.testing.assert_allclose(
        result["bartlett"]["statistic"], calculate_bartlett_sphericity(pooled)[0]
    )
    if rotation == "varimax":
        assert result["factor_correlations"] is None
    else:
        assert np.array(result["factor_correlations"]).shape == (2, 2)


def test_two_factor_structure_is_recovered():
    """The test data has x1-x4 loading on one factor and x5-x8 on the other."""
    result = _run_efa(_make_network(_load_test_data()), columns=COLUMNS)
    assert result["n_factors_source"] == "kaiser"
    assert result["n_factors"] == 2
    L = _loadings(result)
    assert set(np.argmax(np.abs(L[:4]), axis=1)) != set(np.argmax(np.abs(L[4:]), axis=1))


def test_small_node_is_excluded():
    datasets = _load_test_data()
    tiny = datasets[0].dropna().iloc[:8]  # <= EFA_MINIMUM_NUMBER_OF_RECORDS
    network = _make_network([tiny, datasets[1], datasets[2]])
    org_ids = network.organization_ids
    result = _run_efa(network, columns=COLUMNS, n_factors=2)
    assert result["excluded_organizations"] == [org_ids[0]]
    assert result["included_organizations"] == org_ids[1:]
    assert result["n_total"] == len(pd.concat(datasets[1:]).dropna())


def test_all_nodes_excluded_returns_no_loadings():
    tiny = _load_test_data()[0].dropna().iloc[:8]
    result = _run_efa(_make_network([tiny, tiny]), columns=COLUMNS)
    assert result["loadings"] is None
    assert result["warnings"]


def test_rotation_none_string():
    result = _run_efa(
        _make_network(_load_test_data()), columns=COLUMNS, n_factors=2, rotation="none"
    )
    assert result["rotation"] is None


@pytest.mark.parametrize(
    "arguments",
    [
        {"columns": ["x1", "x2"]},
        {"columns": "x1"},
        {"columns": ["x1", "x1", "x2"]},
        {"columns": COLUMNS, "n_factors": 8},
        {"columns": COLUMNS, "n_factors": 0},
        {"columns": COLUMNS, "n_factors": 1.5},
        {"columns": COLUMNS, "method": "pca"},
        {"columns": COLUMNS, "rotation": "quartimax"},
        {"columns": [*COLUMNS[:3], "missing"]},
    ],
)
def test_invalid_arguments_fail(arguments):
    """The mock client turns an exception inside a task into ``exit(1)``."""
    with pytest.raises(SystemExit):
        _run_efa(_make_network(_load_test_data()), **arguments)
