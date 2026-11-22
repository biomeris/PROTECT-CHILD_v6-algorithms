"""
Test the full algorithm locally (without building a Docker image) using the
vantage6 MockNetwork.

Run as:

    uv run pytest test/test_compute.py

The happy path runs the central ``manova`` function on the three test datasets
and compares every statistic with statsmodels' MANOVA on the pooled data. The
other tests cover node exclusion, small-group suppression and argument
validation.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from statsmodels.multivariate.manova import MANOVA

from vantage6.algorithm.mock.network import MockNetwork

current_path = Path(__file__).parent
DATABASE_LABEL = "default"
OUTCOMES = ["y1", "y2", "y3"]


def _load_test_data() -> list[pd.DataFrame]:
    return [pd.read_csv(current_path / f"test_data_{i}.csv") for i in (1, 2, 3)]


def _make_network(datasets: list[pd.DataFrame]) -> MockNetwork:
    return MockNetwork(
        datasets=[{DATABASE_LABEL: {"database": d}} for d in datasets],
        module_name="v6-manova-py",
    )


def _run_manova(network: MockNetwork, **arguments) -> dict:
    client = network.user_client
    org_ids = [o["id"] for o in client.organization.list()]
    task = client.task.create(
        method="manova",
        arguments=arguments,
        organizations=[org_ids[0]],
        databases=[
            {"type": "dataframe", "dataframe_id": network.hq.dataframes[0]["id"]}
        ],
    )
    return client.wait_for_results(task.get("id"))[0]


def _pooled_reference(df: pd.DataFrame) -> pd.DataFrame:
    res = MANOVA.from_formula(" + ".join(OUTCOMES) + " ~ group", data=df).mv_test()
    return res.results["group"]["stat"].astype(float)


def test_happy_path_matches_pooled_statsmodels():
    datasets = _load_test_data()
    network = _make_network(datasets)
    org_ids = network.organization_ids

    result = _run_manova(network, group_column="group", outcome_columns=OUTCOMES)

    pooled = pd.concat(datasets).dropna()
    ref = _pooled_reference(pooled)

    assert result["included_organizations"] == org_ids
    assert result["excluded_organizations"] == []
    assert result["suppressed_groups"] == {}
    assert result["warnings"] == []
    assert result["n_total"] == len(pooled)
    assert result["n_groups"] == 3
    assert result["group_sizes"] == pooled["group"].value_counts().sort_index().to_dict()
    np.testing.assert_allclose(
        result["group_means"]["B"], pooled[pooled.group == "B"][OUTCOMES].mean(), rtol=1e-12
    )

    for name, stat in result["tests"].items():
        np.testing.assert_allclose(
            [stat["value"], stat["num_df"], stat["den_df"], stat["f_value"], stat["p_value"]],
            ref.loc[name].to_numpy(),
            rtol=1e-9,
        )


def test_small_node_is_excluded():
    datasets = _load_test_data()
    tiny = datasets[0].dropna().iloc[:8]  # <= MANOVA_MINIMUM_NUMBER_OF_RECORDS
    network = _make_network([tiny, datasets[1], datasets[2]])
    org_ids = network.organization_ids

    result = _run_manova(network, group_column="group", outcome_columns=OUTCOMES)

    assert result["excluded_organizations"] == [org_ids[0]]
    assert result["included_organizations"] == org_ids[1:]
    ref = _pooled_reference(pd.concat(datasets[1:]).dropna())
    np.testing.assert_allclose(
        result["tests"]["Wilks' lambda"]["value"], ref.loc["Wilks' lambda", "Value"]
    )


def test_all_nodes_excluded_returns_no_tests():
    tiny = _load_test_data()[0].dropna().iloc[:8]
    result = _run_manova(
        _make_network([tiny, tiny]), group_column="group", outcome_columns=OUTCOMES
    )
    assert result["tests"] is None
    assert result["included_organizations"] == []
    assert result["warnings"]


def test_small_group_is_suppressed_at_one_node():
    datasets = _load_test_data()
    d1 = datasets[0].dropna()
    d1 = pd.concat([d1[d1.group != "C"], d1[d1.group == "C"].iloc[:3]])
    network = _make_network([d1, datasets[1], datasets[2]])
    org_ids = network.organization_ids

    result = _run_manova(network, group_column="group", outcome_columns=OUTCOMES)

    assert result["suppressed_groups"] == {str(org_ids[0]): ["C"]}
    pooled = pd.concat([d1[d1.group != "C"], datasets[1], datasets[2]]).dropna()
    assert result["n_total"] == len(pooled)
    ref = _pooled_reference(pooled)
    np.testing.assert_allclose(
        result["tests"]["Pillai's trace"]["value"], ref.loc["Pillai's trace", "Value"]
    )


def test_organizations_to_include_subset():
    datasets = _load_test_data()
    network = _make_network(datasets)
    org_ids = network.organization_ids
    result = _run_manova(
        network,
        group_column="group",
        outcome_columns=OUTCOMES,
        organizations_to_include=org_ids[:2],
    )
    assert result["included_organizations"] == org_ids[:2]
    assert result["n_total"] == len(pd.concat(datasets[:2]).dropna())


@pytest.mark.parametrize(
    "arguments",
    [
        {"group_column": "group", "outcome_columns": ["y1"]},
        {"group_column": "group", "outcome_columns": "y1"},
        {"group_column": "group", "outcome_columns": ["y1", "y1"]},
        {"group_column": "y1", "outcome_columns": ["y1", "y2"]},
        {"group_column": "group", "outcome_columns": ["y1", "missing"]},
        {"group_column": "missing", "outcome_columns": ["y1", "y2"]},
    ],
)
def test_invalid_arguments_fail(arguments):
    """The mock client turns an exception inside a task into ``exit(1)``."""
    with pytest.raises(SystemExit):
        _run_manova(_make_network(_load_test_data()), **arguments)
