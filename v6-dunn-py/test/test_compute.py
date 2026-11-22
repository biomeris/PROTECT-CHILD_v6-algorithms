"""
Test the full algorithm locally (without building a Docker image) using the
vantage6 MockNetwork.

Run as:

    uv run pytest test/test_compute.py

The happy path runs the central ``dunn`` function on the three test datasets
and compares the result with ``scikit_posthocs.posthoc_dunn`` and
``scipy.stats.kruskal`` on the pooled data. Federated ranks are binned, so the
statistics are close to, not equal to, the exact ones. The other tests cover
node exclusion, small-group suppression, the coarse-ranks warning and argument
validation.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import scikit_posthocs as skp
from scipy.stats import kruskal

from vantage6.algorithm.mock.network import MockNetwork

current_path = Path(__file__).parent
DATABASE_LABEL = "default"


def _load_test_data() -> list[pd.DataFrame]:
    return [pd.read_csv(current_path / f"test_data_{i}.csv") for i in (1, 2, 3)]


def _make_network(datasets: list[pd.DataFrame]) -> MockNetwork:
    return MockNetwork(
        datasets=[{DATABASE_LABEL: {"database": d}} for d in datasets],
        module_name="v6-dunn-py",
    )


def _run_dunn(network: MockNetwork, **arguments) -> dict:
    client = network.user_client
    org_ids = [o["id"] for o in client.organization.list()]
    arguments.setdefault("group_column", "group")
    arguments.setdefault("column", "score")
    task = client.task.create(
        method="dunn",
        arguments=arguments,
        organizations=[org_ids[0]],
        databases=[
            {"type": "dataframe", "dataframe_id": network.hq.dataframes[0]["id"]}
        ],
    )
    return client.wait_for_results(task.get("id"))[0]


@pytest.mark.parametrize("p_adjust", ["bonferroni", "holm", "fdr_bh"])
def test_happy_path_close_to_pooled_exact_test(p_adjust):
    datasets = _load_test_data()
    network = _make_network(datasets)
    result = _run_dunn(network, p_adjust=p_adjust)

    pooled = pd.concat(datasets).dropna()
    labels = sorted(pooled.group.unique())

    assert result["included_organizations"] == network.organization_ids
    assert result["excluded_organizations"] == []
    assert result["suppressed_groups"] == {}
    assert result["warnings"] == []
    assert result["n_total"] == len(pooled)
    assert result["group_sizes"] == pooled.group.value_counts().sort_index().to_dict()
    assert result["n_rank_bins"] >= 10

    ref_h = kruskal(*[pooled.score[pooled.group == g] for g in labels]).statistic
    assert abs(result["kruskal_wallis"]["h_statistic"] / ref_h - 1) < 0.05
    assert result["kruskal_wallis"]["df"] == len(labels) - 1

    ref = skp.posthoc_dunn(pooled, val_col="score", group_col="group", p_adjust=p_adjust)
    assert len(result["comparisons"]) == len(labels) * (len(labels) - 1) // 2
    for row in result["comparisons"]:
        ref_p = ref.loc[row["group_1"], row["group_2"]]
        assert row["reject"] == (ref_p <= 0.05)
        # within a factor of 3 on p, or both negligible
        assert abs(np.log10(max(row["p_adjusted"], 1e-12)) - np.log10(max(ref_p, 1e-12))) < 0.5


def test_small_node_is_excluded():
    datasets = _load_test_data()
    tiny = datasets[0].dropna().iloc[:8]
    network = _make_network([tiny, datasets[1], datasets[2]])
    org_ids = network.organization_ids
    result = _run_dunn(network)
    assert result["excluded_organizations"] == [org_ids[0]]
    assert result["included_organizations"] == org_ids[1:]
    assert result["n_total"] == len(pd.concat(datasets[1:]).dropna())


def test_all_nodes_excluded_returns_no_comparisons():
    tiny = _load_test_data()[0].dropna().iloc[:8]
    result = _run_dunn(_make_network([tiny, tiny]))
    assert result["comparisons"] is None
    assert result["warnings"]


def test_small_group_is_suppressed_at_one_node():
    datasets = _load_test_data()
    d1 = datasets[0].dropna()
    d1 = pd.concat([d1[d1.group != "D"], d1[d1.group == "D"].iloc[:3]])
    network = _make_network([d1, datasets[1], datasets[2]])
    result = _run_dunn(network)
    assert result["suppressed_groups"] == {str(network.organization_ids[0]): ["D"]}
    expected_d = sum((d.dropna().group == "D").sum() for d in datasets[1:])
    assert result["group_sizes"]["D"] == expected_d


def test_small_sample_warns_about_coarse_ranks():
    datasets = [d.dropna().iloc[:30] for d in _load_test_data()]
    result = _run_dunn(_make_network(datasets))
    assert result["n_rank_bins"] < 10
    assert any("rank bins" in w for w in result["warnings"])


@pytest.mark.parametrize(
    "arguments",
    [
        {"column": "group"},
        {"column": ""},
        {"column": "missing"},
        {"group_column": "missing"},
        {"p_adjust": "sidak"},
        {"alpha": 1.5},
        {"alpha": True},
    ],
)
def test_invalid_arguments_fail(arguments):
    """The mock client turns an exception inside a task into ``exit(1)``."""
    with pytest.raises(SystemExit):
        _run_dunn(_make_network(_load_test_data()), **arguments)
