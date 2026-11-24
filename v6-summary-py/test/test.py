"""
Run this script to test your algorithm locally (without building a Docker
image) using the mock client.

Run as:

    python test.py

Make sure to do so in an environment where `vantage6-algorithm-tools` is
installed. This can be done by running:

    pip install vantage6-algorithm-tools
"""

import json
import os
import pytest
import pandas as pd
import numpy as np

from vantage6.algorithm.mock.network import MockNetwork
from vantage6.algorithm.tools.exceptions import (
    InputError,
)

# Create fake data. Three columns with random numbers, two columns with factors
np.random.seed(123)
columns = ["A", "B", "C", "D", "E", "F"]
size = 1000

data = pd.DataFrame(
    {
        "A": np.random.choice(range(1, 11), size=size, replace=True),
        "B": np.random.choice([1, 2, 3, np.nan], size=size, replace=True),
        "C": np.random.choice(list(range(6, 20)) + [np.nan], size=size, replace=True),
        "D": np.random.choice(["1", "2", "3", "4"], size=size, replace=True),
        "E": np.random.choice(["female", "male", None], size=size, replace=True),
        "F": np.random.choice(["other"], size=size, replace=True),
    }
)

# Split the dataframe into two sets
n_rows = len(data)
size_per_df = int(size / 2)

df1 = data.iloc[:size_per_df, :]
df2 = data.iloc[size_per_df:, :]

DATABASE_LABEL = "Database"

## Mock network
network = MockNetwork(
    datasets=[
        {DATABASE_LABEL: {"database": df1}},
        {DATABASE_LABEL: {"database": df2}},
    ],
    module_name="v6-summary-py",
)
client = network.user_client

DATABASES = [{"type": "dataframe", "dataframe_id": network.hq.dataframes[0]["id"]}]

# list mock organizations
organizations = client.organization.list()
org_ids = [organization["id"] for organization in organizations]


# def _unwrap_central(results):
#     """The central summary method returns JSON strings for table fields."""
#     for key in (
#         "numeric",
#         "categorical",
#         "counts_unique_values",
#         "num_complete_rows_per_node",
#     ):
#         if isinstance(results[0].get(key), str):
#             results[0][key] = json.loads(results[0][key])
#     if isinstance(results[0].get("num_complete_rows_per_node"), dict):
#         results[0]["num_complete_rows_per_node"] = list(
#             results[0]["num_complete_rows_per_node"].values()
#         )
#     return results


central_task = client.task.create(
    method="summary",
    arguments={
        "columns": columns,
    },
    organizations=[org_ids[0]],
    databases=DATABASES,
)
# results = _unwrap_central(client.wait_for_results(central_task.get("id")))
results = client.wait_for_results(central_task.get("id"))
import json

with open("risultati.json", "w") as f:
    json.dump(results, f, indent=2)


# def test_central_all_columns():
#     """test central method on all columns"""
#     central_task = client.task.create(
#         method="summary",
#         arguments={
#             "columns": columns,
#         },
#         organizations=[org_ids[0]],
#         databases=DATABASES,
#     )
#     results = _unwrap_central(client.wait_for_results(central_task.get("id")))

#     # check results from central task
#     num_complete_rows_per_node = results[0]["num_complete_rows_per_node"]
#     assert num_complete_rows_per_node[0] == df1.dropna().shape[0]
#     assert num_complete_rows_per_node[1] == df2.dropna().shape[0]
#     numeric_results = results[0]["numeric"]
#     assert numeric_results["A"]["count"] == data["A"].count()
#     assert numeric_results["B"]["count"] == data["B"].count()
#     assert numeric_results["C"]["count"] == data["C"].count()
#     assert numeric_results["A"]["min"] == data["A"].min()
#     assert numeric_results["B"]["min"] == data["B"].min()
#     assert numeric_results["C"]["min"] == data["C"].min()
#     assert numeric_results["A"]["max"] == data["A"].max()
#     assert numeric_results["B"]["max"] == data["B"].max()
#     assert numeric_results["C"]["max"] == data["C"].max()
#     assert numeric_results["A"]["missing"] == data["A"].isna().sum()
#     assert numeric_results["B"]["missing"] == data["B"].isna().sum()
#     assert numeric_results["C"]["missing"] == data["C"].isna().sum()
#     assert numeric_results["A"]["sum"] == data["A"].sum()
#     assert numeric_results["B"]["sum"] == data["B"].sum()
#     assert numeric_results["C"]["sum"] == data["C"].sum()
#     assert numeric_results["A"]["mean"] == pytest.approx(data["A"].mean())
#     assert numeric_results["B"]["mean"] == pytest.approx(data["B"].mean())
#     assert numeric_results["C"]["mean"] == pytest.approx(data["C"].mean())
#     assert numeric_results["A"]["std"] == pytest.approx(data["A"].std())
#     assert numeric_results["B"]["std"] == pytest.approx(data["B"].std())
#     assert numeric_results["C"]["std"] == pytest.approx(data["C"].std())
#     categorical_results = results[0]["categorical"]
#     assert categorical_results["D"]["count"] == data["D"].count()
#     assert categorical_results["E"]["count"] == data["E"].count()
#     assert categorical_results["F"]["count"] == data["F"].count()
#     assert categorical_results["D"]["missing"] == data["D"].isna().sum()
#     assert categorical_results["E"]["missing"] == data["E"].isna().sum()
#     assert categorical_results["F"]["missing"] == data["F"].isna().sum()
#     counts_unique_values = results[0]["counts_unique_values"]
#     assert counts_unique_values["D"]["1"] == data["D"].value_counts().get("1", 0)
#     assert counts_unique_values["D"]["2"] == data["D"].value_counts().get("2", 0)
#     assert counts_unique_values["D"]["3"] == data["D"].value_counts().get("3", 0)
#     assert counts_unique_values["D"]["4"] == data["D"].value_counts().get("4", 0)
#     assert counts_unique_values["E"]["female"] == data["E"].value_counts().get(
#         "female", 0
#     )
#     assert counts_unique_values["E"]["male"] == data["E"].value_counts().get("male", 0)
#     assert counts_unique_values["F"]["other"] == data["F"].value_counts().get(
#         "other", 0
#     )


# def test_central_single_numeric_column():
#     """ensure that we can run a task for a single numeric column"""
#     task = client.task.create(
#         method="summary",
#         arguments={
#             "columns": ["A"],
#         },
#         organizations=[org_ids[0]],
#         databases=DATABASES,
#     )
#     results = _unwrap_central(client.wait_for_results(task.get("id")))
#     assert results[0]["numeric"]["A"]["count"] == data["A"].count()
#     assert results[0]["numeric"]["A"]["min"] == data["A"].min()
#     assert results[0]["numeric"]["A"]["max"] == data["A"].max()
#     assert results[0]["numeric"]["A"]["missing"] == data["A"].isna().sum()
#     assert results[0]["numeric"]["A"]["sum"] == data["A"].sum()
#     assert results[0]["categorical"] == {}
#     assert results[0]["counts_unique_values"] == {}
#     assert results[0]["num_complete_rows_per_node"][0] == df1["A"].dropna().shape[0]
#     assert results[0]["num_complete_rows_per_node"][1] == df2["A"].dropna().shape[0]


# def test_central_single_categorical_column():
#     """ensure that we can run a task for a single categorical column"""
#     task = client.task.create(
#         method="summary",
#         arguments={
#             "columns": ["E"],
#         },
#         organizations=[org_ids[0]],
#         databases=DATABASES,
#     )
#     results = _unwrap_central(client.wait_for_results(task.get("id")))
#     assert results[0]["categorical"]["E"]["count"] == data["E"].count()
#     assert results[0]["categorical"]["E"]["missing"] == data["E"].isna().sum()
#     assert results[0]["numeric"] == {}
#     assert results[0]["counts_unique_values"]["E"]["female"] == data[
#         "E"
#     ].value_counts().get("female", 0)
#     assert results[0]["counts_unique_values"]["E"]["male"] == data[
#         "E"
#     ].value_counts().get("male", 0)
#     assert results[0]["num_complete_rows_per_node"][0] == df1["E"].dropna().shape[0]
#     assert results[0]["num_complete_rows_per_node"][1] == df2["E"].dropna().shape[0]


# def test_central_non_existing_column():
#     """check that non-existing columns give an error"""
#     with pytest.raises((InputError, SystemExit)):
#         task = client.task.create(
#             method="summary",
#             arguments={
#                 "columns": ["non-existing-column"],
#             },
#             organizations=[org_ids[0]],
#             databases=DATABASES,
#         )
#         client.wait_for_results(task.get("id"))


# def test_partial_non_existing_column():
#     """Test that non-existing columns give an error"""
#     with pytest.raises((InputError, SystemExit)):
#         task = client.task.create(
#             method="summary_per_data_station",
#             arguments={
#                 "columns": ["non-existing-column"],
#             },
#             organizations=[org_ids[0]],
#             databases=DATABASES,
#         )
#         client.wait_for_results(task.get("id"))


# def test_privacy_threshold_categorical():
#     """Test that counts of unique values in a categorical column are not returned if the
#     privacy threshold is violated
#     """
#     os.environ["SUMMARY_PRIVACY_THRESHOLD"] = "1000"
#     task = client.task.create(
#         method="summary",
#         arguments={
#             "columns": ["D"],
#         },
#         organizations=[org_ids[0]],
#         databases=DATABASES,
#     )
#     results = _unwrap_central(client.wait_for_results(task.get("id")))
#     os.environ["SUMMARY_PRIVACY_THRESHOLD"] = "5"
#     assert results[0]["counts_unique_values"]["D"] == {}


# def test_convert_categorical_to_numeric():
#     """Test that we can convert a categorical column to a numeric column"""
#     # note that column D is a categorical column that consists of "1", "2", "3", "4"
#     # so it should be convertible to a numeric column
#     central_task = client.task.create(
#         method="summary",
#         arguments={
#             "columns": ["D"],
#             "numeric_columns": ["D"],
#         },
#         organizations=[org_ids[0]],
#         databases=DATABASES,
#     )
#     results = _unwrap_central(client.wait_for_results(central_task.get("id")))
#     numeric_data_D = data["D"].astype(int)
#     assert results[0]["numeric"]["D"]["count"] == numeric_data_D.count()
#     assert results[0]["numeric"]["D"]["min"] == numeric_data_D.min()
#     assert results[0]["numeric"]["D"]["max"] == numeric_data_D.max()
#     assert results[0]["numeric"]["D"]["missing"] == numeric_data_D.isna().sum()
#     assert results[0]["numeric"]["D"]["sum"] == numeric_data_D.sum()
#     assert results[0]["categorical"] == {}
#     assert results[0]["counts_unique_values"] == {}
#     assert results[0]["num_complete_rows_per_node"][0] == df1["D"].dropna().shape[0]
#     assert results[0]["num_complete_rows_per_node"][1] == df2["D"].dropna().shape[0]


# def test_partial_all_columns():
#     """Verify results from partial task"""
#     task = client.task.create(
#         method="summary_per_data_station",
#         arguments={
#             "columns": columns,
#         },
#         organizations=org_ids,
#         databases=DATABASES,
#     )

#     # Get the results from the task
#     results = client.wait_for_results(task.get("id"))

#     # verify results from partial tasks
#     for node_idx in [0, 1]:
#         check_df = data.iloc[size_per_df * node_idx : size_per_df * (node_idx + 1)]

#         num_complete_rows_per_node = results[node_idx]["num_complete_rows_per_node"]
#         assert num_complete_rows_per_node == check_df.dropna().shape[0]

#         numeric_results = results[node_idx]["numeric"]
#         assert numeric_results["A"]["count"] == check_df["A"].count()
#         assert numeric_results["B"]["count"] == check_df["B"].count()
#         assert numeric_results["C"]["count"] == check_df["C"].count()
#         assert numeric_results["A"]["min"] == check_df["A"].min()
#         assert numeric_results["B"]["min"] == check_df["B"].min()
#         assert numeric_results["C"]["min"] == check_df["C"].min()
#         assert numeric_results["A"]["max"] == check_df["A"].max()
#         assert numeric_results["B"]["max"] == check_df["B"].max()
#         assert numeric_results["C"]["max"] == check_df["C"].max()
#         assert numeric_results["A"]["missing"] == check_df["A"].isna().sum()
#         assert numeric_results["B"]["missing"] == check_df["B"].isna().sum()
#         assert numeric_results["C"]["missing"] == check_df["C"].isna().sum()
#         assert numeric_results["A"]["sum"] == check_df["A"].sum()
#         assert numeric_results["B"]["sum"] == check_df["B"].sum()
#         assert numeric_results["C"]["sum"] == check_df["C"].sum()

#         categorical_results = results[node_idx]["categorical"]
#         assert categorical_results["D"]["count"] == check_df["D"].count()
#         assert categorical_results["E"]["count"] == check_df["E"].count()
#         assert categorical_results["F"]["count"] == check_df["F"].count()
#         assert categorical_results["D"]["missing"] == check_df["D"].isna().sum()
#         assert categorical_results["E"]["missing"] == check_df["E"].isna().sum()
#         assert categorical_results["F"]["missing"] == check_df["F"].isna().sum()

#         counts_unique_values = results[node_idx]["counts_unique_values"]
#         assert counts_unique_values["D"]["1"] == check_df["D"].value_counts().get(
#             "1", 0
#         )
#         assert counts_unique_values["D"]["2"] == check_df["D"].value_counts().get(
#             "2", 0
#         )
#         assert counts_unique_values["D"]["3"] == check_df["D"].value_counts().get(
#             "3", 0
#         )
#         assert counts_unique_values["D"]["4"] == check_df["D"].value_counts().get(
#             "4", 0
#         )
#         assert counts_unique_values["E"]["female"] == check_df["E"].value_counts().get(
#             "female", 0
#         )
#         assert counts_unique_values["E"]["male"] == check_df["E"].value_counts().get(
#             "male", 0
#         )
#         assert counts_unique_values["F"]["other"] == check_df["F"].value_counts().get(
#             "other", 0
#         )


# def test_central_quantiles_per_node():
#     """Median, quartiles, IQR and extra percentiles are returned per node"""
#     central_task = client.task.create(
#         method="summary",
#         arguments={"columns": columns, "percentiles": [0.1, 0.9]},
#         organizations=[org_ids[0]],
#         databases=DATABASES,
#     )
#     results = client.wait_for_results(central_task.get("id"))
#     rows = results[0]["quantiles_per_node"]

#     # one row per (organization, numeric column)
#     assert len(rows) == len(org_ids) * 3
#     assert sorted({r["organization_id"] for r in rows}) == sorted(org_ids)

#     # in the mock network, organization ids are 1..n in the order of the datasets
#     node_data = {org_ids[0]: df1, org_ids[1]: df2}
#     for row in rows:
#         values = node_data[row["organization_id"]][row["column"]].astype(float)
#         assert row["median"] == pytest.approx(values.median())
#         assert row["q1"] == pytest.approx(values.quantile(0.25))
#         assert row["q3"] == pytest.approx(values.quantile(0.75))
#         assert row["iqr"] == pytest.approx(
#             values.quantile(0.75) - values.quantile(0.25)
#         )
#         assert row["p10"] == pytest.approx(values.quantile(0.1))
#         assert row["p90"] == pytest.approx(values.quantile(0.9))


# def test_central_invalid_percentiles():
#     """Percentiles outside [0, 1] are rejected"""
#     with pytest.raises((InputError, SystemExit)):
#         central_task = client.task.create(
#             method="summary",
#             arguments={"columns": ["A"], "percentiles": [25]},
#             organizations=[org_ids[0]],
#             databases=DATABASES,
#         )
#         client.wait_for_results(central_task.get("id"))


# def test_partial_quantiles_with_org_id():
#     """Partial results contain the organization id and the local quantiles"""
#     task = client.task.create(
#         method="summary_per_data_station",
#         arguments={"columns": ["A", "C"]},
#         organizations=org_ids,
#         databases=DATABASES,
#     )
#     results = client.wait_for_results(task.get("id"))
#     node_data = {org_ids[0]: df1, org_ids[1]: df2}
#     for result in results:
#         df_node = node_data[result["organization_id"]]
#         assert result["quantiles"]["A"]["median"] == pytest.approx(
#             df_node["A"].median()
#         )
#         assert result["quantiles"]["C"]["iqr"] == pytest.approx(
#             df_node["C"].quantile(0.75) - df_node["C"].quantile(0.25)
#         )


def test_central_result_is_valid_json():
    """The whole central result can be saved as JSON and read back unchanged"""
    central_task = client.task.create(
        method="summary",
        arguments={"columns": columns},
        organizations=[org_ids[0]],
        databases=DATABASES,
    )
    results = client.wait_for_results(central_task.get("id"))

    # all fields are native structures, not JSON strings
    for key in (
        "numeric",
        "categorical",
        "counts_unique_values",
        "num_complete_rows_per_node",
        "quantiles_per_node",
    ):
        assert not isinstance(results[0][key], str), key

    # strict JSON (no NaN) and lossless round trip
    as_text = json.dumps(results, allow_nan=False)
    assert json.loads(as_text) == results
