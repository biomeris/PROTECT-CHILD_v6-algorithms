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


def _unwrap_central(results):
    """The central summary method returns JSON strings for table fields."""
    for key in (
        "numeric",
        "categorical",
        "counts_unique_values",
        "num_complete_rows_per_node",
    ):
        if isinstance(results[0].get(key), str):
            results[0][key] = json.loads(results[0][key])
    if isinstance(results[0].get("num_complete_rows_per_node"), dict):
        results[0]["num_complete_rows_per_node"] = list(
            results[0]["num_complete_rows_per_node"].values()
        )
    return results


central_task = client.task.create(
    method="summary",
    arguments={
        "columns": columns,
    },
    organizations=[org_ids[0]],
    databases=DATABASES,
)
results = client.wait_for_results(central_task.get("id"))
