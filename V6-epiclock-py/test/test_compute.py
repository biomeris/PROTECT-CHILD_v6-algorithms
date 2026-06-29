"""
Run this script to test you compute function locally (without building a Docker image)
using the mock client.

Run as:

    python test_compute.py

Make sure to do so in an environment where `vantage6-algorithm-tools` is
installed. This can be done by running:

    pip install vantage6-algorithm-tools
"""
import pandas as pd

from vantage6.algorithm.mock.network import MockNetwork
from pathlib import Path

# get path of current directory
current_path = Path(__file__).parent

# Load test data - using the beta values from test_data files
data1 = pd.read_csv(current_path / "test_data_1.csv")
data2 = pd.read_csv(current_path / "test_data_2.csv")
data3 = pd.read_csv(current_path / "test_data_3.csv")

DATABASE_LABEL = "default"

# Create a MockNetwork with test datasets for three organizations
network = MockNetwork(
    datasets=[
        {DATABASE_LABEL: {"database": data1}},
        {DATABASE_LABEL: {"database": data2}},
        {DATABASE_LABEL: {"database": data3}},
    ],
    module_name="epiclock_v5"
)

# Get the client to interact with the MockNetwork
client = network.user_client

organizations = client.organization.list()
print(organizations)
org_ids = [organization["id"] for organization in organizations]

# Use the mock dataframe id created by the MockNetwork.
# The mock task API expects a dataframe reference in the form
# {'type': 'dataframe', 'dataframe_id': <id>}.
mock_dataframes = client.dataframe.list()
if not mock_dataframes:
    raise RuntimeError("No mock dataframe available for task execution")
DATAFRAME_ID = mock_dataframes[0]["id"]


def _print_table(title: str, df: pd.DataFrame):
    """Print a pandas DataFrame as a nicely formatted table."""
    print(f"\n{title}")
    print(df.to_string())


def _print_central_results(results: dict):
    """Print central aggregation results in table form."""
    print("\nCentral aggregation results:")

    # Global aggregates (weighted means and pooled std)
    if "global_aggregates" in results and results["global_aggregates"]:
        df_global = pd.DataFrame.from_dict(results["global_aggregates"], orient="index")
        _print_table("Global aggregates", df_global)

    # Organization-level breakdown
    if "organization_breakdown" in results and results["organization_breakdown"]:
        for org_id, org_stats in results["organization_breakdown"].items():
            df_org = pd.DataFrame.from_dict(org_stats, orient="index")
            _print_table(f"Organization {org_id} breakdown", df_org)

    # Individual results (optional)
    if "individual_results" in results and results["individual_results"]:
        for org_id, org_indiv in results["individual_results"].items():
            for clock, sample_ages in org_indiv.items():
                df_indiv = pd.DataFrame.from_dict(sample_ages, orient="index", columns=[clock])
                _print_table(f"Organization {org_id} individual ages ({clock})", df_indiv)


# Run the central method on 1 node and get the results
central_task = client.task.create(
    method="central_function",
    arguments={
        "lista_relojes": ["horvath2013", "hannum", "pcphenoage"],
        "return_individual_results": False,
    },
    organizations=[org_ids[0]],
    databases=[{"type": "dataframe", "dataframe_id": DATAFRAME_ID}],
)
results = client.wait_for_results(central_task.get("id"))
_print_central_results(results)

# Run the federated method for all organizations
task = client.task.create(
    method="federated_function",
    arguments={
        "lista_relojes": ["horvath2013", "hannum", "pcphenoage"],
        "return_individual_results": False
    },
    organizations=org_ids,
    databases=[{"type": "dataframe", "dataframe_id": DATAFRAME_ID}],
)
print(task)

# Get the results from the task
results = client.wait_for_results(task.get("id"))
print("\nPartial aggregation results from each organization:")
for i, result in enumerate(results):
    if isinstance(result, dict) and "aggregated" in result:
        df_partial = pd.DataFrame.from_dict(result["aggregated"], orient="index")
        _print_table(f"Organization {i} aggregated results", df_partial)
        
        # Print individual results if present
        if "individual" in result and result["individual"]:
            for clock, sample_ages in result["individual"].items():
                df_indiv = pd.DataFrame.from_dict(sample_ages, orient="index", columns=[clock])
                _print_table(f"Organization {i} individual ages ({clock})", df_indiv)
    else:
        print(f"Organization {i}: {result}")

# Create and display the organization means table
print("\n" + "=" * 80)
print("ORGANIZATION MEANS TABLE (ALL CLOCKS)")
print("=" * 80)

# Build a structure compatible with create_organization_means_table
from epiclock_v5.central import create_organization_means_table, format_organization_summary

aggregated_data = {}
global_aggs = {}

for i, result in enumerate(results):
    if isinstance(result, dict) and "aggregated" in result:
        aggregated_data[str(i)] = result["aggregated"]
        
        # Build global aggregates (simple average for display)
        for clock, stats in result["aggregated"].items():
            if clock not in global_aggs:
                global_aggs[clock] = {"weighted_mean": 0, "std_pooled": 0, "total_n": 0, "n_organizations": 0}

# Create mock central results structure
mock_central_results = {
    "organization_breakdown": aggregated_data,
    "global_aggregates": global_aggs
}

# Calculate proper global aggregates
for clock in global_aggs.keys():
    org_means = []
    org_stds = []
    org_ns = []
    
    for org_id, org_stats in aggregated_data.items():
        if clock in org_stats and org_stats[clock]["N"] > 0:
            org_means.append(org_stats[clock]["mean"])
            org_stds.append(org_stats[clock]["std"])
            org_ns.append(org_stats[clock]["N"])
    
    if org_ns:
        import numpy as np
        total_n = sum(org_ns)
        weighted_mean = np.average(org_means, weights=org_ns)
        pooled_variance = sum(
            (n - 1) * (s ** 2) for n, s in zip(org_ns, org_stds)
        ) / (total_n - len(org_ns))
        pooled_std = np.sqrt(pooled_variance) if pooled_variance >= 0 else np.nan
        
        global_aggs[clock] = {
            "weighted_mean": float(weighted_mean),
            "std_pooled": float(pooled_std),
            "total_n": int(total_n),
            "n_organizations": len(org_ns)
        }

means_table = create_organization_means_table(mock_central_results)
print(means_table.to_string())
print("=" * 80)
