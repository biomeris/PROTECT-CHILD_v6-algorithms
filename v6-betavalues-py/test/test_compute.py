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

from pathlib import Path
import warnings
from vantage6.algorithm.mock.network import MockNetwork

# get path of current directory
current_path = Path(__file__).parent

# The MockNetwork expects a list of datasets. Point them to folders inside this test
# directory. If these folders are missing or empty, the script will warn and fall
# back to synthetic data so the aggregation logic can still be exercised.
DATABASE_LABEL = "default"

# Create a MockNetwork for two nodes. Use the local package name `globalIDAT`.
network = MockNetwork(
    datasets=[
        {DATABASE_LABEL: {"database": str(current_path / "hospital_A"), "db_type": "folder"}},
        {DATABASE_LABEL: {"database": str(current_path / "hospital_B"), "db_type": "folder"}},
    ],
    module_name="globalIDAT"
)

# Once the network is created, we can get the client to interact with the MockNetwork.
client = network.user_client

# List mock organizations
organizations = client.organization.list()
print(organizations)
org_ids = [organization["id"] for organization in organizations]

# Import the federated implementation from the local package. For preprocessing we
# attempt to call the local preprocessing function; if that fails we generate
# synthetic preprocessed data and emit a warning.
from globalIDAT.federated import federated_impl
try:
    from globalIDAT.preprocess import data_preprocessing_function as preprocessing_impl
except Exception:
    preprocessing_impl = None

node_results = []
for folder in ["hospital_A", "hospital_B"]:
    idat_dir = current_path / folder
    print(f"\nProcessing {folder}...")

    # Require the folder to contain a CSV with preprocessed Beta/M values.
    if not idat_dir.exists() or not any(idat_dir.iterdir()):
        raise FileNotFoundError(f"No data found in {idat_dir}; please add preprocessed CSV files")

    # Look for a CSV in the folder and load it. Prefer files named 'preprocessed.csv'.
    csv_files = list(idat_dir.glob("*.csv"))
    preferred = idat_dir / "preprocessed.csv"
    if preferred.exists():
        pre_df = pd.read_csv(preferred)
    elif csv_files:
        pre_df = pd.read_csv(csv_files[0])
    else:
        # If no CSV present, try the preprocessing function if available
        if preprocessing_impl is None:
            raise RuntimeError(f"No preprocessed CSV in {idat_dir} and no preprocessing implementation available")
        pre_df = preprocessing_impl(pd.DataFrame({"idat_dir": [str(idat_dir)]}), idat_dir=str(idat_dir))

    print(f"  preprocessed shape: {pre_df.shape}")
    fed_df = federated_impl(pre_df)
    print(f"  federated shape: {fed_df.shape}")
    print(fed_df.head())
    node_results.append(fed_df)

# Simulate central aggregation
print("\nAggregating node results...")
combined = pd.concat(node_results, ignore_index=True)
combined["beta_weighted"] = combined["beta_mean"] * combined["n_samples"]
combined["m_weighted"] = combined["m_mean"] * combined["n_samples"]
global_summary = (
    combined.groupby("probe_id")
    .agg(
        beta_sum=("beta_weighted", "sum"),
        m_sum=("m_weighted", "sum"),
        n_samples_total=("n_samples", "sum"),
    )
    .reset_index()
)
global_summary["beta_mean_global"] = global_summary["beta_sum"] / global_summary["n_samples_total"]
global_summary["m_mean_global"] = global_summary["m_sum"] / global_summary["n_samples_total"]
global_summary = global_summary[["probe_id", "beta_mean_global", "m_mean_global", "n_samples_total"]]

print(f"\nGlobal summary shape: {global_summary.shape}")
print(global_summary.head())
