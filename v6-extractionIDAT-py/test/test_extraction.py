"""
Run this script to test your extraction function locally (without building a Docker
image) using the mock client.

Run as:

    python test_extraction.py

Make sure to do so in an environment where `vantage6-algorithm-tools` is
installed. This can be done by running:

    pip install vantage6-algorithm-tools
"""
import sys
from pathlib import Path

from vantage6.algorithm.mock.network import MockNetwork

# get path of current directory
current_path = Path(__file__).parent
# ensure the repository root is importable when running this test locally
sys.path.insert(0, str(current_path.parent))

DATABASE_LABEL = "default"

# Pass the actual IDAT folder paths as the database URI for each mock node.
# The @data_extraction decorator will pick these up as DATABASE_URI and pass
# them through to connection_details["uri"] in your extraction function.
network = MockNetwork(
    datasets=[
        {DATABASE_LABEL: {"database": str(current_path / "Hospital A"), "db_type": "folder"}},
        {DATABASE_LABEL: {"database": str(current_path / "Hospital B"), "db_type": "folder"}},
    ],
    module_name="extractionIDAT"
)

# Once the network is created, we can get the client to interact with the MockNetwork.
client = network.user_client

# List mock organizations
organizations = client.organization.list()
print(organizations)
org_ids = [organization["id"] for organization in organizations]

# Run the data extraction function using the local Hospital A folder as the target.
idat_folder_a = str(current_path / "Hospital A")
idat_folder_b = str(current_path / "Hospital B")
# Run via MockNetwork (decorated flow) — idat_dir not needed, URI comes from mock dataset
task = client.dataframe.create(
    method="data_extraction_function",
    arguments={},
    organizations=org_ids,
    label=DATABASE_LABEL,
)

# Wait for the task to complete
results = client.wait_for_results(task.get("id"))
print("decorated results:", results)

# Also call the undecorated implementation directly to see the real DataFrame for each hospital
from extractionIDAT.extract import extraction_impl
print("calling extraction_impl directly for Hospital A...")
df_a = extraction_impl({}, idat_dir=idat_folder_a)
print("Hospital A discovered samples shape:", df_a.shape)
print(df_a.head())

print("calling extraction_impl directly for Hospital B...")
df_b = extraction_impl({}, idat_dir=idat_folder_b)
print("Hospital B discovered samples shape:", df_b.shape)
print(df_b.head())

