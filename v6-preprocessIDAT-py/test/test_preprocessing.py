"""
Run this script to test you preprocessing function locally (without building a Docker
image) using the mock client.

Run as:

    python test_preprocessing.py

Make sure to do so in an environment where `vantage6-algorithm-tools` is
installed. This can be done by running:

    pip install vantage6-algorithm-tools
"""

import pandas as pd
import sys
from pathlib import Path
from vantage6.algorithm.mock.network import MockNetwork

# Add parent directory to path so we can import preprocessIDAT
sys.path.insert(0, str(Path(__file__).parent.parent))

from preprocessIDAT.preprocess import preprocessing_impl

# get path of current directory
current_path = Path(__file__).parent

# Check if hospital folders exist with IDAT files
hospitals = ["hospital_A", "hospital_B"]
for hospital in hospitals:
    hospital_path = current_path / hospital
    if not hospital_path.exists():
        print(f"⚠️  {hospital} folder not found at {hospital_path}")
        print(f"   Please add IDAT files manually to: {hospital_path}")
    else:
        idat_files = list(hospital_path.glob("*.idat"))
        if not idat_files:
            print(f"⚠️  No IDAT files found in {hospital_path}")
            print(f"   Please add .idat files to this directory")
        else:
            print(f"✓ Found {len(idat_files)} IDAT files in {hospital}")

print("\n" + "="*70)
DATABASE_LABEL = "default"

try:
    network = MockNetwork(
        datasets=[
            {DATABASE_LABEL: {"database": str(current_path / "hospital_A"), "db_type": "folder"}},
            {DATABASE_LABEL: {"database": str(current_path / "hospital_B"), "db_type": "folder"}},
            {DATABASE_LABEL: {"database": str(current_path / "hospital_A"), "db_type": "folder"}},
        ],
        module_name="preprocessIDAT"
    )

    # Once the network is created, we can get the client to interact with the MockNetwork.
    client = network.user_client

    # List mock organizations for verification
    organizations = client.organization.list()
    print("organizations:", organizations)

    # Call the preprocessing implementation directly on each hospital folder
    for folder in ["hospital_A", "hospital_B"]:
        idat_dir = str(current_path / folder)
        print(f"\nRunning preprocessing on {folder}...")
        df = pd.DataFrame({"idat_dir": [idat_dir]})
        result = preprocessing_impl(
            df,
            idat_dir=idat_dir,
            pvalue_threshold=0.05,
            min_beads=1,
        )
        print(f"preprocessed shape: {result.shape}")
        print(result.head())
except ValueError as e:
    print(f"\n❌ Error: {e}")
    print("\nNo IDAT files detected. Please add IDAT files to the test directories:")
    for hospital in hospitals:
        hospital_path = current_path / hospital
        print(f"  - {hospital_path}")