"""
Run the federated EFA on the vantage6 MockNetwork and print the results. Each
CSV becomes one mock hospital.

Run as:

    uv run python test/run_example.py
    uv run python test/run_example.py --columns a b c d --n-factors 2 --rotation varimax x.csv y.csv
"""

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from vantage6.algorithm.mock.network import MockNetwork

current_path = Path(__file__).parent
DEFAULT_FILES = [current_path / f"test_data_{i}.csv" for i in (1, 2, 3)]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[1])
    parser.add_argument("csv", nargs="*", type=Path, default=DEFAULT_FILES)
    parser.add_argument("--columns", nargs="+", default=[f"x{i}" for i in range(1, 9)])
    parser.add_argument("--n-factors", type=int, default=None)
    parser.add_argument("--method", default="minres")
    parser.add_argument("--rotation", default="promax")
    args = parser.parse_args()

    datasets = [pd.read_csv(f) for f in args.csv]
    for f, d in zip(args.csv, datasets):
        print(f"{f.name}: {len(d)} rows")

    logging.disable(logging.INFO)
    network = MockNetwork(
        datasets=[{"default": {"database": d}} for d in datasets],
        module_name="v6-efa-py",
    )
    client = network.user_client
    task = client.task.create(
        method="efa",
        arguments={
            "columns": args.columns,
            "n_factors": args.n_factors,
            "method": args.method,
            "rotation": args.rotation,
        },
        organizations=[network.organization_ids[0]],
        databases=[
            {"type": "dataframe", "dataframe_id": network.hq.dataframes[0]["id"]}
        ],
    )
    r = client.wait_for_results(task.get("id"))[0]

    print("\nIncluded organizations:", r["included_organizations"])
    print("Excluded organizations:", r["excluded_organizations"])
    for w in r["warnings"]:
        print("Warning:", w)
    if r["loadings"] is None:
        return

    print(
        f"\nN = {r['n_total']}, {r['n_factors']} factor(s) ({r['n_factors_source']}), "
        f"method = {r['method']}, rotation = {r['rotation']}"
    )
    print("Eigenvalues:", np.round(r["eigenvalues"], 3).tolist())
    b = r["bartlett"]
    print(f"KMO = {r['kmo']['overall']:.3f}; Bartlett chi2 = {b['statistic']:.2f}, "
          f"df = {b['df']:.0f}, p = {b['p_value']:.3g}")

    table = pd.DataFrame(r["loadings"], index=r["factor_names"]).T
    table["communality"] = pd.Series(r["communalities"])
    print("\nLoadings")
    print(table.round(3).to_string())

    v = r["variance"]
    print("\nVariance explained")
    print(pd.DataFrame(v, index=r["factor_names"]).T.round(3).to_string())

    if r["factor_correlations"] is not None:
        print("\nFactor correlations")
        print(pd.DataFrame(r["factor_correlations"], index=r["factor_names"],
                           columns=r["factor_names"]).round(3).to_string())


if __name__ == "__main__":
    main()
