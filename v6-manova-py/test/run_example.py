"""
Run the federated MANOVA on the vantage6 MockNetwork and print the results next
to statsmodels' MANOVA on the pooled data. Each CSV becomes one mock hospital.

Run as:

    uv run python test/run_example.py
    uv run python test/run_example.py --group group --outcomes y1 y2 y3 a.csv b.csv
"""

import argparse
import logging
from pathlib import Path

import pandas as pd
from statsmodels.multivariate.manova import MANOVA

from vantage6.algorithm.mock.network import MockNetwork

current_path = Path(__file__).parent
DEFAULT_FILES = [current_path / f"test_data_{i}.csv" for i in (1, 2, 3)]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[1])
    parser.add_argument("csv", nargs="*", type=Path, default=DEFAULT_FILES)
    parser.add_argument("--group", default="group")
    parser.add_argument("--outcomes", nargs="+", default=["y1", "y2", "y3"])
    args = parser.parse_args()

    datasets = [pd.read_csv(f) for f in args.csv]
    for f, d in zip(args.csv, datasets):
        print(f"{f.name}: {len(d)} rows")

    logging.disable(logging.INFO)
    network = MockNetwork(
        datasets=[{"default": {"database": d}} for d in datasets],
        module_name="v6-manova-py",
    )
    client = network.user_client
    task = client.task.create(
        method="manova",
        arguments={"group_column": args.group, "outcome_columns": args.outcomes},
        organizations=[network.organization_ids[0]],
        databases=[
            {"type": "dataframe", "dataframe_id": network.hq.dataframes[0]["id"]}
        ],
    )
    result = client.wait_for_results(task.get("id"))[0]

    print("\nIncluded organizations:", result["included_organizations"])
    print("Excluded organizations:", result["excluded_organizations"])
    print("Suppressed groups:     ", result["suppressed_groups"])
    for w in result["warnings"]:
        print("Warning:", w)
    if result["tests"] is None:
        return

    print(f"\nN = {result['n_total']}, group sizes = {result['group_sizes']}")
    table = pd.DataFrame(result["tests"]).T
    print("\nFederated MANOVA")
    print(table.to_string(float_format="%.6g"))

    if not result["excluded_organizations"] and not result["suppressed_groups"]:
        pooled = pd.concat(datasets)[[args.group, *args.outcomes]].dropna()
        formula = " + ".join(args.outcomes) + f" ~ {args.group}"
        ref = MANOVA.from_formula(formula, data=pooled).mv_test()
        print("\nPooled statsmodels MANOVA (reference)")
        print(ref.results[args.group]["stat"].astype(float).to_string(float_format="%.6g"))


if __name__ == "__main__":
    main()
