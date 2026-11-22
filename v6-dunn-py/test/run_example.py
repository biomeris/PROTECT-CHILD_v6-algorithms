"""
Run the federated Dunn's test on the vantage6 MockNetwork and print the results
next to the exact test (scikit-posthocs) on the pooled data. Each CSV becomes
one mock hospital.

Run as:

    uv run python test/run_example.py
    uv run python test/run_example.py --group group --column score --p-adjust holm a.csv b.csv
"""

import argparse
import logging
from pathlib import Path

import pandas as pd
import scikit_posthocs as skp
from scipy.stats import kruskal

from vantage6.algorithm.mock.network import MockNetwork

current_path = Path(__file__).parent
DEFAULT_FILES = [current_path / f"test_data_{i}.csv" for i in (1, 2, 3)]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[1])
    parser.add_argument("csv", nargs="*", type=Path, default=DEFAULT_FILES)
    parser.add_argument("--group", default="group")
    parser.add_argument("--column", default="score")
    parser.add_argument("--p-adjust", default="bonferroni")
    parser.add_argument("--alpha", type=float, default=0.05)
    args = parser.parse_args()

    datasets = [pd.read_csv(f) for f in args.csv]
    for f, d in zip(args.csv, datasets):
        print(f"{f.name}: {len(d)} rows")

    logging.disable(logging.INFO)
    network = MockNetwork(
        datasets=[{"default": {"database": d}} for d in datasets],
        module_name="v6-dunn-py",
    )
    client = network.user_client
    task = client.task.create(
        method="dunn",
        arguments={
            "group_column": args.group,
            "column": args.column,
            "p_adjust": args.p_adjust,
            "alpha": args.alpha,
        },
        organizations=[network.organization_ids[0]],
        databases=[
            {"type": "dataframe", "dataframe_id": network.hq.dataframes[0]["id"]}
        ],
    )
    r = client.wait_for_results(task.get("id"))[0]

    print("\nIncluded organizations:", r["included_organizations"])
    print("Excluded organizations:", r["excluded_organizations"])
    print("Suppressed groups:     ", r["suppressed_groups"])
    for w in r["warnings"]:
        print("Warning:", w)
    if r["comparisons"] is None:
        return

    print(f"\nN = {r['n_total']}, group sizes = {r['group_sizes']}, "
          f"rank bins = {r['n_rank_bins']}")
    kw = r["kruskal_wallis"]
    print(f"Kruskal-Wallis: H = {kw['h_statistic']:.3f}, df = {kw['df']}, "
          f"p = {kw['p_value']:.3g}")

    table = pd.DataFrame(r["comparisons"]).set_index(["group_1", "group_2"])
    print(f"\nFederated Dunn's test (p_adjust = {r['p_adjust']})")
    print(table.to_string(float_format="%.4g"))

    if not r["excluded_organizations"] and not r["suppressed_groups"]:
        pooled = pd.concat(datasets)[[args.group, args.column]].dropna()
        pooled[args.group] = pooled[args.group].astype(str)
        groups = sorted(pooled[args.group].unique())
        h = kruskal(*[pooled[args.column][pooled[args.group] == g] for g in groups])
        print(f"\nExact (pooled) Kruskal-Wallis: H = {h.statistic:.3f}, p = {h.pvalue:.3g}")
        ref = skp.posthoc_dunn(
            pooled, val_col=args.column, group_col=args.group,
            p_adjust=None if args.p_adjust == "none" else args.p_adjust,
        )
        table["exact_p_adjusted"] = [ref.loc[a, b] for a, b in table.index]
        print("Exact (pooled) adjusted p-values for comparison")
        print(table[["p_adjusted", "exact_p_adjusted"]].to_string(float_format="%.4g"))


if __name__ == "__main__":
    main()
