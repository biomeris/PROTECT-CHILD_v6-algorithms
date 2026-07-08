"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""

from typing import Any, Literal

import pandas as pd
import numpy as np
from scipy.stats import fisher_exact

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.client import AlgorithmClient


@central
@algorithm_client
def central_function(
    client: AlgorithmClient,
    user_set_path: list[str],
    user_universe_path: str,
    region_db_path: str,
    min_overlap: int = 1,
    redefine_user_set: bool = False,
    direction: Literal["enrichment", "depletion"] = "enrichment",
    organizations_to_include: list[int] | None = None,
) -> Any:
    """Central part of the algorithm"""

    # If organization_to_include is None, get all organizations
    if organizations_to_include is None:
        # get all organizations (ids) within the collaboration so you can send a
        # task to them.
        organizations = client.organization.list()
        organizations_to_include = [
            organization.get("id") for organization in organizations
        ]

    # create a subtask for all organizations in the collaboration.
    info("Creating subtask for all organizations in the collaboration")
    task = client.task.create(
        method="federated_function",
        arguments={
            "user_set_path": user_set_path,
            "user_universe_path": user_universe_path,
            "region_db_path": region_db_path,
            "min_overlap": min_overlap,
            "redefine_user_set": redefine_user_set,
        },
        organizations=organizations_to_include,
        name="Overlaps",
        description="Compute overlaps between user-defined genomic region sets and a"
        " reference region database (LOLA)",
    )

    # wait for node to return results of the subtask.
    info("Waiting for results")
    results = client.wait_for_results(task_id=task.get("id"))
    info("Results obtained!")

    info("Aggregating overlap counts across organizations...")
    score_tables = [pd.DataFrame(r["scoreTable"]) for r in results]
    score_tables = pd.concat(score_tables, ignore_index=True)

    # Group score tables
    score_table_grouped = score_tables.groupby(["userSet", "dbSet"], as_index=False)[
        ["support", "b", "c", "d"]
    ].sum()

    if direction == "depletion":
        alternative = "less"
    else:
        alternative = "greater"

    info("Calculating Fisher scores...")
    score_table_grouped[["oddsRatio", "pValue"]] = score_table_grouped.apply(
        _compute_fisher, axis=1, args=(alternative,)
    )

    info("Computing ranking statistics...")
    score_table_grouped["pValueLog"] = -np.log10(score_table_grouped["pValue"] + 1e-322)

    # rnkSup
    score_table_grouped["rnkSup"] = score_table_grouped.groupby("userSet")[
        "support"
    ].rank(ascending=False, method="min")

    # pValueLog
    score_table_grouped["rnkPV"] = score_table_grouped.groupby("userSet")[
        "pValueLog"
    ].rank(ascending=False, method="min")

    # oddsRatio
    score_table_grouped["rnkOR"] = score_table_grouped.groupby("userSet")[
        "oddsRatio"
    ].rank(ascending=False, method="min")

    # max rank
    score_table_grouped["maxRnk"] = score_table_grouped[
        ["rnkSup", "rnkPV", "rnkOR"]
    ].max(axis=1)

    # mean rank
    score_table_grouped["meanRnk"] = (
        score_table_grouped[["rnkSup", "rnkPV", "rnkOR"]].mean(axis=1).round(3)
    )

    annotation_dt = pd.DataFrame(results[0]["annotationDT"]).reset_index(drop=True)
    annotation_dt["dbSet"] = annotation_dt.index + 1

    info("Merging overlap statistics with LOLA annotations...")
    score_table_merge = score_table_grouped.merge(annotation_dt, on="dbSet", how="left")
    warn(f"Missing annotations: {score_table_merge['description'].isna().sum()}")

    # truncate description
    score_table_merge["description"] = score_table_merge["description"].str.slice(0, 80)

    info("Formatting final output columns...")
    # column order
    ordered_cols = [
        "userSet",
        "dbSet",
        "collection",
        "pValueLog",
        "oddsRatio",
        "support",
        "rnkPV",
        "rnkOR",
        "rnkSup",
        "maxRnk",
        "meanRnk",
        "b",
        "c",
        "d",
        "description",
        "cellType",
        "tissue",
        "antibody",
        "treatment",
        "dataSource",
        "filename",
    ]

    unordered_cols = [c for c in score_table_merge.columns if c not in ordered_cols]

    score_table_merge = score_table_merge[ordered_cols + unordered_cols]

    score_table_merge = score_table_merge.sort_values(
        by=["pValueLog", "meanRnk"], ascending=[False, False]
    )

    # return the final results of the algorithm
    info("Returning results!")
    return score_table_merge.to_dict(orient="records")


def _compute_fisher(row, alternative):
    odds, p = fisher_exact(
        [[row.support, row.b], [row.c, row.d]],
        alternative=alternative,
    )
    return pd.Series([odds, p])
