"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""

import numbers
from typing import Any

from vantage6.algorithm.tools.util import info, warn, get_env_var
from vantage6.algorithm.tools.exceptions import InputError
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.client import AlgorithmClient

from .globals import (
    DUNN_FEW_RANK_BINS_WARNING,
    DUNN_HISTOGRAM_BINS,
    DUNN_MAXIMUM_RANK_BINS,
    DUNN_MINIMUM_CELL_COUNT,
    DUNN_RANGE_SD,
)
from .dunn_math import (
    N_THRESHOLD_FLAG,
    P_ADJUST_METHODS,
    combine_scores,
    dunn as dunn_test,
    kruskal_wallis,
    rank_bins,
    reporting_grid,
)


def _env_float(var_name: str, default: float) -> float:
    """``get_env_var`` only converts to str/bool/int, so floats are parsed here."""
    value = get_env_var(var_name, None)
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        raise InputError(
            f"Environment variable '{var_name}' has value '{value}', which cannot "
            "be converted to a float."
        ) from None


def _validate_arguments(group_column, column, p_adjust, alpha) -> None:
    for name, value in (("group_column", group_column), ("column", column)):
        if not isinstance(value, str) or not value:
            raise InputError(f"'{name}' must be a non-empty string.")
    if group_column == column:
        raise InputError("'group_column' and 'column' must be different.")
    if p_adjust not in P_ADJUST_METHODS:
        raise InputError(
            f"'p_adjust' must be one of {list(P_ADJUST_METHODS)}, got {p_adjust!r}."
        )
    if (
        not isinstance(alpha, numbers.Real)
        or isinstance(alpha, bool)
        or not 0 < alpha < 1
    ):
        raise InputError(f"'alpha' must be a number between 0 and 1, got {alpha!r}.")


def _run_round(
    client: AlgorithmClient, method: str, arguments: dict, ids: list[int], name: str
) -> dict[int, dict]:
    """Run ``method`` as one subtask per organization and return results by id.

    One subtask per organization lets every result be attributed to the
    organization that produced it (the order of results across a multi-
    organization task is not guaranteed).
    """
    tasks = {
        org_id: client.task.create(
            method=method,
            arguments=arguments,
            organizations=[org_id],
            name=name,
            description=name,
        )
        for org_id in ids
    }
    out = {}
    for org_id, task in tasks.items():
        results = client.wait_for_results(task_id=task.get("id"))
        if len(results) != 1 or results[0] is None:
            raise InputError(
                f"Organization {org_id} returned no result for '{method}'; its "
                "task run may have failed. Aborting."
            )
        out[org_id] = results[0]
    return out


@central
@algorithm_client
def dunn(
    client: AlgorithmClient,
    group_column: str,
    column: str,
    p_adjust: str = "bonferroni",
    alpha: float = 0.05,
    organizations_to_include: list[int] | None = None,
) -> Any:
    """
    Federated Dunn's post-hoc test.

    Compares the mean ranks of ``column`` between every pair of groups in
    ``group_column``, after a Kruskal-Wallis test (also reported). Global ranks
    are approximated in three rounds without any node sharing an individual
    value: a pooled grid from moments, a group-blind histogram that the central
    function turns into roughly equal-count rank bins, and per-group sums of
    the records' rank-bin mid-ranks.

    Parameters
    ----------
    client : AlgorithmClient
        The client instance used to interact with the vantage6 server.
    group_column : str
        The column defining the groups.
    column : str
        The numeric column whose ranks are compared.
    p_adjust : str
        Multiple-comparison adjustment: ``"bonferroni"`` (default), ``"holm"``,
        ``"fdr_bh"`` (Benjamini-Hochberg) or ``"none"``.
    alpha : float
        Significance level for the ``reject`` flags. Defaults to 0.05.
    organizations_to_include : list[int], optional
        The organizations to include in the analysis. Defaults to every
        organization in the collaboration.

    Returns
    -------
    dict
        The Kruskal-Wallis test, all pairwise comparisons, the group sizes and
        mean ranks, the number of rank bins, and which organizations were
        included or excluded.
    """
    _validate_arguments(group_column, column, p_adjust, alpha)

    if organizations_to_include is None:
        organizations = client.organization.list()
        organizations_to_include = [o.get("id") for o in organizations]
    ids = list(organizations_to_include)
    if not ids:
        raise InputError("'organizations_to_include' must not be empty.")

    histogram_bins = get_env_var(
        "DUNN_HISTOGRAM_BINS", DUNN_HISTOGRAM_BINS, as_type="int"
    )
    maximum_rank_bins = get_env_var(
        "DUNN_MAXIMUM_RANK_BINS", DUNN_MAXIMUM_RANK_BINS, as_type="int"
    )
    minimum_cell_count = get_env_var(
        "DUNN_MINIMUM_CELL_COUNT", DUNN_MINIMUM_CELL_COUNT, as_type="int"
    )
    range_sd = _env_float("DUNN_RANGE_SD", DUNN_RANGE_SD)
    base_arguments = {"group_column": group_column, "column": column}

    output = {
        "group_column": group_column,
        "column": column,
        "p_adjust": p_adjust,
        "alpha": alpha,
        "warnings": [],
    }

    # Round 1: moments, which also decide which organizations take part
    info(f"Round 1: moments from organizations {ids}")
    moments = _run_round(
        client, "compute_local_moments", base_arguments, ids, "Dunn round 1: moments"
    )
    included, excluded, suppressed = [], [], {}
    for org_id, result in moments.items():
        if N_THRESHOLD_FLAG in result:
            warn(f"Insufficient records for organization {org_id}; excluding it.")
            excluded.append(org_id)
            continue
        included.append(org_id)
        if result["suppressed_groups"]:
            suppressed[str(org_id)] = result["suppressed_groups"]
    output.update(
        {
            "included_organizations": included,
            "excluded_organizations": excluded,
            "suppressed_groups": suppressed,
        }
    )

    if not included:
        warn("No organization meets the minimum number of records.")
        output["warnings"].append(
            "No organization meets the minimum number of records; nothing computed."
        )
        output["comparisons"] = None
        return output
    if len(included) == 1 and len(ids) > 1:
        output["warnings"].append(
            "Only one organization contributed; the result describes that "
            "organization's data alone."
        )

    edges = reporting_grid(
        [moments[i] for i in included], histogram_bins, range_sd
    )

    # Round 2: group-blind histogram -> rank bins
    info("Round 2: group-blind histograms")
    histograms = _run_round(
        client,
        "compute_local_histogram",
        {**base_arguments, "edges": edges},
        included,
        "Dunn round 2: histogram",
    )
    rank_edges, mid_ranks, n_rank_bins = rank_bins(
        edges,
        [histograms[i]["counts"] for i in included],
        minimum_cell_count,
        maximum_rank_bins,
    )
    if n_rank_bins < 2:
        raise InputError(
            "Too few records to form at least 2 rank bins under the minimum cell "
            "count; the test cannot be computed."
        )
    if n_rank_bins < DUNN_FEW_RANK_BINS_WARNING:
        output["warnings"].append(
            f"Only {n_rank_bins} rank bins could be formed; the ranks are coarse and "
            "the statistics may deviate substantially from an exact (pooled) test. "
            "Treat the result as indicative."
        )

    # Round 3: rank-score sums per group
    info(f"Round 3: rank scores on {n_rank_bins} rank bins")
    scores = _run_round(
        client,
        "compute_local_rank_scores",
        {**base_arguments, "edges": rank_edges, "mid_ranks": mid_ranks},
        included,
        "Dunn round 3: rank scores",
    )
    groups = combine_scores([scores[i]["groups"] for i in included])
    if len(groups) < 2:
        raise InputError(
            f"At least 2 groups are needed after suppression, found {list(groups)}."
        )

    info("Computing Kruskal-Wallis and Dunn's test")
    output.update(
        {
            "n_total": int(sum(g["n"] for g in groups.values())),
            "n_groups": len(groups),
            "group_sizes": {lbl: int(g["n"]) for lbl, g in groups.items()},
            "mean_ranks": {
                lbl: g["score_sum"] / g["n"] for lbl, g in groups.items()
            },
            "n_rank_bins": n_rank_bins,
            "kruskal_wallis": kruskal_wallis(groups),
            "comparisons": dunn_test(groups, p_adjust, alpha),
        }
    )

    info("Returning results!")
    return output
