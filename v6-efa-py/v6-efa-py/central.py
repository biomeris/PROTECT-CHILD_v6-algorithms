"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""

import numbers
from typing import Any

import numpy as np

from vantage6.algorithm.tools.util import info, warn
from vantage6.algorithm.tools.exceptions import InputError
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.client import AlgorithmClient

from .efa_math import (
    METHODS,
    N_THRESHOLD_FLAG,
    ROTATIONS,
    UNIQUENESS_BOUNDS,
    bartlett,
    combine,
    correlation_matrix,
    fit_efa,
    kaiser_n_factors,
    kmo,
)


def _validate_arguments(
    columns: Any, n_factors: Any, method: Any, rotation: Any
) -> tuple[list[str], str | None]:
    if not isinstance(columns, (list, tuple)) or not all(
        isinstance(c, str) for c in columns
    ):
        raise InputError("'columns' must be a list of column names.")
    columns = list(columns)
    if len(columns) < 3:
        raise InputError("Factor analysis needs at least 3 columns.")
    if len(set(columns)) != len(columns):
        raise InputError(f"'columns' contains duplicates: {columns}")
    if n_factors is not None:
        if (
            not isinstance(n_factors, numbers.Integral)
            or isinstance(n_factors, bool)
            or not 1 <= n_factors < len(columns)
        ):
            raise InputError(
                f"'n_factors' must be an integer from 1 to {len(columns) - 1} "
                f"(fewer than the number of columns), got {n_factors!r}."
            )
    if method not in METHODS:
        raise InputError(f"'method' must be one of {list(METHODS)}, got {method!r}.")
    if isinstance(rotation, str) and rotation.lower() == "none":
        rotation = None
    if rotation not in ROTATIONS:
        raise InputError(
            f"'rotation' must be one of {[r for r in ROTATIONS if r]} or None, "
            f"got {rotation!r}."
        )
    return columns, rotation


def _by_column(columns: list[str], values) -> dict:
    return {c: np.asarray(v).tolist() for c, v in zip(columns, values)}


@central
@algorithm_client
def efa(
    client: AlgorithmClient,
    columns: list[str],
    n_factors: int | None = None,
    method: str = "minres",
    rotation: str | None = "promax",
    organizations_to_include: list[int] | None = None,
) -> Any:
    """
    Federated exploratory factor analysis.

    Each node shares only its number of complete cases, column sums and
    cross-product matrix. The central function builds the pooled correlation
    matrix from these, checks sampling adequacy (KMO) and sphericity
    (Bartlett), extracts the factors and rotates them. The result is identical
    to an EFA on the pooled data.

    Parameters
    ----------
    client : AlgorithmClient
        The client instance used to interact with the vantage6 server.
    columns : list[str]
        The numeric variables to factor-analyse (at least 3).
    n_factors : int, optional
        The number of factors to extract. Defaults to the Kaiser criterion (the
        number of eigenvalues of the correlation matrix greater than 1).
    method : str
        Extraction method: ``"minres"`` (default), ``"ml"`` or ``"principal"``.
    rotation : str or None
        ``"promax"`` (default), ``"oblimin"``, ``"varimax"`` or ``None``.
    organizations_to_include : list[int], optional
        The organizations to include in the analysis. Defaults to every
        organization in the collaboration.

    Returns
    -------
    dict
        Loadings, communalities, explained variance, factor correlations (for
        oblique rotations), eigenvalues, KMO, Bartlett's test, and which
        organizations were included or excluded.
    """
    columns, rotation = _validate_arguments(columns, n_factors, method, rotation)

    if organizations_to_include is None:
        organizations = client.organization.list()
        organizations_to_include = [o.get("id") for o in organizations]
    ids = list(organizations_to_include)
    if not ids:
        raise InputError("'organizations_to_include' must not be empty.")

    # One subtask per organization so every result can be attributed to the
    # organization that produced it (the order of results across a multi-
    # organization task is not guaranteed).
    info(f"Creating subtasks for organizations {ids}")
    tasks = {
        org_id: client.task.create(
            method="compute_local_moments",
            arguments={"columns": columns},
            organizations=[org_id],
            name="EFA local moments",
            description="Number of records, column sums and cross-product matrix",
        )
        for org_id in ids
    }

    info("Waiting for results")
    moments, included, excluded = [], [], []
    for org_id, task in tasks.items():
        results = client.wait_for_results(task_id=task.get("id"))
        if len(results) != 1 or results[0] is None:
            raise InputError(
                f"Organization {org_id} returned no result; its task run may have "
                "failed. Aborting."
            )
        if N_THRESHOLD_FLAG in results[0]:
            warn(
                f"Insufficient records for organization {org_id}. Excluding "
                "organization from analysis."
            )
            excluded.append(org_id)
            continue
        included.append(org_id)
        moments.append(results[0])
    info("Results obtained!")

    output = {
        "included_organizations": included,
        "excluded_organizations": excluded,
        "columns": columns,
        "method": method,
        "rotation": rotation,
        "warnings": [],
    }

    if not moments:
        warn("No organization meets the minimum number of records.")
        output["warnings"].append(
            "No organization meets the minimum number of records; nothing computed."
        )
        output["loadings"] = None
        return output

    if len(included) == 1 and len(ids) > 1:
        output["warnings"].append(
            "Only one organization contributed; the result describes that "
            "organization's data alone."
        )

    info("Building the pooled correlation matrix")
    pooled = combine(moments)
    n, p = pooled["n"], len(columns)
    if n <= p:
        raise InputError(
            f"The pooled number of complete records ({n}) must exceed the number "
            f"of columns ({p})."
        )
    R = correlation_matrix(pooled)

    eigenvalues = np.linalg.eigvalsh(R)[::-1]
    kmo_per_item, kmo_overall = kmo(R)
    chi2, chi2_df, chi2_p = bartlett(R, n)

    if n_factors is None:
        n_factors = kaiser_n_factors(R)
        if n_factors >= p:
            n_factors = p - 1
        info(f"Kaiser criterion: {n_factors} factor(s)")
        output["n_factors_source"] = "kaiser"
    else:
        output["n_factors_source"] = "user"

    if method != "principal" and ((p - n_factors) ** 2 - (p + n_factors)) < 0:
        output["warnings"].append(
            f"{n_factors} factors is too many for {p} variables: the model has "
            "negative degrees of freedom and is not identified."
        )
    if kmo_overall < 0.6:
        output["warnings"].append(
            f"Overall KMO is {kmo_overall:.2f} (< 0.6): the data may be unsuitable "
            "for factor analysis."
        )
    if rotation is not None and n_factors == 1:
        output["warnings"].append("No rotation is performed with a single factor.")

    info(f"Extracting {n_factors} factor(s) with {method}, rotation {rotation}")
    fit = fit_efa(R, n_factors, method, rotation)

    if not fit["converged"]:
        output["warnings"].append("The extraction optimizer did not converge.")
    heywood = [
        c
        for c, u in zip(columns, fit["uniquenesses"])
        if method != "principal" and u <= UNIQUENESS_BOUNDS[0] + 1e-6
    ]
    if heywood:
        output["warnings"].append(
            f"Heywood case: uniqueness of {heywood} at the lower bound "
            f"({UNIQUENESS_BOUNDS[0]}); interpret those loadings with care."
        )

    factor_names = [f"F{i + 1}" for i in range(n_factors)]
    output.update(
        {
            "n_total": int(n),
            "n_factors": int(n_factors),
            "factor_names": factor_names,
            "loadings": _by_column(columns, fit["loadings"]),
            "communalities": dict(zip(columns, fit["communalities"].tolist())),
            "uniquenesses": dict(zip(columns, fit["uniquenesses"].tolist())),
            "variance": {
                "ss_loadings": fit["ss_loadings"].tolist(),
                "proportion": fit["proportion_variance"].tolist(),
                "cumulative": fit["cumulative_variance"].tolist(),
            },
            "factor_correlations": (
                fit["phi"].tolist() if fit["phi"] is not None else None
            ),
            "structure": (
                _by_column(columns, fit["structure"])
                if fit["structure"] is not None
                else None
            ),
            "eigenvalues": eigenvalues.tolist(),
            "kmo": {
                "overall": kmo_overall,
                "per_item": dict(zip(columns, kmo_per_item.tolist())),
            },
            "bartlett": {"statistic": chi2, "df": chi2_df, "p_value": chi2_p},
            "correlation_matrix": R.tolist(),
            "converged": fit["converged"],
        }
    )

    info("Returning results!")
    return output
