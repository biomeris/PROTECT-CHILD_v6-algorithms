"""
Central entrypoint: add up the cohort counts of every node.

Run it in the session, on the dataframe ``generate_cohort`` left there. The
subtasks it creates inherit that dataframe, so each node's ``cohort_count``
reads its own outcome row back; this function then combines the rows into
one total. It cannot generate the cohort itself: on vantage6 5 the subtasks
a central function creates are federated steps, which get no database
access.
"""

from __future__ import annotations

import time
from typing import Any, Callable, TypeVar

from vantage6.algorithm.client import AlgorithmClient
from vantage6.algorithm.tools.util import info
from vantage6.common.enum import RunStatus

from .cohort import aggregate_central_results

DecoratedFunction = TypeVar("DecoratedFunction", bound=Callable[..., Any])


try:
    from vantage6.algorithm.decorator.action import central
    from vantage6.algorithm.decorator.algorithm_client import algorithm_client
except ValueError as exc:
    if "r_home is None" not in str(exc):
        raise

    def central(func: DecoratedFunction) -> DecoratedFunction:
        return func

    def algorithm_client(func: DecoratedFunction) -> DecoratedFunction:
        return func


def _wait_for_every_run(
    client: AlgorithmClient, task_id: int, poll_interval: float
) -> None:
    # HQ marks a task finished as soon as one of its runs fails, while the
    # other nodes may still be running; their rows would be missed.
    while True:
        runs = client.run.from_task(task_id)
        if runs and all(RunStatus.has_finished(run["status"]) for run in runs):
            return
        time.sleep(poll_interval)


def _run_central_function(
    client: AlgorithmClient,
    organizations_to_include: list[int] | None = None,
    poll_interval: float = 1.0,
) -> dict[str, Any]:
    available = [organization["id"] for organization in client.organization.list()]
    if organizations_to_include is None:
        org_ids = available
    else:
        org_ids = list(organizations_to_include)
        unknown = sorted(set(org_ids) - set(available))
        if unknown:
            return {
                "status": "error",
                "message": f"Organizations not in the collaboration: {unknown}",
            }

    info(f"Collecting cohort counts from organizations {org_ids}")
    task = client.task.create(
        method="cohort_count",
        organizations=org_ids,
        name="Cohort count",
        description="Read back the cohort count recorded at each node",
    )
    if not isinstance(task, dict) or "id" not in task:
        # e.g. HQ refuses when the dataframe is missing at some organization
        # because its generate_cohort run failed.
        reason = task.get("msg", task) if isinstance(task, dict) else task
        return {
            "status": "error",
            "message": f"HQ did not create the cohort_count subtasks: {reason}",
        }
    _wait_for_every_run(client, task["id"], poll_interval)
    results = client.result.from_task(task_id=task["id"])

    return aggregate_central_results(expected_org_ids=org_ids, raw_results=results)


@central
@algorithm_client
def central_function(
    client: AlgorithmClient,
    organizations_to_include: list[int] | None = None,
) -> dict[str, Any]:
    """Add up the cohort counts generate_cohort recorded at each node.

    Returns each node's row, the errors, and the total: ``total_count`` when
    it is exact, and ``total_range`` (lowest and highest possible total) when
    some node withheld a count below its minimum.

    Parameters
    ----------
    organizations_to_include : list[int], optional
        Organizations to ask. Defaults to every organization in the
        collaboration.
    """
    return _run_central_function(client, organizations_to_include)
