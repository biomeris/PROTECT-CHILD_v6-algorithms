"""
Sample QC after the extraction: a summary per hospital and cohort, and an
optional preprocessing step that removes the flagged samples.

The extraction marks every sample with `qc_failed_fraction` (share of its
probes failing detection) and `qc_flagged` (above the flag threshold, 10% by
default, ChAMP's sample cut-off). Flagged samples are kept: following the
"flag, don't drop" principle, removing them is an explicit choice.

- qc_summary (central) / partial_qc_summary (federated): per hospital and
  cohort, the number of samples, flagged samples and samples left after
  excluding the flagged ones, and whether that falls below a minimum (the later
  analyses need 2-5 samples per cohort and hospital). Only counts leave the
  hospitals, never sample IDs.
- exclude_flagged_samples (preprocessing): removes the flagged samples from the
  session's dataframe at every hospital, in place, without extracting again.
"""

import pandas as pd

from vantage6.algorithm.client import AlgorithmClient
from vantage6.algorithm.decorator.action import central, federated, preprocessing
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.decorator.data import dataframe
from vantage6.algorithm.tools.util import error, info, warn

QC_COLUMNS = ("qc_flagged", "qc_failed_fraction")
NEEDS_NEW_EXTRACTION = (
    "This dataframe has no per-sample QC columns (qc_flagged): it was extracted "
    "with an extraction version before 2.3.0. Extract it again to use the sample QC."
)


def _samples(df: pd.DataFrame, cohort_column: str) -> pd.DataFrame:
    """One row per sample: cohort, flagged."""
    missing = [c for c in ("sample_label", cohort_column, "qc_flagged") if c not in df.columns]
    if missing:
        if "qc_flagged" in missing:
            raise ValueError(NEEDS_NEW_EXTRACTION)
        raise ValueError(f"missing columns: {missing}")
    samples = df[["sample_label", cohort_column, "qc_flagged"]].drop_duplicates("sample_label")
    return samples.rename(columns={cohort_column: "cohort"})


@federated
@dataframe(1)
def partial_qc_summary(df1: pd.DataFrame, cohort_column: str = "cohort") -> dict:
    """Per cohort at this hospital: samples and flagged samples (counts only)."""
    samples = _samples(df1, cohort_column)
    rows = []
    for cohort, group in samples.groupby(samples["cohort"].astype(str)):
        rows.append({
            "cohort": cohort,
            "n_samples": int(len(group)),
            "n_flagged": int(group["qc_flagged"].astype(bool).sum()),
        })
    info(f"Sample QC: {int(samples['qc_flagged'].astype(bool).sum())} of {len(samples)} samples flagged")
    return {"cohorts": rows}


@central
@algorithm_client
def qc_summary(
    client: AlgorithmClient,
    cohort_column: str = "cohort",
    min_samples: int = 5,
    organizations_to_include: list | None = None,
) -> dict:
    """
    Sample QC per hospital and cohort: samples, flagged samples, samples left
    if the flagged ones are excluded, and whether that falls below
    ``min_samples`` (the minimum the later analyses need per cohort and
    hospital). Counts only; sample IDs never leave the hospitals.
    """
    org_ids = [o["id"] for o in client.organization.list()]
    if organizations_to_include:
        wanted = [int(o) for o in organizations_to_include]
        unknown = sorted(set(wanted) - set(org_ids))
        if unknown:
            raise ValueError(f"organizations_to_include: {unknown} are not in this collaboration")
        org_ids = [o for o in org_ids if o in wanted]

    # One subtask per hospital, so each result is known to belong to its hospital
    tasks = {}
    for org_id in org_ids:
        task = client.task.create(
            method="partial_qc_summary",
            arguments={"cohort_column": cohort_column},
            organizations=[org_id],
            name=f"Sample QC (organization {org_id})",
        )
        if not isinstance(task, dict) or task.get("id") is None:
            message = task.get("msg") if isinstance(task, dict) else task
            raise RuntimeError(f"HQ did not create the subtask for organization {org_id}: {message}")
        tasks[org_id] = task["id"]

    rows, problems = [], []
    for org_id, task_id in tasks.items():
        replies = client.wait_for_results(task_id=task_id)
        reply = replies[0] if isinstance(replies, list) and replies else replies
        if not isinstance(reply, dict) or "cohorts" not in reply:
            reason = reply.get("error") if isinstance(reply, dict) else "no result"
            problems.append(f"organization {org_id}: {reason}")
            continue
        for row in reply["cohorts"]:
            left = row["n_samples"] - row["n_flagged"]
            rows.append({
                "organization_id": org_id,
                "cohort": row["cohort"],
                "n_samples": row["n_samples"],
                "n_flagged": row["n_flagged"],
                "n_after_exclusion": left,
                "below_minimum_after_exclusion": bool(left < min_samples <= row["n_samples"]),
            })

    if problems and not rows:
        error("; ".join(problems))
        raise ValueError("No hospital returned its sample QC: " + "; ".join(problems))

    n_samples = sum(r["n_samples"] for r in rows)
    n_flagged = sum(r["n_flagged"] for r in rows)
    lost = [r for r in rows if r["below_minimum_after_exclusion"]]
    summary = (
        f"{n_flagged} of {n_samples} samples are flagged (more failed probes than the "
        "extraction's threshold). They are kept unless the 'Exclude QC-flagged samples' "
        f"step is run, which would leave {n_samples - n_flagged}."
    )
    if lost:
        summary += (
            f" Excluding them would bring {len(lost)} cohort(s) below {min_samples} samples at "
            "their hospital: " + ", ".join(f"{r['cohort']} at organization {r['organization_id']}"
                                           for r in lost) + "."
        )
    if problems:
        summary += " Not reported: " + "; ".join(problems) + "."
        warn("; ".join(problems))
    info(summary)
    return {
        "status": [{"summary": summary}],
        "summary": summary,
        "qc_summary": rows,
        "n_samples": n_samples,
        "n_flagged": n_flagged,
        "min_samples": int(min_samples),
    }


@preprocessing
def exclude_flagged_samples(df1: pd.DataFrame) -> pd.DataFrame:
    """
    Remove the QC-flagged samples (qc_flagged) from this hospital's dataframe.
    The session's dataframe is replaced; the extraction is not repeated.
    """
    return exclude_flagged(df1)


def exclude_flagged(df1: pd.DataFrame) -> pd.DataFrame:
    """Undecorated implementation of exclude_flagged_samples (for local tests)."""
    if "qc_flagged" not in df1.columns:
        error(NEEDS_NEW_EXTRACTION)
        raise ValueError(NEEDS_NEW_EXTRACTION)
    flagged = df1["qc_flagged"].astype(bool)
    n_before = df1.loc[:, "sample_label"].nunique()
    kept = df1[~flagged]
    n_after = kept["sample_label"].nunique()
    info(f"Excluded {n_before - n_after} QC-flagged sample(s); {n_after} of {n_before} samples kept")
    if n_after == 0:
        warn("Every sample at this hospital was flagged: its dataframe is now empty")
    return kept.reset_index(drop=True)
