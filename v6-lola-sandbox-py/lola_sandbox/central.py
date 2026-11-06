"""
LOLA enrichment of DMRs, computed once.

Sandbox version of v6-lola-py. Its three inputs are not patient-level data:

- user set: the DMRs of a v6-dmp-dmr-py result (``global_dmrs``: chromosome,
  start, end), an aggregate result
- universe: the probes that DMP/DMR tested (its ``universe`` field), i.e. the
  CpGs that could have been in a DMR
- reference: LOLACore hg38, built into the Docker image (nodes have no internet)

So nothing is asked from the hospitals: the central function runs on one node,
counts the overlaps once and computes one Fisher test per reference region set.
(v6-lola-py sends the counting to every node and sums the tables; with the same
inputs at every node that multiplies all counts by the number of nodes.)

The DMRs are redefined as the universe CpGs they cover, so that user set and
universe count the same units (tested CpGs); see lola_core.run_lola.
"""
from __future__ import annotations

import base64
import io
import os
from typing import Any, Literal

import numpy as np
import pandas as pd
from scipy.stats import false_discovery_control, fisher_exact

from vantage6.algorithm.client import AlgorithmClient
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.tools.util import info

from .lola_core import run_lola

# LOLACore hg38 collections with cached region sets in the image
COLLECTIONS = (
    "ucsc_features", "encode_tfbs", "sheffield_dnase", "codex",
    "cistrome_cistrome", "cistrome_epigenome",
)
REGION_DB = os.environ.get("LOLA_REGION_DB", "/opt/lola/LOLACore/hg38")
MAX_RESULT_ROWS = 10_000
RESULT_COLUMNS = [
    "dbSet", "collection", "description", "cellType", "tissue", "antibody",
    "treatment", "dataSource", "filename",
    "support", "b", "c", "d", "oddsRatio", "pValue", "qValue", "pValueLog",
    "rnkSup", "rnkPV", "rnkOR", "maxRnk", "meanRnk",
]


def decode_table(table) -> pd.DataFrame:
    """A table from a DMP/DMR result: Parquet in base64 ({"format", "data"}) or records."""
    if isinstance(table, dict) and table.get("format") == "parquet+base64":
        return pd.read_parquet(io.BytesIO(base64.b64decode(table["data"])))
    return pd.DataFrame(table)


def _validate(dmrs: pd.DataFrame, universe: pd.DataFrame, collections, direction, top_n, known) -> None:
    missing = {"chromosome", "start", "end"} - set(dmrs.columns)
    if missing:
        raise ValueError(f"dmrs needs the columns chromosome, start and end; missing {sorted(missing)}")
    missing = {"chromosome", "position"} - set(universe.columns)
    if missing:
        raise ValueError(f"universe needs the columns chromosome and position; missing {sorted(missing)}")
    unknown = sorted(set(collections) - set(known))
    if unknown:
        raise ValueError(f"Unknown collection(s) {unknown}; available: {list(known)}")
    if direction not in ("enrichment", "depletion"):
        raise ValueError(f"direction must be 'enrichment' or 'depletion', got {direction!r}")
    if not isinstance(top_n, int) or isinstance(top_n, bool) or not 1 <= top_n <= MAX_RESULT_ROWS:
        raise ValueError(f"top_n must be an integer between 1 and {MAX_RESULT_ROWS:,}, got {top_n!r}")


def lola_enrichment(
    dmrs: pd.DataFrame,
    universe: pd.DataFrame,
    collections: tuple[str, ...] = COLLECTIONS,
    direction: str = "enrichment",
    min_overlap: int = 1,
    top_n: int = 1000,
    region_db: str = REGION_DB,
    known: tuple[str, ...] = COLLECTIONS,
) -> dict:
    """Undecorated implementation (importable from tests, with another database)."""
    _validate(dmrs, universe, collections, direction, top_n, known)
    output = {"summary": {}, "warnings": [], "results": []}
    dmrs = dmrs[["chromosome", "start", "end"]].dropna()
    universe = universe[["chromosome", "position"]].dropna()
    universe = universe[universe["position"] > 0]
    if dmrs.empty:
        output["warnings"].append("No DMRs given: nothing to test")
        return output

    info(f"LOLA: {len(dmrs)} DMRs against {len(universe):,} universe CpGs, collections {list(collections)}")
    scores, annotation, n_user_cpgs = run_lola(dmrs, universe, region_db, tuple(collections), min_overlap)
    info(f"{n_user_cpgs} universe CpGs lie in the DMRs; {len(scores):,} region sets tested")

    alternative = "less" if direction == "depletion" else "greater"
    for column in ("support", "b", "c", "d"):
        scores[column] = scores[column].astype(int)
    if (scores[["b", "c", "d"]] < 0).any().any():
        raise RuntimeError("Negative counts in the 2x2 tables: the DMR CpGs are not all in the universe")
    fisher = [fisher_exact([[r.support, r.b], [r.c, r.d]], alternative=alternative)
              for r in scores.itertuples()]
    scores["oddsRatio"] = [f[0] for f in fisher]
    scores["pValue"] = [f[1] for f in fisher]
    scores["qValue"] = false_discovery_control(scores["pValue"], method="bh")
    scores["pValueLog"] = -np.log10(scores["pValue"].clip(lower=1e-322))
    scores["rnkSup"] = scores["support"].rank(ascending=False, method="min")
    scores["rnkPV"] = scores["pValueLog"].rank(ascending=False, method="min")
    scores["rnkOR"] = scores["oddsRatio"].rank(ascending=False, method="min")
    scores["maxRnk"] = scores[["rnkSup", "rnkPV", "rnkOR"]].max(axis=1)
    scores["meanRnk"] = scores[["rnkSup", "rnkPV", "rnkOR"]].mean(axis=1).round(3)

    annotation = annotation.reset_index(drop=True)
    annotation["dbSet"] = (annotation.index + 1).astype(str)
    scores["dbSet"] = scores["dbSet"].astype(str)
    result = scores.merge(annotation, on="dbSet", how="left")
    for column in RESULT_COLUMNS:
        if column not in result.columns:
            result[column] = None
    result = result.sort_values(["pValue", "meanRnk"], ascending=[True, True]).reset_index(drop=True)

    output["summary"] = {
        "n_dmrs": int(len(dmrs)),
        "n_universe": int(len(universe)),
        "n_dmr_cpgs": n_user_cpgs,
        "n_region_sets": int(len(result)),
        "n_significant_q_0.05": int((result["qValue"] < 0.05).sum()),
        "direction": direction,
        "collections": list(collections),
        "n_returned": int(min(top_n, len(result))),
    }
    if n_user_cpgs < len(dmrs):
        output["warnings"].append(
            f"Only {n_user_cpgs} universe CpGs lie in the {len(dmrs)} DMRs: check that the DMRs and the universe come from the same DMP/DMR result"
        )
    top = result.head(top_n)[RESULT_COLUMNS]
    output["results"] = top.astype(object).where(top.notna(), None).to_dict(orient="records")
    info(f"{output['summary']['n_significant_q_0.05']} region sets significant at q < 0.05")
    return output


def latest_task(client, accepts, image_hints, label: str, limit: int = 50) -> int:
    """Id of the most recent finished task of this session whose result `accepts`.

    Lists the session's finished top-level tasks, newest first, and checks their
    results; tasks whose image contains one of `image_hints` are checked first, to
    avoid reading large results of other analyses.
    """
    session_id = getattr(client, "session_id", None)
    if session_id is None or not hasattr(client, "request"):
        raise ValueError(f"No session to look in: give the {label} task number")
    response = client.request("task", params={
        "session_id": session_id, "is_user_created": 1, "status": "completed",
        "per_page": limit})
    tasks = response.get("data", []) if isinstance(response, dict) else []
    tasks = sorted(tasks, key=lambda t: (not any(h in str(t.get("image", "")) for h in image_hints),
                                         -int(t["id"])))
    for task in tasks:
        results = [r for r in client.result.from_task(int(task["id"])) if r is not None]
        if results and accepts(results[0]):
            return int(task["id"])
    raise ValueError(f"No finished {label} task in this session: run {label} first in this session, "
                     "or give a task number")


def is_dmp_result(result: Any) -> bool:
    """A DMP/DMR result with DMRs and the universe of tested probes (2.1.0 or later)."""
    return isinstance(result, dict) and "global_dmrs" in result and "universe" in result


def inputs_from_task(client: AlgorithmClient, task_id: int) -> tuple[Any, Any, dict]:
    """DMRs and universe of a finished DMP/DMR task (v6-dmp-dmr-py 2.1 or later) of
    the collaboration, and a description of where they come from."""
    task = client.task.get(int(task_id))
    # A real client returns the task, or HQ's message when it cannot be read; the
    # mock network used in tests has no task details (None)
    if task is None:
        task = {"id": int(task_id), "name": None}
    elif not isinstance(task, dict) or task.get("id") is None:
        message = task.get("msg") if isinstance(task, dict) else task
        raise ValueError(f"Task {task_id} could not be read: {message}")
    results = [r for r in client.result.from_task(int(task_id)) if r is not None]
    name = task.get("name")
    if not results:
        raise ValueError(f"Task {task_id} ('{name}') has no result yet: is it finished and did it succeed?")
    result = results[0]
    if not (isinstance(result, dict) and "global_dmrs" in result):
        raise ValueError(f"Task {task_id} ('{name}') is not a DMP/DMR result")
    if "universe" not in result:
        raise ValueError(f"DMP/DMR task {task_id} ('{name}') has no universe of tested probes: "
                         "it comes from DMP/DMR before 2.1.0; run DMP/DMR again")
    comparison = result.get("comparison", {})
    n_universe = result.get("summary", {}).get("n_universe")
    description = (f"the {len(result['global_dmrs'])} DMRs and "
                   f"{f'{n_universe:,}' if isinstance(n_universe, int) else 'the'} "
                   f"tested probes of DMP/DMR task {task_id} '{name}' "
                   f"({comparison.get('cohort_b')} vs {comparison.get('cohort_a')})")
    return result["global_dmrs"], result["universe"], {
        "source": "task", "task_id": int(task_id), "task_name": name,
        "algorithm": "DMP/DMR", "description": description}


@central
@algorithm_client
def central_function(
    client: AlgorithmClient,
    dmp_task_id: int | None = None,
    dmrs: list[dict] | None = None,
    universe: Any = None,
    collections: list[str] | None = None,
    direction: Literal["enrichment", "depletion"] = "enrichment",
    min_overlap: int = 1,
    top_n: int = 1000,
) -> Any:
    """LOLA enrichment of the DMRs of a DMP/DMR result, against LOLACore hg38.

    Parameters
    ----------
    dmp_task_id : int | None
        Id of a finished DMP/DMR task (2.1.0 or later): its DMRs and universe are
        used. Give either this or ``dmrs`` and ``universe``; with neither, the most
        recent finished DMP/DMR task of this session is used, and the result says
        which.
    dmrs : list[dict] | None
        The ``global_dmrs`` of a v6-dmp-dmr-py result (chromosome, start, end).
    universe : dict | None
        The ``universe`` of the same DMP/DMR result (tested CpGs with chromosome
        and position, Parquet in base64), or a list of records.
    collections : list[str] | None
        LOLACore collections to test (default: all available, see COLLECTIONS).
    direction : str
        "enrichment" (default) or "depletion": the one-sided Fisher test.
    min_overlap : int
        Minimum overlap in bp between a region and a reference region.
    top_n : int
        Number of region sets returned (smallest p-value first), 1 to 10,000.

    Returns
    -------
    dict
        ``input`` (where the DMRs and universe came from), ``summary`` (counts,
        direction, collections), ``warnings`` and ``results``: one record per
        reference region set with its annotation, the 2x2 counts (support, b, c,
        d), oddsRatio, pValue, qValue (Benjamini-Hochberg over all region sets
        tested) and LOLA's ranks.
    """
    given = dmrs is not None and universe not in (None, "", {})
    if dmp_task_id not in (None, "") and given:
        raise ValueError("Give either dmp_task_id or dmrs and universe, not both")
    if dmp_task_id not in (None, ""):
        dmrs, universe, provenance = inputs_from_task(client, dmp_task_id)
    elif given:
        provenance = {"source": "files", "description": "uploaded DMRs and universe"}
    else:
        task_id = latest_task(client, is_dmp_result, ("dmp-dmr",), "DMP/DMR")
        dmrs, universe, provenance = inputs_from_task(client, task_id)
        provenance["selected"] = "automatically"
        provenance["description"] = ("Automatically selected the most recent DMP/DMR task in this "
                                     f"session (task {task_id}): " + provenance["description"])
    info(f"Input: {provenance['description']}")
    output = lola_enrichment(
        decode_table(dmrs), decode_table(universe),
        tuple(collections) if collections else COLLECTIONS,
        direction, min_overlap, top_n,
    )
    return {"input": provenance, **output}
