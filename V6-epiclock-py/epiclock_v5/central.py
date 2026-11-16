"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""
from typing import Any, List
import pandas as pd
import numpy as np
from scipy import stats

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.client import AlgorithmClient

from .clock_bundle import CLOCK_SETS, DEFAULT_SET, ONLY_LISTED, clocks_in_set, load_bundle
from .federated import DEFAULT_BIN_WIDTH, DEFAULT_CLOCKS, DEFAULT_MIN_BIN_COUNT, NODE_RESULT_COLUMNS

GLOBAL_RESULT_COLUMNS = [
    "cohort",
    "clock",
    "mean_age_global",
    "sd_age_global",
    "n_samples_total",
    "n_nodes",
    "coverage_pct_min",
    "coverage_pct_max",
    "n_with_age_total",
    "mean_chronological_age_global",
    "mean_age_acceleration_global",
    "sd_age_acceleration_global",
    "mean_abs_error_global",
    "correlation_global",
]

ACCURACY_COLUMNS = [
    "clock",
    "n_with_age_total",
    "n_cohorts",
    "mean_abs_error_global",
    "correlation_global",
    "mean_age_acceleration_global",
    "coverage_pct_min",
]


def coverage_pct(value) -> float | None:
    """A coverage (share of a clock's CpGs present, 0-1) as a percentage, 1 decimal.

    There is no threshold: EPIC v2 lacks some clock CpGs, and the user judges
    whether a clock's coverage is enough."""
    if value is None or pd.isna(value):
        return None
    return round(100 * float(value), 1)


def pooled_mean_sd(n: np.ndarray, means: np.ndarray, sds: np.ndarray) -> tuple[float, float | None]:
    """Mean and sample standard deviation of all samples, from per-node n, mean and SD."""
    total_n = n.sum()
    mean = float((n * means).sum() / total_n)
    if total_n < 2:
        return mean, None
    sum_squares = ((n - 1) * np.nan_to_num(sds) ** 2 + n * (means - mean) ** 2).sum()
    return mean, float(np.sqrt(sum_squares / (total_n - 1)))


def pooled_accuracy(reported: pd.DataFrame) -> tuple[float | None, float | None]:
    """Mean absolute error and Pearson correlation between predicted and chronological
    age over all samples of `reported` (rows of node results with an age comparison).

    Exact from per-row n, means and SDs: with acceleration a = predicted - real,
    cov(predicted, real) = (var(predicted) + var(real) - var(a)) / 2, all pooled
    with pooled_mean_sd. Rows from nodes without these fields (older versions) are
    not used; then both are empty.
    """
    needed = ["sd_chronological_age", "sd_predicted_age_with_age", "mean_abs_error"]
    if reported.empty or any(c not in reported for c in needed):
        return None, None
    reported = reported.dropna(subset=needed)
    if reported.empty:
        return None, None
    n = reported["n_with_age"].to_numpy(dtype=float)
    real = reported["mean_chronological_age"].to_numpy(dtype=float)
    accel = reported["mean_age_acceleration"].to_numpy(dtype=float)
    mae = float((n * reported["mean_abs_error"].to_numpy(dtype=float)).sum() / n.sum())
    _, sd_real = pooled_mean_sd(n, real, reported["sd_chronological_age"].to_numpy(dtype=float))
    _, sd_pred = pooled_mean_sd(n, real + accel, reported["sd_predicted_age_with_age"].to_numpy(dtype=float))
    _, sd_accel = pooled_mean_sd(n, accel, reported["sd_age_acceleration"].to_numpy(dtype=float))
    if not sd_real or not sd_pred or sd_accel is None:
        return mae, None
    cov = (sd_pred ** 2 + sd_real ** 2 - sd_accel ** 2) / 2
    return mae, float(np.clip(cov / (sd_pred * sd_real), -1.0, 1.0))


def clock_accuracy(combined: pd.DataFrame) -> pd.DataFrame:
    """Per clock, over all cohorts and nodes: how close the predicted age is to the
    chronological age (mean absolute error, correlation, mean acceleration)."""
    rows = []
    if "mean_age_acceleration" not in combined:
        return pd.DataFrame(columns=ACCURACY_COLUMNS)
    for clk, group in combined.groupby("clock"):
        reported = group[pd.to_numeric(group["mean_age_acceleration"], errors="coerce").notna()]
        if reported.empty:
            continue
        n = reported["n_with_age"].to_numpy(dtype=float)
        accel_mean, _ = pooled_mean_sd(
            n, reported["mean_age_acceleration"].to_numpy(dtype=float), np.zeros(len(n)))
        mae, corr = pooled_accuracy(reported)
        rows.append({
            "clock": clk,
            "n_with_age_total": int(n.sum()),
            "n_cohorts": int(reported["cohort"].nunique()),
            "mean_abs_error_global": mae,
            "correlation_global": corr,
            "mean_age_acceleration_global": accel_mean,
            "coverage_pct_min": coverage_pct(group["coverage"].min()),
        })
    table = pd.DataFrame(rows, columns=ACCURACY_COLUMNS)
    return table.sort_values("mean_abs_error_global", na_position="last", kind="stable").reset_index(drop=True)


def pooled_age_comparison(group: pd.DataFrame) -> dict:
    """Pool the chronological age and age acceleration of one (cohort, clock) over
    the nodes that reported them (weighted by n_with_age)."""
    fields = {
        "n_with_age_total": 0,
        "mean_chronological_age_global": None,
        "mean_age_acceleration_global": None,
        "sd_age_acceleration_global": None,
        "mean_abs_error_global": None,
        "correlation_global": None,
    }
    reported = group[pd.to_numeric(group["mean_age_acceleration"], errors="coerce").notna()]
    if reported.empty:
        return fields
    n = reported["n_with_age"].to_numpy(dtype=float)
    real_mean, _ = pooled_mean_sd(
        n, reported["mean_chronological_age"].to_numpy(dtype=float), np.zeros(len(n))
    )
    accel_mean, accel_sd = pooled_mean_sd(
        n, reported["mean_age_acceleration"].to_numpy(dtype=float),
        reported["sd_age_acceleration"].to_numpy(dtype=float),
    )
    fields.update({
        "n_with_age_total": int(n.sum()),
        "mean_chronological_age_global": real_mean,
        "mean_age_acceleration_global": accel_mean,
        "sd_age_acceleration_global": accel_sd,
    })
    fields["mean_abs_error_global"], fields["correlation_global"] = pooled_accuracy(reported)
    return fields


def node_results_frame(results: list) -> pd.DataFrame:
    """All nodes' (cohort, clock) records in one DataFrame."""
    node_dfs = []
    for r in results:
        organization_id = None
        if isinstance(r, dict):
            organization_id = r.get("organization_id")
            r = r.get("results", [])
        if isinstance(r, pd.DataFrame):
            node_dfs.append(r)
        elif isinstance(r, list):
            frame = pd.DataFrame(r, columns=NODE_RESULT_COLUMNS)
            frame["organization_id"] = organization_id
            node_dfs.append(frame)
    if not node_dfs:
        return pd.DataFrame(columns=NODE_RESULT_COLUMNS)
    return pd.concat(node_dfs, ignore_index=True)


def aggregate_cohort_results(results: list) -> pd.DataFrame:
    """
    Combine node-level statistics into global statistics per (cohort, clock).

    For the nodes i that returned a (cohort, clock):
        n_samples_total = sum(n_i)
        mean_age_global = sum(n_i * mean_i) / n_samples_total
        sd_age_global   = sqrt(sum((n_i - 1) * sd_i^2 + n_i * (mean_i - mean_age_global)^2)
                               / (n_samples_total - 1))
    which equals the mean and sample standard deviation of all the cohort's samples.
    The age acceleration (predicted minus chronological age) is pooled the same way,
    weighted by n_with_age, over the nodes that reported it.

    Parameters:
    -----------
    results : list
        One entry per node: ``{"results": records, ...}`` as returned by
        federated_function, or directly a list of records / DataFrame with columns
        cohort, clock, n_samples, mean_age and sd_age

    Returns:
    --------
    pd.DataFrame : Columns cohort, clock, mean_age_global, sd_age_global,
        n_samples_total, n_nodes, coverage_pct_min, coverage_pct_max (lowest and
        highest clock coverage, in %, over the nodes that contributed), and
        n_with_age_total,
        mean_chronological_age_global, mean_age_acceleration_global,
        sd_age_acceleration_global, mean_abs_error_global and correlation_global
        (Pearson, predicted vs chronological age) (empty if no node reported ages)
    """
    combined = node_results_frame(results)
    if combined.empty:
        return pd.DataFrame(columns=GLOBAL_RESULT_COLUMNS)

    rows = []
    for (cohort, clk), group in combined.groupby(["cohort", "clock"]):
        n = group["n_samples"].to_numpy(dtype=float)
        means = group["mean_age"].to_numpy(dtype=float)
        sds = group["sd_age"].to_numpy(dtype=float)

        total_n = n.sum()
        mean_global, sd_global = pooled_mean_sd(n, means, sds)

        rows.append({
            "cohort": cohort,
            "clock": clk,
            "mean_age_global": mean_global,
            "sd_age_global": sd_global,
            "n_samples_total": int(total_n),
            "n_nodes": int(len(group)),
            "coverage_pct_min": coverage_pct(group["coverage"].min()),
            "coverage_pct_max": coverage_pct(group["coverage"].max()),
            **pooled_age_comparison(group),
        })

    return pd.DataFrame(rows, columns=GLOBAL_RESULT_COLUMNS)


# ---------------------------------------------------------------------------
# Cohort comparison (1.6.0)
# ---------------------------------------------------------------------------

COMPARISON_COLUMNS = [
    "clock", "cohort", "reference_cohort", "n", "n_reference", "difference",
    "ci_low", "ci_high", "p_value", "p_adjusted",
]
ADJUSTED_COLUMNS = COMPARISON_COLUMNS + ["age_slope"]
DISTRIBUTION_COLUMNS = [
    "cohort", "clock", "measure", "bin_start", "bin_end", "n", "n_total", "n_not_shown",
]
CALIBRATION_COLUMNS = [
    "clock", "age_bin_start", "age_bin_end", "n", "mean_chronological_age",
    "mean_predicted_age", "sd_predicted_age", "mean_difference",
]


def benjamini_hochberg(p_values) -> list:
    """Benjamini-Hochberg adjusted p-values (None stays None)."""
    p = pd.Series(p_values, dtype=float)
    valid = p.dropna()
    if valid.empty:
        return [None] * len(p)
    order = valid.sort_values()
    m = len(order)
    adjusted = (order * m / np.arange(1, m + 1))[::-1].cummin()[::-1].clip(upper=1.0)
    out = p.copy()
    out.loc[adjusted.index] = adjusted
    return [None if pd.isna(v) else float(v) for v in out]


def choose_reference(cohorts: list, reference_cohort: str | None) -> str | None:
    """The reference cohort: the one given, or the first in alphabetical order."""
    cohorts = sorted(map(str, cohorts))
    if not cohorts:
        return None
    if reference_cohort not in (None, ""):
        if str(reference_cohort) not in cohorts:
            raise ValueError(f"reference_cohort {reference_cohort!r} is not among the cohorts {cohorts}")
        return str(reference_cohort)
    return cohorts[0]


def welch_comparison(global_summary: pd.DataFrame, reference: str | None) -> pd.DataFrame:
    """Per clock, each cohort minus the reference cohort in mean age acceleration
    (predicted minus chronological age): difference, 95% CI and Welch p-value from
    the pooled n, mean and SD; p_adjusted: Benjamini-Hochberg over the clocks (per
    cohort). Exact from the pooled statistics, as if computed on all samples."""
    rows = []
    if reference is None or global_summary.empty:
        return pd.DataFrame(columns=COMPARISON_COLUMNS)
    table = global_summary.copy()
    table["cohort"] = table["cohort"].astype(str)
    for clk, group in table.groupby("clock"):
        ref = group[group["cohort"] == reference]
        if ref.empty:
            continue
        ref = ref.iloc[0]
        n_r, m_r, s_r = ref["n_with_age_total"], ref["mean_age_acceleration_global"], ref["sd_age_acceleration_global"]
        for _, row in group[group["cohort"] != reference].iterrows():
            n_c, m_c, s_c = row["n_with_age_total"], row["mean_age_acceleration_global"], row["sd_age_acceleration_global"]
            values = [n_r, m_r, s_r, n_c, m_c, s_c]
            if any(v is None or pd.isna(v) for v in values) or n_r < 2 or n_c < 2:
                continue
            v_c, v_r = s_c ** 2 / n_c, s_r ** 2 / n_r
            se = np.sqrt(v_c + v_r)
            diff = float(m_c - m_r)
            if se == 0:
                continue
            df = (v_c + v_r) ** 2 / (v_c ** 2 / (n_c - 1) + v_r ** 2 / (n_r - 1))
            t = stats.t.ppf(0.975, df)
            rows.append({
                "clock": clk, "cohort": row["cohort"], "reference_cohort": reference,
                "n": int(n_c), "n_reference": int(n_r), "difference": diff,
                "ci_low": float(diff - t * se), "ci_high": float(diff + t * se),
                "p_value": float(2 * stats.t.sf(abs(diff / se), df)),
            })
    out = pd.DataFrame(rows, columns=COMPARISON_COLUMNS)
    for cohort, index in out.groupby("cohort").groups.items():
        out.loc[index, "p_adjusted"] = benjamini_hochberg(out.loc[index, "p_value"])
    return out


def cohort_sums(combined: pd.DataFrame) -> pd.DataFrame:
    """Per (cohort, clock), pooled over the hospitals: n, and the sums of the
    chronological age a, the predicted age y, a^2, y^2 and a*y over the samples with
    both, rebuilt exactly from each hospital's n, means and SDs (the covariance from
    var(y) + var(a) - var(y - a) = 2 cov(y, a))."""
    needed = ["n_with_age", "mean_chronological_age", "mean_age_acceleration", "sd_chronological_age",
              "sd_predicted_age_with_age", "sd_age_acceleration"]
    if combined.empty or any(c not in combined for c in needed):
        return pd.DataFrame()
    rows = combined.dropna(subset=needed)
    rows = rows[pd.to_numeric(rows["n_with_age"], errors="coerce") >= 2]
    if rows.empty:
        return pd.DataFrame()
    n = rows["n_with_age"].astype(float)
    ma = rows["mean_chronological_age"].astype(float)
    my = ma + rows["mean_age_acceleration"].astype(float)
    sa, sy, sd = (rows[c].astype(float) for c in needed[3:])
    cov = (sy ** 2 + sa ** 2 - sd ** 2) / 2
    sums = pd.DataFrame({
        "cohort": rows["cohort"].astype(str), "clock": rows["clock"], "n": n,
        "a": n * ma, "y": n * my,
        "aa": (n - 1) * sa ** 2 + n * ma ** 2,
        "yy": (n - 1) * sy ** 2 + n * my ** 2,
        "ay": (n - 1) * cov + n * ma * my,
    })
    return sums.groupby(["cohort", "clock"], as_index=False).sum()


def age_adjusted_comparison(combined: pd.DataFrame, reference: str | None) -> pd.DataFrame:
    """Per clock, the linear regression predicted age ~ chronological age + cohort
    over all samples with an age (one common age slope): each cohort's coefficient
    is its difference from the reference cohort at the same chronological age, with
    95% CI and t-test p-value; p_adjusted: Benjamini-Hochberg over the clocks (per
    cohort). Fitted exactly from the per-cohort sums (cohort_sums)."""
    sums = cohort_sums(combined)
    if reference is None or sums.empty:
        return pd.DataFrame(columns=ADJUSTED_COLUMNS)
    rows = []
    for clk, group in sums.groupby("clock"):
        group = group.set_index("cohort")
        if reference not in group.index or len(group) < 2:
            continue
        others = [c for c in sorted(group.index) if c != reference]
        k = 2 + len(others)
        xtx, xty = np.zeros((k, k)), np.zeros(k)
        yy = group["yy"].sum()
        for cohort, g in group.iterrows():
            # design row: [1, age, one dummy per non-reference cohort]
            idx = [0, 1] + ([2 + others.index(cohort)] if cohort != reference else [])
            block = np.array([[g["n"], g["a"]], [g["a"], g["aa"]]])
            vec = np.array([g["y"], g["ay"]])
            if cohort != reference:
                block = np.block([[block, np.array([[g["n"]], [g["a"]]])],
                                  [np.array([[g["n"], g["a"], g["n"]]])]])
                vec = np.append(vec, g["y"])
            xtx[np.ix_(idx, idx)] += block
            xty[idx] += vec
        n_total = group["n"].sum()
        dof = n_total - k
        if dof < 1 or np.linalg.matrix_rank(xtx) < k:
            continue
        inverse = np.linalg.inv(xtx)
        beta = inverse @ xty
        rss = max(yy - beta @ xty, 0.0)
        sigma2 = rss / dof
        t = stats.t.ppf(0.975, dof)
        for j, cohort in enumerate(others, start=2):
            se = np.sqrt(sigma2 * inverse[j, j])
            p = float(2 * stats.t.sf(abs(beta[j] / se), dof)) if se > 0 else None
            rows.append({
                "clock": clk, "cohort": cohort, "reference_cohort": reference,
                "n": int(group.loc[cohort, "n"]), "n_reference": int(group.loc[reference, "n"]),
                "difference": float(beta[j]), "ci_low": float(beta[j] - t * se),
                "ci_high": float(beta[j] + t * se), "p_value": p,
                "age_slope": float(beta[1]),
            })
    out = pd.DataFrame(rows, columns=ADJUSTED_COLUMNS)
    for cohort, index in out.groupby("cohort").groups.items():
        out.loc[index, "p_adjusted"] = benjamini_hochberg(out.loc[index, "p_value"])
    return out


def pooled_distribution(results: list, width: float) -> pd.DataFrame:
    """Sum the hospitals' binned counts per (cohort, clock, measure, bin). A bin a
    hospital did not report (fewer than the minimum there) adds nothing: n_not_shown
    says how many samples of that cohort and clock are in such bins."""
    counts, totals = {}, {}
    for r in results:
        if not isinstance(r, dict):
            continue
        for entry in r.get("distribution", []):
            key = (str(entry["cohort"]), entry["clock"], entry["measure"])
            total = totals.setdefault(key, [0, 0])
            total[0] += entry["n"]
            total[1] += entry["n_not_shown"]
            for b in entry["bins"]:
                counts[key + (b["bin_start"],)] = counts.get(key + (b["bin_start"],), 0) + b["n"]
    rows = [
        {"cohort": c, "clock": clk, "measure": m, "bin_start": start, "bin_end": start + width,
         "n": n, "n_total": totals[(c, clk, m)][0], "n_not_shown": totals[(c, clk, m)][1]}
        for (c, clk, m, start), n in sorted(counts.items(), key=lambda i: (i[0][1], i[0][2], i[0][0], i[0][3]))
    ]
    return pd.DataFrame(rows, columns=DISTRIBUTION_COLUMNS)


def pooled_calibration(results: list, width: float) -> pd.DataFrame:
    """Per clock and chronological-age bin, pooled over the hospitals that reported
    it: mean chronological age, mean and SD of the predicted age, and their mean
    difference (predicted minus chronological)."""
    sums = {}
    for r in results:
        if not isinstance(r, dict):
            continue
        for entry in r.get("calibration", []):
            s = sums.setdefault((entry["clock"], entry["age_bin_start"]), np.zeros(4))
            s += [entry["n"], entry["sum_predicted"], entry["sumsq_predicted"], entry["sum_chronological"]]
    rows = []
    for (clk, start), (n, sp, spp, sa) in sorted(sums.items()):
        mean_p = sp / n
        rows.append({
            "clock": clk, "age_bin_start": start, "age_bin_end": start + width, "n": int(n),
            "mean_chronological_age": sa / n, "mean_predicted_age": mean_p,
            "sd_predicted_age": float(np.sqrt(max(spp - n * mean_p ** 2, 0.0) / (n - 1))) if n > 1 else None,
            "mean_difference": mean_p - sa / n,
        })
    return pd.DataFrame(rows, columns=CALIBRATION_COLUMNS)


def comparison_notes(combined: pd.DataFrame, reference: str | None, welch: pd.DataFrame,
                     adjusted: pd.DataFrame) -> list[str]:
    """Plain-language caveats for the cohort comparisons."""
    if reference is None:
        return []
    if welch.empty and adjusted.empty:
        return ["No cohort comparison: it needs the chronological age of at least 2 samples "
                f"in {reference} and in another cohort (age_column)."]
    notes = [
        f"Cohorts are compared with {reference} (reference). Age acceleration: Welch test on "
        "predicted minus chronological age. Age-adjusted: linear regression predicted age ~ "
        "chronological age + cohort, one common age slope. p_adjusted: Benjamini-Hochberg over "
        "the clocks; the clocks are correlated (they share CpGs and all estimate age), so they "
        "are not independent confirmations of each other.",
    ]
    if "organization_id" in combined:
        sites = combined.groupby(combined["cohort"].astype(str))["organization_id"].agg(lambda s: set(s.dropna()))
        compared = [c for c in sites.index if c == reference or c in set(welch["cohort"]) | set(adjusted["cohort"])]
        if compared and all(len(sites[c]) == 1 for c in compared) and \
                len({next(iter(sites[c])) for c in compared}) == len(compared):
            notes.append("Each compared cohort comes from a single, different hospital: a "
                         "difference between cohorts may also be a difference between hospitals.")
    return notes


def check_requested_clocks(lista_relojes: List[str], bundle: dict | None) -> None:
    """Raise a clear error for clocks that are not bundled in this image.

    Without a bundle (local development, online pyaging) any clock is accepted.
    """
    if bundle is None:
        return
    available = bundle["clocks"]
    unknown = [c for c in lista_relojes if c not in available]
    if unknown:
        raise ValueError(
            f"Clock(s) {unknown} are not available in this algorithm image. "
            f"{len(available)} clocks are available: {', '.join(sorted(available))}"
        )


def summarise_clock_coverage(
    results: list, lista_relojes: List[str], bundle: dict | None = None,
) -> list[dict]:
    """Per clock: lowest and highest coverage (in %) over the nodes, and on how many
    nodes it failed to compute (those nodes have no results).
    With a clock bundle, also the clock's non-CpG inputs (``extra_features``, which
    the algorithm does not provide; pyaging fills them with reference values or 0),
    tissue and population from pyaging's catalogue."""
    rows = []
    for clk in lista_relojes:
        values = [
            r["clock_coverage"].get(clk)
            for r in results
            if isinstance(r, dict) and clk in r.get("clock_coverage", {})
        ]
        measured = [v for v in values if v is not None]
        rows.append({
            "clock": clk,
            "coverage_pct_min": coverage_pct(min(measured)) if measured else None,
            "coverage_pct_max": coverage_pct(max(measured)) if measured else None,
            "n_nodes_reported": len(values),
            "n_nodes_failed": sum(v is None for v in values),
        })
        if bundle is not None and clk in bundle["clocks"]:
            entry = bundle["clocks"][clk]
            rows[-1].update({
                "extra_features": entry.get("extra_features", []),
                "tissue": entry.get("tissue"),
                "population": entry.get("population"),
                # What the clock estimates, in which unit, and where it comes from
                "predicts": entry.get("predicts"),
                "unit": entry.get("unit"),
                "year": entry.get("year"),
                "citation": entry.get("citation"),
            })
    return rows


def select_organizations(client: AlgorithmClient, organizations_to_include: list[int] | None) -> list[int]:
    """Organization ids of the collaboration, or only those in `organizations_to_include`."""
    org_ids = [organization.get("id") for organization in client.organization.list()]
    if not organizations_to_include:
        return org_ids
    wanted = [int(org_id) for org_id in organizations_to_include]
    unknown = sorted(set(wanted) - set(org_ids))
    if unknown:
        raise ValueError(f"organizations_to_include: {unknown} are not in this collaboration ({org_ids})")
    return [org_id for org_id in org_ids if org_id in wanted]


def check_subtask(task: Any, name: str) -> int:
    """The id of a created subtask. HQ answers with a message instead of a task when
    it refuses one (e.g. the dataframe is missing at a node); waiting would never end."""
    if not isinstance(task, dict) or task.get("id") is None:
        message = task.get("msg") if isinstance(task, dict) else task
        raise RuntimeError(f"HQ did not create the subtask '{name}': {message}")
    return task["id"]


def resolve_clocks(clock_set: str | None, listed: List[str] | None, bundle: dict | None) -> List[str]:
    """The clocks to calculate: a clock set (see CLOCK_SETS) plus the listed clocks.
    Without a set: the listed clocks, or DEFAULT_CLOCKS if none are listed."""
    listed = [clock for clock in (listed or []) if clock]
    if clock_set in (None, ""):
        return listed or list(DEFAULT_CLOCKS)
    if clock_set == ONLY_LISTED:
        if not listed:
            raise ValueError("Clock set 'Only the clocks listed below', but no clock is listed")
        return listed
    if clock_set == DEFAULT_SET:
        base = list(DEFAULT_CLOCKS)
    elif clock_set in CLOCK_SETS:
        if bundle is None:
            raise ValueError("Clock sets need the clock bundle of the Docker image")
        base = clocks_in_set(clock_set, bundle)
    else:
        raise ValueError(f"Unknown clock set {clock_set!r}; choose one of {list(CLOCK_SETS)}")
    return base + [clock for clock in listed if clock not in base]


@central
@algorithm_client
def central_function(
    client: AlgorithmClient,
    lista_relojes: List[str] | None = None,
    clock_set: str | None = None,
    cohort_column: str = "cohort",
    min_samples: int = 3,
    age_column: str | None = "age",
    organizations_to_include: List[int] | None = None,
    reference_cohort: str | None = None,
    bin_width: float = DEFAULT_BIN_WIDTH,
    min_bin_count: int = DEFAULT_MIN_BIN_COUNT,
) -> Any:
    """
    Central aggregation of epigenetic age predictions per cohort.

    Implements PRIVACY-PRESERVING AGGREGATION: nodes return only the number of
    samples, mean and standard deviation per (cohort, clock), never individual
    patient data. A cohort may span several organizations; the central node pools
    each cohort across organizations, weighting by the number of samples.

    Parameters:
    -----------
    lista_relojes : List[str], optional
        pyaging clock names to calculate (e.g., ['horvath2013', 'hannum', 'pedbe']).
        If not given or empty, DEFAULT_CLOCKS (defined in federated.py) are
        calculated, unless a ``clock_set`` is chosen.
    clock_set : str, optional
        One of CLOCK_SETS (clock_bundle.py): the default clocks, a set computed
        from the bundle (e.g. blood chronological-age clocks), or only the
        listed clocks. The clocks of ``lista_relojes`` are added to the set.
    cohort_column : str, optional
        Name of the column in each node's data that holds the cohort of each sample
        (default: "cohort")
    min_samples : int, optional
        Minimum number of samples a cohort needs on a node to be included in that
        node's result (default: 3)
    age_column : str, optional
        Column in each node's data with the chronological age of each sample
        (default: "age", added by the IDAT extraction's sample_columns). Nodes that
        have it also report the age acceleration (predicted minus chronological
        age); None skips the comparison.
    organizations_to_include : List[int], optional
        Organizations (hospitals) to use; all of the collaboration if not given.
    reference_cohort : str, optional
        Cohort the others are compared with (``cohort_comparison``,
        ``age_adjusted_comparison``); the first in alphabetical order if not given.
    bin_width : float, optional
        Width in years of the bins of the distribution (predicted age, age
        acceleration) and of the chronological-age bins of the calibration (default 2).
    min_bin_count : int, optional
        Minimum samples a bin needs at a hospital to be reported (default 5);
        smaller bins are left out, and ``n_not_shown`` counts their samples.

    Returns:
    --------
    dict : ``{"results": [one record per (cohort, clock) with mean_age_global,
        sd_age_global, n_samples_total, n_nodes, coverage_pct_min, coverage_pct_max
        (clock coverage in %: EPIC v2 lacks some clock CpGs; there is no threshold,
        the user judges it), n_with_age_total, mean_chronological_age_global,
        mean_age_acceleration_global and sd_age_acceleration_global], "clock_coverage": [one record per clock with coverage_pct_min,
        coverage_pct_max, n_nodes_reported and n_nodes_failed],
        "n_nodes_legacy_loci_mapping": nodes that mapped renamed EPIC v2 CpGs (packaged
        mapping or $MANIFEST_PATH)}``, and (1.6.0) ``cohort_comparison`` (age
        acceleration, Welch), ``age_adjusted_comparison`` (regression on age and
        cohort), ``distribution`` (binned counts), ``calibration`` (mean predicted
        age per chronological-age bin) and ``summary`` (notes, as a one-row table).
    """
    bin_width = float(bin_width or DEFAULT_BIN_WIDTH)
    min_bin_count = int(min_bin_count or DEFAULT_MIN_BIN_COUNT)
    if bin_width <= 0:
        raise ValueError("bin_width must be positive")
    if min_bin_count < 3:
        raise ValueError("min_bin_count must be at least 3: smaller bins would show single patients")
    info("Retrieving organizations in collaboration")
    org_ids = select_organizations(client, organizations_to_include)
    info(f"Using {len(org_ids)} organizations")

    bundle = load_bundle()
    lista_relojes = resolve_clocks(clock_set, lista_relojes, bundle)
    info(f"Clocks ({clock_set or 'as listed or default'}): {lista_relojes}")
    check_requested_clocks(lista_relojes, bundle)
    info(f"Creating subtask to calculate clocks: {lista_relojes}")
    task = client.task.create(
        organizations=org_ids,
        method="federated_function",
        name="Epigenetic clock calculation",
        description="Calculate epigenetic age per cohort using multiple clocks (aggregated statistics only)",
        arguments={
            "lista_relojes": lista_relojes,
            "cohort_column": cohort_column,
            "min_samples": min_samples,
            "age_column": age_column,
            "bin_width": bin_width,
            "min_bin_count": min_bin_count,
        },
    )

    task_id = check_subtask(task, "Epigenetic clock calculation")
    info(f"Waiting for aggregated results from {len(org_ids)} organizations")
    results = client.wait_for_results(task_id=task_id)
    info(f"Received aggregated results from {len(results)} organizations")

    global_summary = aggregate_cohort_results(results)
    accuracy = clock_accuracy(node_results_frame(results))
    if global_summary.empty:
        warn("No cohort met the min_samples threshold on any node; no results to return")
    else:
        info(
            f"Global summary: {global_summary['cohort'].nunique()} cohort(s), "
            f"{global_summary['clock'].nunique()} clock(s)"
        )
    coverage = summarise_clock_coverage(results, lista_relojes, bundle)
    for row in coverage:
        if row.get("extra_features"):
            warn(f"Clock {row['clock']} also needs {row['extra_features']}, which this algorithm "
                 "does not provide; pyaging fills them with reference values or 0")
        if row["n_nodes_failed"]:
            warn(f"Clock {row['clock']} failed to compute on {row['n_nodes_failed']} node(s)")
    n_mapped = sum(bool(r.get("legacy_loci_mapping")) for r in results if isinstance(r, dict))
    if n_mapped < len(results):
        info(f"{len(results) - n_mapped} node(s) did not map renamed EPIC v2 CpGs (mapping disabled or missing)")
    combined = node_results_frame(results)
    reference = choose_reference(list(global_summary["cohort"].unique()), reference_cohort)
    welch = welch_comparison(global_summary, reference)
    adjusted = age_adjusted_comparison(combined, reference)
    notes = comparison_notes(combined, reference, welch, adjusted)
    for note in notes:
        info(note)
    return {
        "clock_set": clock_set,
        "clocks": lista_relojes,
        "results": global_summary.to_dict(orient="records"),
        "reference_cohort": reference,
        "cohort_comparison": welch.to_dict(orient="records"),
        "age_adjusted_comparison": adjusted.to_dict(orient="records"),
        "distribution": pooled_distribution(results, bin_width).to_dict(orient="records"),
        "calibration": pooled_calibration(results, bin_width).to_dict(orient="records"),
        "summary": [{"note": note} for note in notes],
        "clock_coverage": coverage,
        "clock_accuracy": accuracy.to_dict(orient="records"),
        "n_nodes_legacy_loci_mapping": n_mapped,
    }
