"""
Federated PCA of DNA methylation — functions that run at each hospital.

Adapted from v6-pca-py (exact covariance PCA from additively aggregated
cross-products) for methylation data:

- Input in long format, as produced by v6-extractionIDAT-py: one row per
  (probe, sample) with ``probe_id``, ``sample_label``, a value column
  (``beta`` or ``m_value``) and per-sample columns such as ``cohort``.
- The PCA uses the most variable CpGs over all hospitals (or a given list),
  restricted to CpGs present in every hospital.
- Sample scores never leave the hospital. Each hospital projects its own
  samples and returns only summaries (count, sum and sum of squares of each
  component) for the hospital and for each cohort with at least
  ``min_samples`` samples.

Three rounds, each one a federated function:

    probe_moments  -> per-CpG count, sum and sum of squares (to pick CpGs)
    partial_scatter -> X^T X of the selected CpGs, centred on the global mean
    partial_scores -> summaries of the local samples' component scores

Errors are returned as ``{"error": message}``.
"""

import base64

import numpy as np
import pandas as pd

from vantage6.algorithm.tools.util import info, warn
from vantage6.algorithm.decorator.action import federated
from vantage6.algorithm.decorator.data import dataframe


PROBE_COLUMN = "probe_id"
# Never summarise a single sample, whatever min_samples a request carries
MIN_SAMPLES_FLOOR = 2


# ---------------------------------------------------------------------
# ENCODING (large arrays travel as base64 float64, not JSON lists)
# ---------------------------------------------------------------------

def encode_array(array):

    array = np.ascontiguousarray(array, dtype="<f8")

    return {
        "shape": list(array.shape),
        "data": base64.b64encode(array.tobytes()).decode("ascii"),
    }


def decode_array(encoded):

    return np.frombuffer(
        base64.b64decode(encoded["data"]), dtype="<f8"
    ).reshape(encoded["shape"])


# ---------------------------------------------------------------------
# LOCAL DATA
# ---------------------------------------------------------------------

def _local_long(df, cohort_column, cohorts, sample_column, value_column,
                min_samples):
    """
    The long data of the samples in the analysis, with one cohort per
    sample. Returns ``(long, samples)`` where ``samples`` maps each sample to
    its cohort, or an error dictionary.
    """

    if df is None or df.empty:
        return {"error": "empty dataframe"}

    min_samples = max(int(min_samples), MIN_SAMPLES_FLOOR)

    if PROBE_COLUMN not in df.columns:
        return {
            "error": (
                f"no '{PROBE_COLUMN}' column: the data must be in long format "
                "(one row per probe and sample), as produced by the IDAT "
                "extraction"
            )
        }

    needed = [sample_column, value_column] + (
        [cohort_column] if cohort_column else []
    )
    missing = [column for column in needed if column not in df.columns]

    if missing:
        return {"error": f"missing columns: {missing}"}

    if cohort_column:
        samples = df[[sample_column, cohort_column]].drop_duplicates()
        if samples[sample_column].duplicated().any():
            return {"error": "a sample has more than one cohort"}
        samples = samples.set_index(sample_column)[cohort_column].astype(str)
    else:
        labels = df[sample_column].drop_duplicates()
        samples = pd.Series("all", index=labels.to_numpy())

    if cohorts:
        samples = samples[samples.isin([str(cohort) for cohort in cohorts])]

    if len(samples) < min_samples:
        warn(f"{len(samples)} samples < min_samples={min_samples}: skipped")
        return {"error": f"insufficient samples (< {min_samples})"}

    long = df[df[sample_column].isin(samples.index)]

    return long, samples


def _local_matrix(df, features, cohort_column, cohorts, sample_column,
                  value_column, min_samples):
    """
    One row per sample and one column per requested CpG (in the given
    order), and the cohort of each row. Returns ``(X, cohorts)`` or an
    error dictionary.
    """

    prepared = _local_long(
        df, cohort_column, cohorts, sample_column, value_column, min_samples
    )

    if isinstance(prepared, dict):
        return prepared

    long, samples = prepared
    long = long[long[PROBE_COLUMN].isin(features)]

    wide = long.pivot(
        index=sample_column, columns=PROBE_COLUMN, values=value_column
    ).reindex(index=samples.index, columns=list(features))

    wide = wide.apply(pd.to_numeric, errors="coerce")

    if wide.isna().any().any():
        n_missing = int(wide.isna().any(axis=0).sum())
        return {
            "error": (
                f"{n_missing} of the {len(features)} CpGs have missing values "
                "at this hospital"
            )
        }

    return wide.to_numpy(dtype=float), samples.to_numpy()


# ---------------------------------------------------------------------
# FEDERATED FUNCTIONS
# ---------------------------------------------------------------------

@federated
@dataframe(1)
def probe_moments(
    df1: pd.DataFrame,
    features: list | None = None,
    cohort_column: str | None = "cohort",
    cohorts: list | None = None,
    sample_column: str = "sample_label",
    value_column: str = "beta",
    min_samples: int = 5,
) -> dict:
    """
    Round 1: count, sum and sum of squares of each CpG over the local
    samples, for the CpGs measured in every local sample. With ``features``,
    only those CpGs.

    These are per-CpG sums over at least ``min_samples`` samples, the same
    kind of aggregate as a per-CpG mean.
    """

    info("PCA methylation: probe_moments")

    prepared = _local_long(
        df1, cohort_column, cohorts, sample_column, value_column, min_samples
    )

    if isinstance(prepared, dict):
        return prepared

    long, samples = prepared

    if features:
        long = long[long[PROBE_COLUMN].isin(features)]

    values = pd.to_numeric(long[value_column], errors="coerce")
    grouped = values.groupby(long[PROBE_COLUMN])

    moments = pd.DataFrame({
        "count": grouped.count(),
        "sum": grouped.sum(),
        "sumsq": (values ** 2).groupby(long[PROBE_COLUMN]).sum(),
    })

    complete = moments[moments["count"] == len(samples)]

    info(
        f"{len(samples)} samples; {len(complete)} of {len(moments)} CpGs "
        "measured in every sample"
    )

    return {
        "n": int(len(samples)),
        "probes": complete.index.astype(str).tolist(),
        "sum": encode_array(complete["sum"].to_numpy(dtype=float)),
        "sumsq": encode_array(complete["sumsq"].to_numpy(dtype=float)),
    }


@federated
@dataframe(1)
def partial_scatter(
    df1: pd.DataFrame,
    features: list,
    mean: list,
    scale: list | None = None,
    cohort_column: str | None = "cohort",
    cohorts: list | None = None,
    sample_column: str = "sample_label",
    value_column: str = "beta",
    min_samples: int = 5,
) -> dict:
    """
    Round 2: Z^T Z for the selected CpGs, where Z is the local data centred
    on the global mean (and divided by the global SD if ``scale`` is given).
    Summed over hospitals this is exactly (N - 1) times the global
    covariance (or correlation) matrix.
    """

    info("PCA methylation: partial_scatter")

    prepared = _local_matrix(
        df1, features, cohort_column, cohorts, sample_column, value_column,
        min_samples,
    )

    if isinstance(prepared, dict):
        warn(prepared["error"])
        return prepared

    X, _ = prepared
    Z = X - np.asarray(mean, dtype=float)

    if scale is not None:
        Z = Z / np.asarray(scale, dtype=float)

    p_exceeds_n = len(features) >= X.shape[0]

    if p_exceeds_n:
        warn(
            f"{len(features)} CpGs >= {X.shape[0]} samples at this hospital: "
            "the shared matrix carries much information about each sample"
        )

    return {
        "n": int(X.shape[0]),
        "scatter": encode_array(Z.T @ Z),
        "p_exceeds_n": bool(p_exceeds_n),
    }


@federated
@dataframe(1)
def partial_scores(
    df1: pd.DataFrame,
    features: list,
    mean: list,
    components: list,
    sd_components: list,
    scale: list | None = None,
    cohort_column: str | None = "cohort",
    cohorts: list | None = None,
    sample_column: str = "sample_label",
    value_column: str = "beta",
    min_samples: int = 5,
    outlier_sd: float = 3.0,
) -> dict:
    """
    Round 3: project the local samples on the components and return only
    summaries of their scores:

    - for the hospital: count, sum and sum of squares of each component;
    - for each cohort with at least ``min_samples`` samples here: the same;
      smaller cohorts are listed by name only (``suppressed``);
    - the number of samples further than ``outlier_sd`` SDs from the global
      mean on any component (a count, without saying which samples).
    """

    info("PCA methylation: partial_scores")

    prepared = _local_matrix(
        df1, features, cohort_column, cohorts, sample_column, value_column,
        min_samples,
    )

    if isinstance(prepared, dict):
        warn(prepared["error"])
        return prepared

    X, labels = prepared
    min_samples = max(int(min_samples), MIN_SAMPLES_FLOOR)
    Z = X - np.asarray(mean, dtype=float)

    if scale is not None:
        Z = Z / np.asarray(scale, dtype=float)

    scores = Z @ np.asarray(components, dtype=float)

    def summary(rows):
        return {
            "n": int(rows.shape[0]),
            "sum": rows.sum(axis=0).tolist(),
            "sumsq": (rows ** 2).sum(axis=0).tolist(),
        }

    groups, suppressed = [], []

    for cohort in sorted(set(labels)):
        rows = scores[labels == cohort]
        if rows.shape[0] < min_samples:
            suppressed.append(str(cohort))
            continue
        groups.append({"cohort": str(cohort), **summary(rows)})

    sd = np.asarray(sd_components, dtype=float)
    sd = np.where(sd > 0, sd, np.inf)
    outliers = int((np.abs(scores) > outlier_sd * sd).any(axis=1).sum())

    if outliers:
        warn(f"{outliers} sample(s) beyond {outlier_sd} SD on a component")

    return {
        "total": summary(scores),
        "groups": groups,
        "suppressed": suppressed,
        "n_outliers": outliers,
    }
