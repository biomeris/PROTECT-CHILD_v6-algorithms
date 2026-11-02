"""
This file contains all federated algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the federated task
or directly to the user (if they requested federated results).

Federated Linear SVM for discovery and characterization of cohort-specific DNA
methylation patterns.

The SVM is an independent module. It receives predefined cohorts and a set of
CpGs (for example selected by Elastic Net, or from any previous analysis).

Important:
    - Cohorts are predefined upstream.
    - Hospitals are data nodes, not cohorts. A node may contain samples from
      several cohorts, including cohorts that are not part of the analysis;
      those samples are excluded.
    - Patient-level methylation data never leaves the node.
    - Folds are assigned locally at each node (stratified by cohort).
    - Only aggregates leave the node: sums, class counts, gradient/Hessian
      sums of the SVM objective, and confusion matrices / score histograms
      pooled over at least ``min_samples`` samples.
    - Per-cohort methylation summaries are suppressed when a cohort has fewer
      than ``min_samples`` samples at the node.

Input layout: either wide (one row per sample, one column per CpG) or long,
as produced by v6-extractionIDAT-py (one row per sample and probe, with
``probe_id``, ``sample_column`` and ``value_column``). Long data are pivoted
at the node, for the requested CpGs only.

Jobs may carry ``organizations``: the hospitals whose data the job uses
(e.g. the training hospitals of a leave-one-hospital-out model). A node that
is not listed skips the job, so models of different hospital subsets can be
fitted in the same federated rounds.
"""

import os

import numpy as np

from vantage6.algorithm.tools.util import warn
from vantage6.common.globals import ContainerEnvNames
from vantage6.algorithm.decorator.action import federated
from vantage6.algorithm.decorator.data import dataframe


# ---------------------------------------------------------------------
# DATA PREPARATION
# ---------------------------------------------------------------------

def _organization_id():
    """
    Organization of this node (set by vantage6 in the algorithm's
    environment), or None if unknown.
    """

    value = os.environ.get(ContainerEnvNames.ORGANIZATION_ID.value)

    return int(value) if value not in (None, "") else None


def _job_applies(job, organization_id):
    """
    Whether this node takes part in a job (all nodes if the job does not
    list its ``organizations``).
    """

    organizations = job.get("organizations")

    if organizations is None:
        return True

    if organization_id is None:
        raise RuntimeError(
            "The node's organization id is not available, so jobs for a "
            "subset of hospitals cannot be assigned."
        )

    return organization_id in organizations


def _local_frame(df, features, outcome_column, sample_column, value_column):
    """
    One row per sample with the requested CpGs and the cohort label.

    Long data (a ``probe_id`` column) are pivoted for the requested CpGs
    only. Returns the frame or an error dictionary.
    """

    if "probe_id" in df.columns:

        missing = [
            column
            for column in [sample_column, value_column, outcome_column]
            if column not in df.columns
        ]

        if missing:
            warn(f"Missing columns: {missing}")
            return {
                "error": f"missing columns: {missing}"
            }

        long = df[df["probe_id"].isin(features)]

        wide = long.pivot(
            index=sample_column,
            columns="probe_id",
            values=value_column,
        )

        labels = df[[sample_column, outcome_column]].drop_duplicates()

        if labels[sample_column].duplicated().any():
            return {
                "error": "a sample has more than one cohort label"
            }

        df = wide.join(labels.set_index(sample_column), how="left")

    if outcome_column not in df.columns:
        warn(f"Missing columns: {[outcome_column]}")
        return {
            "error": f"missing columns: {[outcome_column]}"
        }

    # After quality control a hospital may lack some probes. The SVM needs
    # every CpG, so the hospital is left out, with the reason.
    missing = [
        feature
        for feature in features
        if feature not in df.columns or df[feature].isna().all()
    ]

    if missing:
        warn(f"{len(missing)} CpG(s) not available at this node")
        return {
            "error": (
                f"{len(missing)} of the {len(features)} CpGs are not "
                f"available at this node (e.g. {missing[:5]})"
            )
        }

    return df[list(features) + [outcome_column]]


def _prepare(df, features, outcome_column, classes, min_samples,
             sample_column="sample_label", value_column="beta"):
    """
    Validate the local dataframe and keep only complete observations that
    belong to one of the analysed cohorts.

    Returns ``(X, labels)`` or an error dictionary.
    """

    frame = _local_frame(
        df,
        features,
        outcome_column,
        sample_column,
        value_column,
    )

    if isinstance(frame, dict):
        return frame

    sub = frame.dropna()

    # Samples from cohorts that are not part of this comparison (e.g. an
    # "other" cohort stored at the same hospital) must not be used as
    # negatives.
    sub = sub[sub[outcome_column].isin(list(classes))]

    if len(sub) < min_samples:
        warn(
            f"Node has {len(sub)} samples < {min_samples} "
            f"minimum samples — skipped"
        )
        return {
            "error": f"insufficient samples (< {min_samples})"
        }

    X = sub[list(features)].to_numpy(dtype=float)
    labels = sub[outcome_column].to_numpy()

    return X, labels


def _make_stratified_folds(labels, n_folds, seed=42):
    """
    Create local stratified folds.

    Each node creates its own fold assignment while preserving the
    cohort proportions locally.

    This means that fold IDs are not shared across hospitals. This is
    intentional because patient-level identities remain local.
    """

    labels = np.asarray(labels)

    if n_folds < 2:
        return np.zeros(len(labels), dtype=int)

    fold_ids = np.empty(len(labels), dtype=int)

    rng = np.random.default_rng(seed)

    for class_label in np.unique(labels):

        class_indices = np.where(labels == class_label)[0]

        shuffled = class_indices.copy()
        rng.shuffle(shuffled)

        for position, index in enumerate(shuffled):
            fold_ids[index] = position % n_folds

    return fold_ids


def _training_mask(fold_ids, fold):
    """
    Training rows for a fold. ``fold=None`` means all rows.
    """

    if fold is None:
        return np.ones(len(fold_ids), dtype=bool)

    return fold_ids != fold


def _validation_mask(fold_ids, fold):
    """
    Validation rows for a fold. ``fold=None`` means all rows (used to
    evaluate a held-out hospital or the final model).
    """

    if fold is None:
        return np.ones(len(fold_ids), dtype=bool)

    return fold_ids == fold


def _standardize(X, mean, std):

    mean = np.asarray(mean, dtype=float)
    std = np.asarray(std, dtype=float)

    std_safe = np.where(std < 1e-12, 1.0, std)

    return (X - mean) / std_safe


# ---------------------------------------------------------------------
# COHORT DETECTION
# ---------------------------------------------------------------------

@federated
@dataframe(1)
def partial_cohorts(
    df1,
    features,
    outcome_column,
    sample_column="sample_label",
    value_column="beta",
):
    """
    Return the cohort labels present at this node and the number of
    complete samples (all CpGs observed) in each.

    Used by the central function to detect how many cohorts are present
    and therefore which pipeline (binary or one-vs-rest) to run.
    """

    frame = _local_frame(
        df1,
        features,
        outcome_column,
        sample_column,
        value_column,
    )

    if isinstance(frame, dict):
        return {**frame, "organization_id": _organization_id()}

    sub = frame.dropna()

    labels, counts = np.unique(
        sub[outcome_column].to_numpy(),
        return_counts=True,
    )

    return {
        # A list of pairs (not a dict) so that non-string labels keep
        # their type after JSON serialization.
        "cohorts": [
            [label.item() if hasattr(label, "item") else label, int(count)]
            for label, count in zip(labels, counts)
        ],
    }


# ---------------------------------------------------------------------
# SUMMARY STATISTICS
# ---------------------------------------------------------------------

@federated
@dataframe(1)
def partial_summary(
    df1,
    features,
    outcome_column,
    classes,
    n_folds=5,
    random_seed=42,
    min_samples=5,
    sample_column="sample_label",
    value_column="beta",
):
    """
    Return the local aggregates needed before training.

    For the full data and for the training partition of every fold:
    n, sum(X), sum(X^2) (federated standardization) and cohort counts
    (federated class weights).

    For the full data, per-cohort sum(X) and sum(X^2) are also returned
    (cohort methylation profiles). Cohorts with fewer than ``min_samples``
    samples at this node are suppressed.
    """

    prepared = _prepare(
        df1,
        features,
        outcome_column,
        classes,
        min_samples,
        sample_column,
        value_column,
    )

    if isinstance(prepared, dict):
        return {**prepared, "organization_id": _organization_id()}

    X, labels = prepared

    fold_ids = _make_stratified_folds(
        labels,
        n_folds,
        random_seed,
    )

    partitions = {}

    partition_keys = [("all", None)] + [
        (str(fold), fold)
        for fold in range(n_folds)
    ]

    for key, fold in partition_keys:

        mask = _training_mask(fold_ids, fold)

        if mask.sum() < min_samples:
            continue

        X_part = X[mask]
        labels_part = labels[mask]

        partitions[key] = {
            "n": int(mask.sum()),
            "sum": X_part.sum(axis=0).tolist(),
            "sumsq": (X_part ** 2).sum(axis=0).tolist(),
            "class_counts": [
                int(np.sum(labels_part == class_label))
                for class_label in classes
            ],
        }

    cohorts = {}

    for class_label in classes:

        mask = labels == class_label
        n_class = int(mask.sum())

        if n_class < min_samples:
            cohorts[str(class_label)] = {
                "suppressed": True,
            }
            continue

        cohorts[str(class_label)] = {
            "n": n_class,
            "sum": X[mask].sum(axis=0).tolist(),
            "sumsq": (X[mask] ** 2).sum(axis=0).tolist(),
        }

    return {
        "organization_id": _organization_id(),
        "partitions": partitions,
        "cohorts": cohorts,
    }


# ---------------------------------------------------------------------
# LOCAL SVM STATISTICS (FEDERATED NEWTON)
# ---------------------------------------------------------------------

@federated
@dataframe(1)
def partial_svm(
    df1,
    features,
    outcome_column,
    classes,
    jobs,
    n_folds=5,
    random_seed=42,
    min_samples=5,
    sample_column="sample_label",
    value_column="beta",
):
    """
    Compute the local contribution to the L2-regularized squared-hinge
    SVM objective for a batch of models.

    The global objective for one model is

        F(w, b) = 0.5 * ||w||^2
                  + C * sum_i s_i * max(0, 1 - y_i (w.x_i + b))^2

    summed over the samples of all hospitals. Each node returns, for its
    own training samples, the loss sum and its gradient and (generalized)
    Hessian with respect to (w, b). The central function adds the
    regularization term and C, and takes a Newton step.

    Each job contains: ``id``, ``organizations`` (optional: the hospitals
    that train it), ``fold`` (None = all data), ``target``
    (cohort coded as +1), ``mean``/``std`` (global standardization),
    ``positive_weight``/``negative_weight`` (global class weights) and the
    current parameters ``w``/``b``.

    No patient-level data leaves the node.
    """

    prepared = _prepare(
        df1,
        features,
        outcome_column,
        classes,
        min_samples,
        sample_column,
        value_column,
    )

    if isinstance(prepared, dict):
        return {**prepared, "organization_id": _organization_id()}

    X, labels = prepared

    fold_ids = _make_stratified_folds(
        labels,
        n_folds,
        random_seed,
    )

    organization_id = _organization_id()

    results = {}

    for job in jobs:

        if not _job_applies(job, organization_id):
            continue

        mask = _training_mask(fold_ids, job["fold"])

        if mask.sum() < min_samples:
            results[job["id"]] = {
                "error": "insufficient training samples"
            }
            continue

        Xs = _standardize(X[mask], job["mean"], job["std"])

        y = np.where(
            labels[mask] == job["target"],
            1.0,
            -1.0,
        )

        sample_weights = np.where(
            y > 0,
            float(job["positive_weight"]),
            float(job["negative_weight"]),
        )

        w = np.asarray(job["w"], dtype=float)
        b = float(job["b"])

        slack = np.maximum(
            0.0,
            1.0 - y * (Xs @ w + b),
        )

        active = slack > 0.0

        # Augment with an intercept column so gradient and Hessian are
        # with respect to (w, b).
        X_active = np.hstack(
            [
                Xs[active],
                np.ones((int(active.sum()), 1)),
            ]
        )

        weighted_slack = (
            sample_weights[active]
            * slack[active]
            * y[active]
        )

        gradient = -2.0 * X_active.T @ weighted_slack

        hessian = 2.0 * (
            X_active * sample_weights[active][:, np.newaxis]
        ).T @ X_active

        results[job["id"]] = {
            "n": int(mask.sum()),
            "loss": float(np.sum(sample_weights * slack ** 2)),
            "gradient": gradient.tolist(),
            "hessian": hessian.tolist(),
        }

    return {
        "jobs": results,
    }


# ---------------------------------------------------------------------
# LOCAL VALIDATION
# ---------------------------------------------------------------------

@federated
@dataframe(1)
def partial_evaluate(
    df1,
    features,
    outcome_column,
    classes,
    jobs,
    n_folds=5,
    random_seed=42,
    min_samples=5,
    score_bins=None,
    sample_column="sample_label",
    value_column="beta",
):
    """
    Evaluate models locally and return only pooled aggregates.

    Each job contains ``id``, ``organizations`` (optional: the hospitals
    that evaluate it) and a list of ``models``. Every model has a
    ``fold`` (None = all samples at this node), ``weights`` (one row per
    comparison), ``biases``, ``mean`` and ``std``. For cross-validation a
    job contains one model per fold, each applied to its own validation
    fold, and the results are pooled over the folds. Individual
    predictions never leave the node.

    Returned per job:

    - ``confusion_matrix``: cohorts x cohorts (rows = true cohort).
    - ``comparison_confusion``: per comparison a 2x2 matrix
      [[target->target, target->rest], [rest->target, rest->rest]] based
      on the sign of that comparison's decision score.
    - ``score_histograms`` (if ``score_bins`` is given): per comparison and
      per true cohort, counts of decision scores in the given bins.

    A job is only reported when it pools at least ``min_samples`` samples.
    """

    prepared = _prepare(
        df1,
        features,
        outcome_column,
        classes,
        min_samples,
        sample_column,
        value_column,
    )

    if isinstance(prepared, dict):
        return {**prepared, "organization_id": _organization_id()}

    X, labels = prepared

    fold_ids = _make_stratified_folds(
        labels,
        n_folds,
        random_seed,
    )

    class_to_index = {
        class_label: index
        for index, class_label in enumerate(classes)
    }

    binary = len(classes) == 2

    targets = [classes[1]] if binary else list(classes)

    if score_bins is not None:
        bin_edges = np.asarray(score_bins, dtype=float)

    organization_id = _organization_id()

    results = {}

    for job in jobs:

        if not _job_applies(job, organization_id):
            continue

        confusion = np.zeros((len(classes), len(classes)), dtype=int)

        comparison_confusion = np.zeros(
            (len(targets), 2, 2),
            dtype=int,
        )

        histograms = None

        if score_bins is not None:
            histograms = np.zeros(
                (len(targets), len(classes), len(bin_edges) - 1),
                dtype=int,
            )

        n_evaluated = 0

        for model in job["models"]:

            mask = _validation_mask(fold_ids, model["fold"])

            if not mask.any():
                continue

            Xs = _standardize(X[mask], model["mean"], model["std"])
            true_labels = labels[mask]

            W = np.asarray(model["weights"], dtype=float)
            B = np.asarray(model["biases"], dtype=float)

            scores = Xs @ W.T + B

            if binary:
                # One hyperplane: positive score -> classes[1].
                predicted = np.where(
                    scores[:, 0] >= 0.0,
                    classes[1],
                    classes[0],
                )
            else:
                predicted = np.asarray(classes)[
                    np.argmax(scores, axis=1)
                ]

            for true_label, predicted_label in zip(true_labels, predicted):
                confusion[
                    class_to_index[true_label],
                    class_to_index[predicted_label],
                ] += 1

            for model_index, target in enumerate(targets):

                is_target = true_labels == target
                predicted_target = scores[:, model_index] >= 0.0

                comparison_confusion[model_index] += np.array(
                    [
                        [
                            np.sum(is_target & predicted_target),
                            np.sum(is_target & ~predicted_target),
                        ],
                        [
                            np.sum(~is_target & predicted_target),
                            np.sum(~is_target & ~predicted_target),
                        ],
                    ]
                )

                if histograms is not None:

                    clipped = np.clip(
                        scores[:, model_index],
                        bin_edges[0],
                        bin_edges[-1],
                    )

                    for class_label, class_index in class_to_index.items():
                        histograms[model_index, class_index] += np.histogram(
                            clipped[true_labels == class_label],
                            bins=bin_edges,
                        )[0]

            n_evaluated += int(mask.sum())

        if n_evaluated < min_samples:
            results[job["id"]] = {
                "error": f"fewer than {min_samples} evaluated samples"
            }
            continue

        result = {
            "n": n_evaluated,
            "confusion_matrix": confusion.tolist(),
            "comparison_confusion": comparison_confusion.tolist(),
        }

        if histograms is not None:
            result["score_histograms"] = histograms.tolist()

        results[job["id"]] = result

    return {
        "jobs": results,
    }
