"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).

Central coordinator for the federated Linear SVM: discovery and
characterization of cohort-specific DNA methylation patterns.

The module answers: "Do these predefined cohorts show a discriminative
methylation pattern, and which CpGs characterize it?" It reports evidence; it
does not decide whether a pattern is an episignature (that characterization
and validation happens downstream).

Responsibilities:

1. Obtain federated standardization parameters and class counts.
2. Train SVMs for each candidate C with federated Newton optimization of the
   exact (centralized) squared-hinge SVM objective.
3. Perform federated cross-validation and select C by balanced accuracy.
4. Train the final model on all available data (the discovered pattern).
5. Assess the pattern: pooled cross-validation, leave-one-hospital-out (LOHO)
   generalization and stability of the CpG ranking.
6. Report one comparison per cohort contrast (binary: one hyperplane read in
   two directions; multiclass: one-vs-rest), each with its ranked CpGs,
   methylation directions and evidence.
7. Return structured output for downstream visualization.

The central node never receives patient-level methylation data.
"""

import numpy as np

from vantage6.algorithm.client import AlgorithmClient
from vantage6.algorithm.decorator.action import central as central_step
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.tools.util import info, warn


DEFAULT_C_GRID = [0.001, 0.01, 0.1, 1.0, 10.0, 100.0]

# Edges of the decision-score histograms. Scores outside the range are
# counted in the outermost bins.
SCORE_BINS = np.linspace(-3.0, 3.0, 25).tolist()

# Upper bound on the number of Hessian entries requested in one task.
MAX_PAYLOAD_NUMBERS = 2_000_000

# Newton optimization constants.
HESSIAN_RIDGE = 1e-8
ARMIJO_FRACTION = 1e-4
MIN_STEP = 1e-10
MAX_LINE_SEARCH_ROUNDS_PER_ITERATION = 5

LOHO_SCOPE = (
    "LOHO excludes the held-out hospital from SVM standardization, class "
    "weighting, C selection and training. It is conditional on the supplied "
    "CpGs: if the CpGs were selected (e.g. by Elastic Net) using data from "
    "the held-out hospital, this does not validate the complete "
    "selection + SVM pipeline."
)


# ---------------------------------------------------------------------
# TASK HELPER
# ---------------------------------------------------------------------

def _run_task(client, method, org_ids, arguments, name):
    """
    Create a subtask, wait for it and return the results without errors.
    """

    task = client.task.create(
        method=method,
        arguments=arguments,
        organizations=org_ids,
        name=name,
    )

    results = []

    for result in client.wait_for_results(task_id=task.get("id")):

        if isinstance(result, dict) and "error" in result:
            warn(f"{method}: {result['error']}")
            continue

        results.append(result)

    return results


# ---------------------------------------------------------------------
# COHORT DETECTION
# ---------------------------------------------------------------------

def _detect_cohorts(client, org_ids, features, outcome_column, classes,
                    reference_cohort, min_samples):
    """
    Detect the cohort labels present across all hospitals and decide which
    cohorts enter the analysis.

    If ``classes`` is given, only those cohorts are used. Otherwise every
    detected cohort with at least ``min_samples`` complete samples in
    total is used. Two cohorts give the binary pipeline, more give the
    one-vs-rest pipeline.

    With a ``reference_cohort`` and two cohorts, the reference is placed
    first so that the hyperplane is oriented towards the other cohort.
    """

    results = _run_task(
        client,
        "partial_cohorts",
        org_ids,
        {
            "features": features,
            "outcome_column": outcome_column,
        },
        "SVM cohort detection",
    )

    detected = {}

    for result in results:
        for label, count in result["cohorts"]:
            detected[label] = detected.get(label, 0) + count

    # Sort for a deterministic order. With two cohorts, the second one is
    # coded as +1 (the hyperplane is reported in both directions anyway).
    detected = dict(sorted(detected.items(), key=lambda item: str(item[0])))

    excluded = {}

    if classes is not None:

        used = list(classes)

        for label, count in detected.items():
            if label not in used:
                excluded[str(label)] = "not requested in classes"

        not_found = [
            label
            for label in used
            if label not in detected
        ]

        if not_found:
            return {
                "error": f"Requested cohorts not found in the data: {not_found}"
            }

    else:

        used = []

        for label, count in detected.items():
            if count >= min_samples:
                used.append(label)
            else:
                excluded[str(label)] = (
                    f"only {count} complete samples (< min_samples)"
                )

    if len(used) < 2:
        return {
            "error": (
                "At least two cohorts with enough samples are required; "
                f"detected: { {str(k): v for k, v in detected.items()} }"
            )
        }

    if reference_cohort is not None:

        if reference_cohort not in used:
            return {
                "error": (
                    f"Reference cohort {reference_cohort!r} is not among the "
                    f"analysed cohorts {used}."
                )
            }

        if len(used) == 2:
            used = [reference_cohort] + [
                label
                for label in used
                if label != reference_cohort
            ]

    return {
        "detected": {
            str(label): count
            for label, count in detected.items()
        },
        "classes": used,
        "reference_cohort": reference_cohort,
        "excluded": excluded,
        "pipeline": (
            "binary_single_hyperplane"
            if len(used) == 2
            else "one_vs_rest"
        ),
    }


# ---------------------------------------------------------------------
# FEDERATED SUMMARY STATISTICS
# ---------------------------------------------------------------------

def _get_summary(client, org_ids, base_args):
    """
    Aggregate per-partition moments and class counts, and per-cohort
    methylation profiles.
    """

    results = _run_task(
        client,
        "partial_summary",
        org_ids,
        base_args,
        "SVM summary statistics",
    )

    if not results:
        return None

    classes = base_args["classes"]
    n_features = len(base_args["features"])

    partitions = {}

    for result in results:
        for key, part in result["partitions"].items():

            aggregate = partitions.setdefault(
                key,
                {
                    "n": 0,
                    "sum": np.zeros(n_features),
                    "sumsq": np.zeros(n_features),
                    "class_counts": np.zeros(len(classes), dtype=int),
                },
            )

            aggregate["n"] += part["n"]
            aggregate["sum"] += np.asarray(part["sum"], dtype=float)
            aggregate["sumsq"] += np.asarray(part["sumsq"], dtype=float)
            aggregate["class_counts"] += np.asarray(
                part["class_counts"],
                dtype=int,
            )

    for aggregate in partitions.values():

        mean = aggregate["sum"] / aggregate["n"]

        variance = np.maximum(
            aggregate["sumsq"] / aggregate["n"] - mean ** 2,
            0.0,
        )

        aggregate["mean"] = mean
        aggregate["std"] = np.sqrt(variance)

    cohorts = {}

    for class_label in classes:

        key = str(class_label)

        n = 0
        total = np.zeros(n_features)
        total_sq = np.zeros(n_features)
        n_suppressed_nodes = 0

        for result in results:

            cohort = result["cohorts"].get(key, {"suppressed": True})

            if cohort.get("suppressed"):
                n_suppressed_nodes += 1
                continue

            n += cohort["n"]
            total += np.asarray(cohort["sum"], dtype=float)
            total_sq += np.asarray(cohort["sumsq"], dtype=float)

        if n > 0:
            mean = total / n
            sd = np.sqrt(np.maximum(total_sq / n - mean ** 2, 0.0))
        else:
            mean = None
            sd = None

        cohorts[key] = {
            "n": int(n),
            "mean": mean,
            "sd": sd,
            "n_nodes_suppressed": n_suppressed_nodes,
        }

    return {
        "partitions": partitions,
        "cohorts": cohorts,
        "n_nodes": len(results),
    }


def _targets(classes):
    """
    Cohort coded as +1 in each comparison.

    Binary problems use ONE hyperplane (classes[1] vs classes[0]); the two
    cohort-specific signatures are the two directions of that hyperplane.
    Multiclass problems use one-vs-rest.
    """

    if len(classes) == 2:
        return [classes[1]]

    return list(classes)


def _class_weights(class_counts, classes, targets):
    """
    Globally balanced class weights for each comparison:

        positive weight = N / (2 * N_positive)
        negative weight = N / (2 * N_negative)
    """

    class_counts = np.asarray(class_counts, dtype=float)
    total = class_counts.sum()

    positive_weights = []
    negative_weights = []

    for target in targets:

        n_positive = class_counts[classes.index(target)]
        n_negative = total - n_positive

        positive_weights.append(
            float(total / (2.0 * n_positive)) if n_positive > 0 else 1.0
        )
        negative_weights.append(
            float(total / (2.0 * n_negative)) if n_negative > 0 else 1.0
        )

    return positive_weights, negative_weights


# ---------------------------------------------------------------------
# FEDERATED NEWTON OPTIMIZATION
# ---------------------------------------------------------------------

def _newton_update(state, statistics, n_features, max_rounds, tolerance):
    """
    Process the aggregated statistics for one model at its trial point and
    decide the next trial point (Newton step with Armijo backtracking).
    """

    spec = state["spec"]
    C = spec["C"]

    if statistics is None or statistics["n"] == 0:
        state["done"] = True
        state["status"] = "no_training_data"
        return

    trial = state["trial"]
    w = trial[:-1]

    objective = 0.5 * float(w @ w) + C * statistics["loss"]

    gradient = C * statistics["gradient"]
    gradient[:-1] += w

    hessian = C * statistics["hessian"]
    hessian[np.diag_indices(n_features)] += 1.0
    hessian[-1, -1] += HESSIAN_RIDGE

    if state["theta"] is not None:

        expected_decrease = (
            ARMIJO_FRACTION
            * state["step"]
            * float(state["gradient"] @ state["direction"])
        )

        if objective > state["objective"] + expected_decrease:

            state["step"] *= 0.5

            if state["step"] < MIN_STEP:
                # No further decrease is numerically possible. This only
                # counts as convergence when we are already close to the
                # optimum.
                close = (
                    np.linalg.norm(state["gradient"])
                    <= 1e-3 * max(1.0, state["initial_gradient_norm"])
                )
                state["done"] = True
                state["converged"] = bool(close)
                state["status"] = (
                    "converged" if close else "line_search_failed"
                )
                return

            state["trial"] = (
                state["theta"] + state["step"] * state["direction"]
            )
            return

        state["iterations"] += 1

    state["theta"] = trial
    state["objective"] = objective
    state["gradient"] = gradient
    state["hessian"] = hessian

    gradient_norm = float(np.linalg.norm(gradient))

    if state["initial_gradient_norm"] is None:
        state["initial_gradient_norm"] = gradient_norm

    if gradient_norm <= tolerance * max(1.0, state["initial_gradient_norm"]):
        state["done"] = True
        state["converged"] = True
        state["status"] = "converged"
        return

    if state["iterations"] >= max_rounds:
        state["done"] = True
        state["status"] = "max_rounds_reached"
        return

    state["direction"] = -np.linalg.solve(hessian, gradient)
    state["step"] = 1.0
    state["trial"] = state["theta"] + state["direction"]


def _fit_models(client, org_ids, base_args, specs, max_rounds, tolerance,
                label):
    """
    Fit several SVM models simultaneously with federated Newton steps.

    Every federated round evaluates all still-active models in one task, so
    the number of tasks depends on the number of Newton iterations, not on
    the number of models (C values x folds x comparisons).
    """

    n_features = len(base_args["features"])
    dimension = n_features + 1

    states = {}

    for spec in specs:
        states[spec["id"]] = {
            "spec": spec,
            "theta": None,
            "trial": np.zeros(dimension),
            "direction": None,
            "step": 1.0,
            "objective": None,
            "gradient": None,
            "hessian": None,
            "initial_gradient_norm": None,
            "iterations": 0,
            "done": False,
            "converged": False,
            "status": "running",
        }

    batch_size = max(1, MAX_PAYLOAD_NUMBERS // (dimension * dimension))

    max_total_rounds = (
        max_rounds * (1 + MAX_LINE_SEARCH_ROUNDS_PER_ITERATION) + 1
    )

    n_rounds = 0

    while n_rounds < max_total_rounds:

        active = [
            state
            for state in states.values()
            if not state["done"]
        ]

        if not active:
            break

        n_rounds += 1

        statistics = {}

        for start in range(0, len(active), batch_size):

            batch = active[start:start + batch_size]

            jobs = [
                {
                    "id": state["spec"]["id"],
                    "fold": state["spec"]["fold"],
                    "target": state["spec"]["target"],
                    "mean": state["spec"]["mean"],
                    "std": state["spec"]["std"],
                    "positive_weight": state["spec"]["positive_weight"],
                    "negative_weight": state["spec"]["negative_weight"],
                    "w": state["trial"][:-1].tolist(),
                    "b": float(state["trial"][-1]),
                }
                for state in batch
            ]

            results = _run_task(
                client,
                "partial_svm",
                org_ids,
                {**base_args, "jobs": jobs},
                f"SVM Newton round {n_rounds} ({label})",
            )

            for result in results:
                for job_id, job_result in result["jobs"].items():

                    if "error" in job_result:
                        continue

                    aggregate = statistics.setdefault(
                        job_id,
                        {
                            "n": 0,
                            "loss": 0.0,
                            "gradient": np.zeros(dimension),
                            "hessian": np.zeros((dimension, dimension)),
                        },
                    )

                    aggregate["n"] += job_result["n"]
                    aggregate["loss"] += job_result["loss"]
                    aggregate["gradient"] += np.asarray(
                        job_result["gradient"],
                        dtype=float,
                    )
                    aggregate["hessian"] += np.asarray(
                        job_result["hessian"],
                        dtype=float,
                    )

        for state in active:
            _newton_update(
                state,
                statistics.get(state["spec"]["id"]),
                n_features,
                max_rounds,
                tolerance,
            )

    fits = {}

    for model_id, state in states.items():

        if not state["done"]:
            state["status"] = "max_rounds_reached"

        if state["theta"] is None:
            fits[model_id] = None
            continue

        fits[model_id] = {
            "w": state["theta"][:-1].tolist(),
            "b": float(state["theta"][-1]),
            "objective": float(state["objective"]),
            "gradient_norm": float(np.linalg.norm(state["gradient"])),
            "iterations": state["iterations"],
            "converged": state["converged"],
            "status": state["status"],
        }

    info(f"{label}: {len(specs)} model(s) fitted in {n_rounds} round(s)")

    return fits


# ---------------------------------------------------------------------
# FEDERATED EVALUATION
# ---------------------------------------------------------------------

def _evaluate(client, org_ids, base_args, jobs, label, score_bins=None):
    """
    Evaluate models at the nodes and aggregate the pooled results.
    """

    n_classes = len(base_args["classes"])
    n_targets = len(_targets(base_args["classes"]))

    arguments = {**base_args, "jobs": jobs}

    if score_bins is not None:
        arguments["score_bins"] = score_bins

    results = _run_task(
        client,
        "partial_evaluate",
        org_ids,
        arguments,
        f"SVM evaluation ({label})",
    )

    aggregates = {}

    for result in results:
        for job_id, job_result in result["jobs"].items():

            if "error" in job_result:
                continue

            aggregate = aggregates.setdefault(
                job_id,
                {
                    "n": 0,
                    "confusion_matrix": np.zeros(
                        (n_classes, n_classes),
                        dtype=int,
                    ),
                    "comparison_confusion": np.zeros(
                        (n_targets, 2, 2),
                        dtype=int,
                    ),
                    "score_histograms": (
                        np.zeros(
                            (n_targets, n_classes, len(score_bins) - 1),
                            dtype=int,
                        )
                        if score_bins is not None
                        else None
                    ),
                },
            )

            aggregate["n"] += job_result["n"]
            aggregate["confusion_matrix"] += np.asarray(
                job_result["confusion_matrix"],
                dtype=int,
            )
            aggregate["comparison_confusion"] += np.asarray(
                job_result["comparison_confusion"],
                dtype=int,
            )

            if score_bins is not None:
                aggregate["score_histograms"] += np.asarray(
                    job_result["score_histograms"],
                    dtype=int,
                )

    return aggregates


# ---------------------------------------------------------------------
# METRICS
# ---------------------------------------------------------------------

def _calculate_metrics(confusion):
    """
    Multiclass metrics from a confusion matrix (rows = true cohort).

    Cohorts without samples are left out of the macro averages instead of
    counting as zero sensitivity (relevant for held-out hospitals that do
    not contain every cohort).
    """

    confusion = np.asarray(confusion, dtype=float)

    total = confusion.sum()

    if total == 0:
        return None

    true_positives = np.diag(confusion)
    support = confusion.sum(axis=1)
    predicted = confusion.sum(axis=0)
    negatives = total - support
    true_negatives = total - support - predicted + true_positives

    present = support > 0
    has_negatives = negatives > 0

    sensitivity = true_positives[present] / support[present]

    specificity = (
        true_negatives[has_negatives] / negatives[has_negatives]
    )

    precision = np.where(
        predicted > 0,
        true_positives / np.maximum(predicted, 1.0),
        0.0,
    )[present]

    f1 = np.where(
        precision + sensitivity > 0,
        2.0 * precision * sensitivity
        / np.maximum(precision + sensitivity, 1e-12),
        0.0,
    )

    return {
        "n": int(total),
        "accuracy": float(true_positives.sum() / total),
        "balanced_accuracy": float(np.mean(sensitivity)),
        "macro_f1": float(np.mean(f1)),
        "macro_sensitivity": float(np.mean(sensitivity)),
        "macro_specificity": (
            float(np.mean(specificity)) if specificity.size else None
        ),
        "macro_precision": float(np.mean(precision)),
    }


def _comparison_metrics(confusion):
    """
    Metrics for one comparison from its 2x2 matrix
    [[target->target, target->rest], [rest->target, rest->rest]].

    Sensitivity refers to the target cohort, specificity to the reference
    cohort(s). Undefined values are None.
    """

    confusion = np.asarray(confusion, dtype=float)

    (target_target, target_rest), (rest_target, rest_rest) = confusion

    sensitivity = (
        target_target / (target_target + target_rest)
        if target_target + target_rest > 0
        else None
    )

    specificity = (
        rest_rest / (rest_target + rest_rest)
        if rest_target + rest_rest > 0
        else None
    )

    precision = (
        target_target / (target_target + rest_target)
        if target_target + rest_target > 0
        else None
    )

    available = [
        value
        for value in (sensitivity, specificity)
        if value is not None
    ]

    if precision is not None and sensitivity is not None:
        f1 = (
            2.0 * precision * sensitivity / (precision + sensitivity)
            if precision + sensitivity > 0
            else 0.0
        )
    else:
        f1 = None

    return {
        "n": int(confusion.sum()),
        "balanced_accuracy": (
            float(np.mean(available)) if available else None
        ),
        "sensitivity": _optional_float(sensitivity),
        "specificity": _optional_float(specificity),
        "precision": _optional_float(precision),
        "f1": _optional_float(f1),
        "confusion_matrix": confusion.astype(int).tolist(),
    }


def _optional_float(value):

    return None if value is None else float(value)


# ---------------------------------------------------------------------
# DISCOVERY: CROSS-VALIDATION AND FINAL MODEL
# ---------------------------------------------------------------------

def _cross_validate(client, org_ids, base_args, summary, settings, label):
    """
    Pooled federated cross-validation over the C grid.

    All C values, folds and comparisons are optimized together. Each node
    evaluates every fold model on its own validation fold and returns the
    confusion matrices pooled over its folds.
    """

    classes = base_args["classes"]
    targets = _targets(classes)
    C_grid = settings["C_grid"]

    folds = [
        fold
        for fold in range(base_args["n_folds"])
        if str(fold) in summary["partitions"]
    ]

    if not folds:
        return None

    specs = []

    for c_index, C in enumerate(C_grid):
        for fold in folds:

            partition = summary["partitions"][str(fold)]

            positive_weights, negative_weights = _class_weights(
                partition["class_counts"],
                classes,
                targets,
            )

            for model_index, target in enumerate(targets):
                specs.append(
                    {
                        "id": f"c{c_index}_f{fold}_m{model_index}",
                        "C": float(C),
                        "fold": fold,
                        "target": target,
                        "mean": partition["mean"].tolist(),
                        "std": partition["std"].tolist(),
                        "positive_weight": positive_weights[model_index],
                        "negative_weight": negative_weights[model_index],
                    }
                )

    fits = _fit_models(
        client,
        org_ids,
        base_args,
        specs,
        settings["max_rounds"],
        settings["tolerance"],
        f"{label}: cross-validation",
    )

    jobs = []
    fold_models = {}

    for c_index in range(len(C_grid)):

        models = []

        for fold in folds:

            ids = [
                f"c{c_index}_f{fold}_m{model_index}"
                for model_index in range(len(targets))
            ]

            if any(fits[model_id] is None for model_id in ids):
                continue

            partition = summary["partitions"][str(fold)]

            models.append(
                {
                    "fold": fold,
                    "weights": [fits[model_id]["w"] for model_id in ids],
                    "biases": [fits[model_id]["b"] for model_id in ids],
                    "mean": partition["mean"].tolist(),
                    "std": partition["std"].tolist(),
                }
            )

        if models:
            jobs.append({"id": f"c{c_index}", "models": models})
            fold_models[c_index] = models

    evaluation = _evaluate(
        client,
        org_ids,
        base_args,
        jobs,
        f"{label}: cross-validation",
    )

    candidates = []

    for c_index, C in enumerate(C_grid):

        result = evaluation.get(f"c{c_index}")

        if result is None:
            continue

        model_fits = [
            fit
            for model_id, fit in fits.items()
            if model_id.startswith(f"c{c_index}_") and fit is not None
        ]

        candidates.append(
            {
                "C": float(C),
                "c_index": c_index,
                "metrics": _calculate_metrics(result["confusion_matrix"]),
                "confusion_matrix": result["confusion_matrix"].tolist(),
                "comparisons": [
                    _comparison_metrics(matrix)
                    for matrix in result["comparison_confusion"]
                ],
                "n_folds_evaluated": len(fold_models[c_index]),
                "n_models": len(model_fits),
                "n_models_converged": sum(
                    fit["converged"] for fit in model_fits
                ),
            }
        )

    candidates = [
        candidate
        for candidate in candidates
        if candidate["metrics"] is not None
    ]

    if not candidates:
        return None

    # Highest balanced accuracy; ties go to the smaller C (stronger
    # regularization).
    best_score = max(
        candidate["metrics"]["balanced_accuracy"]
        for candidate in candidates
    )

    best = min(
        (
            candidate
            for candidate in candidates
            if candidate["metrics"]["balanced_accuracy"] >= best_score - 1e-12
        ),
        key=lambda candidate: candidate["C"],
    )

    return {
        "candidates": candidates,
        "best": best,
        "best_fold_models": fold_models[best["c_index"]],
    }


def _fit_final(client, org_ids, base_args, summary, C, settings, label):
    """
    Train the final model(s) on all data of the given hospitals.
    """

    classes = base_args["classes"]
    targets = _targets(classes)

    partition = summary["partitions"].get("all")

    if partition is None:
        return None

    positive_weights, negative_weights = _class_weights(
        partition["class_counts"],
        classes,
        targets,
    )

    specs = [
        {
            "id": f"final_m{model_index}",
            "C": float(C),
            "fold": None,
            "target": target,
            "mean": partition["mean"].tolist(),
            "std": partition["std"].tolist(),
            "positive_weight": positive_weights[model_index],
            "negative_weight": negative_weights[model_index],
        }
        for model_index, target in enumerate(targets)
    ]

    fits = _fit_models(
        client,
        org_ids,
        base_args,
        specs,
        settings["max_rounds"],
        settings["tolerance"],
        f"{label}: final model",
    )

    ordered = [fits[spec["id"]] for spec in specs]

    if any(fit is None for fit in ordered):
        return None

    return {
        "C": float(C),
        "weights": [fit["w"] for fit in ordered],
        "biases": [fit["b"] for fit in ordered],
        "fits": ordered,
        "mean": partition["mean"].tolist(),
        "std": partition["std"].tolist(),
        "positive_weights": positive_weights,
        "negative_weights": negative_weights,
    }


def _run_discovery(client, org_ids, base_args, settings, label):
    """
    Summary statistics, cross-validated C selection and final training on
    the given hospitals.
    """

    summary = _get_summary(client, org_ids, base_args)

    if summary is None or "all" not in summary["partitions"]:
        return {"error": "No hospital returned usable summary statistics."}

    cross_validation = _cross_validate(
        client,
        org_ids,
        base_args,
        summary,
        settings,
        label,
    )

    if cross_validation is None:
        return {"error": "No valid SVM candidate could be evaluated."}

    final = _fit_final(
        client,
        org_ids,
        base_args,
        summary,
        cross_validation["best"]["C"],
        settings,
        label,
    )

    if final is None:
        return {"error": "Final federated SVM training failed."}

    return {
        "summary": summary,
        "cross_validation": cross_validation,
        "final": final,
    }


# ---------------------------------------------------------------------
# LEAVE-ONE-HOSPITAL-OUT VALIDATION
# ---------------------------------------------------------------------

def _run_loho(client, org_ids, base_args, settings):
    """
    Strict leave-one-hospital-out validation.

    For every held-out hospital the complete SVM discovery (standardization,
    class weights, C selection and final training) is repeated on the
    remaining hospitals, and the resulting model is evaluated once on the
    held-out hospital. The held-out patients keep their cohort labels.
    """

    classes = base_args["classes"]
    n_targets = len(_targets(classes))

    if len(org_ids) < 2:
        return {
            "available": False,
            "reason": "LOHO requires at least two organizations.",
            "results": [],
        }

    results = []
    final_models = []

    pooled_confusion = np.zeros((len(classes), len(classes)), dtype=int)
    pooled_comparisons = np.zeros((n_targets, 2, 2), dtype=int)
    pooled_histograms = np.zeros(
        (n_targets, len(classes), len(SCORE_BINS) - 1),
        dtype=int,
    )

    for held_out in org_ids:

        train_org_ids = [
            org_id
            for org_id in org_ids
            if org_id != held_out
        ]

        info(f"LOHO: holding out organization {held_out}")

        discovery = _run_discovery(
            client,
            train_org_ids,
            base_args,
            settings,
            f"LOHO without {held_out}",
        )

        if "error" in discovery:
            results.append(
                {
                    "held_out_hospital": held_out,
                    "status": "failed",
                    "reason": discovery["error"],
                }
            )
            continue

        final = discovery["final"]

        evaluation = _evaluate(
            client,
            [held_out],
            base_args,
            [
                {
                    "id": "held_out",
                    "models": [
                        {
                            "fold": None,
                            "weights": final["weights"],
                            "biases": final["biases"],
                            "mean": final["mean"],
                            "std": final["std"],
                        }
                    ],
                }
            ],
            f"LOHO held-out {held_out}",
            score_bins=SCORE_BINS,
        ).get("held_out")

        if evaluation is None:
            results.append(
                {
                    "held_out_hospital": held_out,
                    "status": "failed",
                    "reason": (
                        "Held-out hospital could not be evaluated "
                        "(fewer than min_samples analysable samples)."
                    ),
                }
            )
            continue

        pooled_confusion += evaluation["confusion_matrix"]
        pooled_comparisons += evaluation["comparison_confusion"]
        pooled_histograms += evaluation["score_histograms"]

        final_models.append(final["weights"])

        results.append(
            {
                "held_out_hospital": held_out,
                "training_hospitals": train_org_ids,
                "status": "success",
                "selected_C": final["C"],
                "training_cv_balanced_accuracy": (
                    discovery["cross_validation"]["best"]["metrics"][
                        "balanced_accuracy"
                    ]
                ),
                "n": int(evaluation["n"]),
                "metrics": _calculate_metrics(
                    evaluation["confusion_matrix"]
                ),
                "confusion_matrix": evaluation["confusion_matrix"].tolist(),
                "comparisons": [
                    _comparison_metrics(matrix)
                    for matrix in evaluation["comparison_confusion"]
                ],
            }
        )

    successful = [
        result
        for result in results
        if result["status"] == "success"
    ]

    per_hospital_ba = [
        result["metrics"]["balanced_accuracy"]
        for result in successful
    ]

    note = None

    if len(org_ids) == 2:
        note = (
            "With two hospitals, LOHO trains on one hospital and tests on "
            "the other, in both directions. Each model is selected and "
            "trained on a single hospital, so the two estimates are noisy "
            "and typically pessimistic, but they directly test whether the "
            "pattern transfers between sites."
        )

    return {
        "available": True,
        "scope": LOHO_SCOPE,
        "note": note,
        "n_hospitals": len(org_ids),
        "n_successful": len(successful),
        "mean_balanced_accuracy": (
            float(np.mean(per_hospital_ba)) if per_hospital_ba else None
        ),
        "min_balanced_accuracy": (
            float(np.min(per_hospital_ba)) if per_hospital_ba else None
        ),
        "pooled_metrics": (
            _calculate_metrics(pooled_confusion) if successful else None
        ),
        "pooled_confusion_matrix": pooled_confusion.tolist(),
        "pooled_comparisons": (
            [
                _comparison_metrics(matrix)
                for matrix in pooled_comparisons
            ]
            if successful
            else None
        ),
        "results": results,
        # Internal: used for stability and visualization, removed from the
        # output below.
        "_final_models": final_models,
        "_score_histograms": pooled_histograms,
    }


# ---------------------------------------------------------------------
# STABILITY, SIGNATURE AND DECISION
# ---------------------------------------------------------------------

def _stability(final_coefficients, resampled_coefficients, top_k):
    """
    Stability of one comparison's coefficient vector across the models
    trained on resampled data (CV folds at the selected C and LOHO models).
    """

    if not resampled_coefficients:
        return None

    final = np.asarray(final_coefficients, dtype=float)
    k = min(top_k, final.size)

    final_top = set(np.argsort(-np.abs(final))[:k].tolist())

    correlations = []
    jaccards = []
    top_k_counts = np.zeros(final.size)
    sign_agreement = np.zeros(final.size)

    for coefficients in resampled_coefficients:

        vector = np.asarray(coefficients, dtype=float)

        if np.std(vector) > 0 and np.std(final) > 0:
            correlations.append(float(np.corrcoef(vector, final)[0, 1]))

        top = set(np.argsort(-np.abs(vector))[:k].tolist())

        jaccards.append(len(top & final_top) / len(top | final_top))
        top_k_counts[list(top)] += 1
        sign_agreement += np.sign(vector) == np.sign(final)

    n = len(resampled_coefficients)

    return {
        "n_resampled_models": n,
        "top_k": k,
        "mean_coefficient_correlation": (
            float(np.mean(correlations)) if correlations else None
        ),
        "min_coefficient_correlation": (
            float(np.min(correlations)) if correlations else None
        ),
        "mean_top_k_jaccard": float(np.mean(jaccards)),
        "top_k_frequency": (top_k_counts / n).tolist(),
        "sign_agreement": (sign_agreement / n).tolist(),
    }


def _signature(coefficients, features, target_label, reference_label,
               target_profile, reference_profile, stability):
    """
    Ranked CpG contributions for one comparison.

    Coefficients refer to standardized methylation values. Their sign gives
    the direction of the contribution to the decision score; their
    magnitude is a model-based contribution, not proof of biological
    importance (correlated CpGs share weight under L2 regularization).
    """

    coefficients = np.asarray(coefficients, dtype=float)

    order = np.argsort(-np.abs(coefficients))

    ranking = []

    for rank, feature_index in enumerate(order, start=1):

        coefficient = float(coefficients[feature_index])

        target_mean = (
            float(target_profile["mean"][feature_index])
            if target_profile["mean"] is not None
            else None
        )
        reference_mean = (
            float(reference_profile["mean"][feature_index])
            if reference_profile["mean"] is not None
            else None
        )

        entry = {
            "rank": rank,
            "cpg": features[feature_index],
            "coefficient": coefficient,
            "absolute_coefficient": abs(coefficient),
            "direction": (
                target_label if coefficient > 0
                else reference_label if coefficient < 0
                else "none"
            ),
            "methylation": {
                "mean_target": target_mean,
                "mean_reference": reference_mean,
                "mean_difference": (
                    target_mean - reference_mean
                    if target_mean is not None and reference_mean is not None
                    else None
                ),
            },
        }

        if stability is not None:
            entry["stability"] = {
                "top_k_frequency": stability["top_k_frequency"][
                    feature_index
                ],
                "sign_agreement": stability["sign_agreement"][feature_index],
            }

        ranking.append(entry)

    return ranking


def _reference_profile(cohorts, reference_classes):
    """
    Methylation profile of the reference side of a comparison (the other
    cohort for binary, all other cohorts for one-vs-rest).
    """

    available = [
        cohorts[str(class_label)]
        for class_label in reference_classes
        if cohorts[str(class_label)]["mean"] is not None
    ]

    if not available:
        return {"n": 0, "mean": None}

    n = sum(profile["n"] for profile in available)

    mean = sum(
        profile["n"] * np.asarray(profile["mean"])
        for profile in available
    ) / n

    return {"n": n, "mean": mean}


def _summary(comparisons, n_top=5):
    """
    Human-readable description of each discovered methylation pattern.

    For every comparison the text explains, in this order: what pattern was
    discovered, which CpGs contribute most to it (with their observed
    methylation direction), the quantitative evidence, and what the result
    does not mean. It only reports values already computed by the
    algorithm; it is a description, not a decision.

    Two directions are kept apart on purpose:

    - contribution to the classifier: the sign of the SVM coefficient, i.e.
      which side of the decision score a CpG pushes towards (a multivariate
      property of the model);
    - observed methylation direction: the sign of ``mean_difference``, the
      mean methylation of the target cohort minus the mean of the other
      side (the other cohort, or the other cohorts pooled with sample-size
      weights), on the original methylation scale across all hospitals.
    """

    def value(number):
        return "not available" if number is None else f"{number:.3f}"

    lines_per_comparison = []

    for comparison in comparisons:

        target = str(comparison["target_cohort"])
        others = [str(cohort) for cohort in comparison["reference_cohorts"]]
        binary = len(others) == 1

        if binary:
            other_side = others[0]
            other_side_long = others[0]
        else:
            other_side = "the other cohorts"
            other_side_long = f"the other cohorts ({', '.join(others)})"

        # ---------------------------------------------------------
        # 1. What pattern was discovered
        # ---------------------------------------------------------

        if comparison["is_reference_cohort"]:
            title = (
                f"Discovered pattern: {target} (reference cohort) vs "
                f"{other_side_long}"
            )
        elif binary and comparison["pattern_of"] is None:
            title = f"Discovered pattern: contrast {target} vs {other_side}"
        else:
            title = f"Discovered pattern: {target} vs {other_side_long}"

        n_cpgs = len(comparison["signature"])

        text = [
            title,
            "",
            "The SVM identified a multivariate DNA methylation pattern, "
            f"combining the {n_cpgs} CpGs analysed, that distinguishes "
            f"{target} from {other_side_long}.",
        ]

        if binary and comparison["pattern_of"] is None:
            text.append(
                "No reference cohort was defined: the pattern describes the "
                "contrast between both cohorts and is read in both "
                "directions."
            )
        elif binary:
            text.append(
                f"{other_side} is the reference cohort; one hyperplane "
                "separates the two cohorts."
            )
        elif comparison["is_reference_cohort"]:
            text.append(
                f"{target} is the reference cohort. This one-vs-rest "
                "comparison is part of the multi-cohort model and is "
                "reported for completeness."
            )

        # ---------------------------------------------------------
        # 2-3. Key CpGs and their observed methylation direction
        # ---------------------------------------------------------

        text += [
            "",
            "Key CpGs contributing to the pattern",
            "  Contribution = sign of the SVM coefficient: which side of the "
            "classifier the CpG pushes towards, given all other CpGs.",
            "  Observed methylation = difference in mean methylation (delta "
            f"beta) between {target} and {other_side}, across all "
            "hospitals.",
        ]

        groups = [
            (f"Contributing towards {target}", 1.0),
            (f"Contributing towards {other_side}", -1.0),
        ]

        opposite_signs = False

        for heading, sign in groups:

            entries = [
                entry
                for entry in comparison["signature"]
                if np.sign(entry["coefficient"]) == sign
            ][:n_top]

            if not entries:
                continue

            text += ["", f"  {heading}:"]

            for entry in entries:

                difference = entry["methylation"]["mean_difference"]

                if difference is None:
                    observed = (
                        "observed methylation difference not available "
                        "(cohort summaries suppressed for privacy)"
                    )
                elif round(difference, 3) == 0:
                    observed = (
                        f"practically no difference in mean methylation "
                        f"between {target} and {other_side} "
                        f"(delta beta {difference:+.4f})"
                    )
                else:
                    observed = (
                        f"{'higher' if difference > 0 else 'lower'} mean "
                        f"methylation in {target} than in {other_side} "
                        f"(delta beta {difference:+.3f})"
                    )

                marker = ""

                if (
                    difference is not None
                    and round(difference, 3) != 0
                    and np.sign(difference) != sign
                ):
                    marker = " [*]"
                    opposite_signs = True

                text.append(
                    f"    - {entry['cpg']} (coefficient "
                    f"{entry['coefficient']:+.3f}): {observed}{marker}"
                )

        if opposite_signs:
            text += [
                "",
                "  [*] The contribution to the classifier and the observed "
                "mean difference point in opposite directions. This can "
                "happen in a multivariate model (e.g. with correlated CpGs) "
                "and is reported as observed.",
            ]

        text.append(
            "  The complete ranked list of CpGs is in the structured output."
        )

        # ---------------------------------------------------------
        # 4. Evidence
        # ---------------------------------------------------------

        cv = comparison["cv_metrics"]
        loho = comparison["loho_metrics"]
        stability = comparison["stability"]

        text += [
            "",
            "Evidence supporting the pattern (balanced accuracy 0.5 = "
            "chance level)",
            "  - Cross-validated balanced accuracy: "
            f"{value(cv['balanced_accuracy'])}",
            f"  - Sensitivity ({target} correctly identified): "
            f"{value(cv['sensitivity'])}",
            f"  - Specificity ({other_side} correctly identified): "
            f"{value(cv['specificity'])}",
            "  - LOHO balanced accuracy (hospitals not used for training): "
            f"{value(loho['balanced_accuracy'] if loho else None)}",
            "  - Stability, mean coefficient correlation across folds and "
            "hospitals: "
            f"{value(stability['mean_coefficient_correlation'] if stability else None)}",
            f"  - Stability, top-{stability['top_k'] if stability else 'k'} "
            "CpG overlap (Jaccard): "
            f"{value(stability['mean_top_k_jaccard'] if stability else None)}",
        ]

        # ---------------------------------------------------------
        # 5. What the result means and does not mean
        # ---------------------------------------------------------

        generalization = (
            f" In hospitals not used for training its balanced accuracy "
            f"was {value(loho['balanced_accuracy'])}, compared with "
            f"{value(cv['balanced_accuracy'])} in cross-validation."
            if loho is not None
            else ""
        )

        text += [
            "",
            "Interpretation",
            f"  The SVM discovered a multivariate methylation pattern that "
            f"separates {target} from {other_side} with a cross-validated "
            f"balanced accuracy of {value(cv['balanced_accuracy'])}."
            f"{generalization} The CpGs act jointly: their coefficients "
            "describe their contribution to the discrimination, not their "
            "individual biological importance. This is a discovery and "
            "characterization result; it does not validate an "
            "episignature, which requires downstream assessment by the "
            "researcher.",
        ]

        lines_per_comparison.append("\n".join(text))

    return lines_per_comparison


def _histograms_to_dict(histograms, comparison_names, classes):

    return {
        comparison_names[model_index]: {
            str(class_label): histograms[model_index][class_index].tolist()
            for class_index, class_label in enumerate(classes)
        }
        for model_index in range(len(comparison_names))
    }


# ---------------------------------------------------------------------
# MAIN CENTRAL ACTION
# ---------------------------------------------------------------------

@central_step
@algorithm_client
def central(
    client: AlgorithmClient,
    features,
    outcome_column,
    classes=None,
    reference_cohort=None,
    C_grid=None,
    n_folds=5,
    max_rounds=50,
    tolerance=1e-9,
    random_seed=42,
    min_samples=5,
    run_loho=True,
    stability_top_k=10,
):
    """
    Federated Linear SVM for discovery and characterization of
    cohort-specific DNA methylation patterns.

    The output describes the discriminative pattern (ranked CpGs, direction
    per cohort) and the evidence for it (performance, LOHO generalization,
    stability). It does not decide whether the pattern is an episignature.

    Parameters
    ----------
    features:
        CpGs to analyse (e.g. selected by Elastic Net, or from any previous
        analysis).

    outcome_column:
        Column with the predefined cohort label of each sample.

    classes:
        Optional. Cohorts to compare. By default the cohorts are detected
        automatically from ``outcome_column`` across all hospitals (cohorts
        with fewer than ``min_samples`` complete samples are excluded).
        Two cohorts give one binary hyperplane (classes[1] vs classes[0]);
        more cohorts give one-vs-rest comparisons. Samples from cohorts
        that are not analysed are ignored.

    reference_cohort:
        Optional. The reference group (e.g. controls, "no_rejection").
        With two cohorts, the pattern is reported as the pattern of the
        other cohort relative to the reference. With more cohorts, the
        reference cohort's own one-vs-rest comparison is flagged as such.
        Without a reference, a binary result describes the contrast between
        the two cohorts.

    C_grid:
        Candidate SVM regularization parameters. C has the same meaning as
        in scikit-learn's LinearSVC (squared hinge loss summed over
        samples).

    n_folds:
        Number of local stratified CV folds.

    max_rounds:
        Maximum number of Newton iterations per model.

    tolerance:
        Convergence tolerance on the gradient norm, relative to the
        initial gradient norm.

    min_samples:
        Minimum number of samples a node must pool before returning any
        aggregate.

    run_loho:
        Whether to run leave-one-hospital-out validation.

    stability_top_k:
        Size of the top-ranked CpG set used in the stability analysis.

    Returns
    -------
    dict
        Cohort detection, model, model selection, validation, one entry per
        comparison (ranked CpGs, directions, metrics, stability), a
        plain-language summary of the evidence, and data for visualization.
    """

    if C_grid is None:
        C_grid = DEFAULT_C_GRID

    features = list(features)
    C_grid = sorted(float(C) for C in C_grid)

    if classes is not None:

        classes = list(classes)

        if len(classes) < 2 or len(set(classes)) != len(classes):
            return {
                "error": "At least two distinct classes are required."
            }

    if n_folds < 2:
        return {
            "error": "n_folds must be at least 2."
        }

    org_ids = [
        organization["id"]
        for organization in client.organization.list()
    ]

    if not org_ids:
        return {
            "error": "No organizations available."
        }

    info(
        f"Starting federated SVM across "
        f"{len(org_ids)} organizations."
    )

    # =============================================================
    # COHORT DETECTION AND PIPELINE SELECTION
    # =============================================================

    cohort_detection = _detect_cohorts(
        client,
        org_ids,
        features,
        outcome_column,
        classes,
        reference_cohort,
        min_samples,
    )

    if "error" in cohort_detection:
        return cohort_detection

    classes = cohort_detection["classes"]

    info(
        f"Cohorts: {classes} -> {cohort_detection['pipeline']} pipeline"
    )

    base_args = {
        "features": features,
        "outcome_column": outcome_column,
        "classes": classes,
        "n_folds": n_folds,
        "random_seed": random_seed,
        "min_samples": min_samples,
    }

    settings = {
        "C_grid": C_grid,
        "max_rounds": max_rounds,
        "tolerance": tolerance,
    }

    targets = _targets(classes)

    if len(classes) == 2:
        comparison_names = [f"{classes[1]} vs {classes[0]}"]
        reference_labels = [str(classes[0])]
        reference_classes = [[classes[0]]]
    else:
        comparison_names = [f"{target} vs rest" for target in targets]
        reference_labels = ["rest"] * len(targets)
        reference_classes = [
            [class_label for class_label in classes if class_label != target]
            for target in targets
        ]

    # =============================================================
    # DISCOVERY
    # =============================================================

    discovery = _run_discovery(
        client,
        org_ids,
        base_args,
        settings,
        "discovery",
    )

    if "error" in discovery:
        return discovery

    summary = discovery["summary"]
    cross_validation = discovery["cross_validation"]
    final = discovery["final"]
    best = cross_validation["best"]

    info(f"Best federated SVM C = {final['C']}")

    # Decision-score distribution of the final model on the discovery data.
    final_scores = _evaluate(
        client,
        org_ids,
        base_args,
        [
            {
                "id": "final",
                "models": [
                    {
                        "fold": None,
                        "weights": final["weights"],
                        "biases": final["biases"],
                        "mean": final["mean"],
                        "std": final["std"],
                    }
                ],
            }
        ],
        "final model scores",
        score_bins=SCORE_BINS,
    ).get("final")

    # =============================================================
    # VALIDATION: LEAVE-ONE-HOSPITAL-OUT
    # =============================================================

    if run_loho:
        loho = _run_loho(client, org_ids, base_args, settings)
    else:
        loho = {
            "available": False,
            "reason": "LOHO was not requested.",
            "results": [],
        }

    loho_final_models = loho.pop("_final_models", [])
    loho_histograms = loho.pop("_score_histograms", None)

    # =============================================================
    # PER-COMPARISON PATTERNS, METRICS AND STABILITY
    # =============================================================

    comparisons = []

    for model_index, target in enumerate(targets):

        resampled = [
            fold_model["weights"][model_index]
            for fold_model in cross_validation["best_fold_models"]
        ] + [
            weights[model_index]
            for weights in loho_final_models
        ]

        stability = _stability(
            final["weights"][model_index],
            resampled,
            stability_top_k,
        )

        cv_comparison = best["comparisons"][model_index]

        loho_comparison = (
            loho["pooled_comparisons"][model_index]
            if loho.get("pooled_comparisons")
            else None
        )

        # Which cohort the pattern characterizes.
        is_reference = len(classes) > 2 and target == reference_cohort

        if len(classes) == 2:
            pattern_of = target if reference_cohort is not None else None
            relative_to = classes[0] if reference_cohort is not None else None
        elif is_reference:
            # The reference cohort's one-vs-rest model is needed for
            # prediction; its pattern is reported but flagged.
            pattern_of = None
            relative_to = None
        else:
            pattern_of = target
            relative_to = "rest"

        signature = _signature(
            final["weights"][model_index],
            features,
            str(target),
            reference_labels[model_index],
            summary["cohorts"][str(target)],
            _reference_profile(
                summary["cohorts"],
                reference_classes[model_index],
            ),
            stability,
        )

        comparison = {
            "comparison": comparison_names[model_index],
            "target_cohort": target,
            "reference_cohorts": reference_classes[model_index],
            # None for a binary comparison without a reference cohort: the
            # result then describes the contrast between both cohorts.
            "pattern_of": pattern_of,
            "relative_to": relative_to,
            "is_reference_cohort": is_reference,
            "cv_metrics": cv_comparison,
            "loho_metrics": loho_comparison,
            "stability": (
                {
                    key: value
                    for key, value in stability.items()
                    if key not in ("top_k_frequency", "sign_agreement")
                }
                if stability is not None
                else None
            ),
            "signature": signature,
            # Two directions of the same hyperplane, not two models.
            "signature_by_direction": {
                str(target): [
                    entry["cpg"]
                    for entry in signature
                    if entry["coefficient"] > 0
                ],
                reference_labels[model_index]: [
                    entry["cpg"]
                    for entry in signature
                    if entry["coefficient"] < 0
                ],
            },
            "model": {
                "weights": final["weights"][model_index],
                "bias": final["biases"][model_index],
                "converged": final["fits"][model_index]["converged"],
                "iterations": final["fits"][model_index]["iterations"],
                "status": final["fits"][model_index]["status"],
            },
        }

        comparisons.append(comparison)

    evidence_summary = _summary(comparisons)

    # =============================================================
    # FINAL OUTPUT
    # =============================================================

    all_partition = summary["partitions"]["all"]

    return {
        "algorithm": "federated_linear_svm",

        "cohort_detection": cohort_detection,

        "classes": classes,
        "features": features,
        "n_features": len(features),
        "n_classes": len(classes),
        "n_total": int(all_partition["n"]),
        "cohort_sizes": {
            str(class_label): int(count)
            for class_label, count in zip(
                classes,
                all_partition["class_counts"],
            )
        },

        "model": {
            "type": (
                "binary_single_hyperplane"
                if len(classes) == 2
                else "one_vs_rest"
            ),
            "loss": "squared_hinge",
            "objective": (
                "0.5*||w||^2 + C * sum_i s_i * max(0, 1 - y_i (w.x_i + b))^2"
                " on globally standardized CpGs, s_i = balanced class weight"
            ),
            "optimizer": "federated Newton with Armijo line search",
            "C": final["C"],
            "standardization": {
                "mean": final["mean"],
                "std": final["std"],
            },
            "class_weights": {
                "positive": final["positive_weights"],
                "negative": final["negative_weights"],
            },
            "converged": all(fit["converged"] for fit in final["fits"]),
        },

        "model_selection": {
            "metric": "balanced_accuracy",
            "procedure": (
                "stratified k-fold per hospital, confusion matrices pooled "
                "over folds and hospitals; ties go to the smaller C"
            ),
            "n_folds": n_folds,
            "best_C": final["C"],
            "candidates": [
                {
                    key: value
                    for key, value in candidate.items()
                    if key != "c_index"
                }
                for candidate in cross_validation["candidates"]
            ],
        },

        "validation": {
            "scope": (
                "CV and LOHO results are conditional on the supplied CpGs."
            ),
            "cv": {
                "C": best["C"],
                "metrics": best["metrics"],
                "confusion_matrix": best["confusion_matrix"],
            },
            "loho": loho,
        },

        # Description of the evidence per comparison; no decision is made.
        "evidence_summary": evidence_summary,

        "comparisons": comparisons,

        "visualization": {
            "score_bins": SCORE_BINS,
            "score_distribution": {
                "final_model_discovery_data": (
                    _histograms_to_dict(
                        final_scores["score_histograms"],
                        comparison_names,
                        classes,
                    )
                    if final_scores is not None
                    else None
                ),
                "loho_held_out": (
                    _histograms_to_dict(
                        loho_histograms,
                        comparison_names,
                        classes,
                    )
                    if loho.get("n_successful")
                    else None
                ),
            },
            "cohort_methylation": {
                str(class_label): {
                    "n": profile["n"],
                    "n_nodes_suppressed": profile["n_nodes_suppressed"],
                    "mean": (
                        profile["mean"].tolist()
                        if profile["mean"] is not None
                        else None
                    ),
                    "sd": (
                        profile["sd"].tolist()
                        if profile["sd"] is not None
                        else None
                    ),
                }
                for class_label, profile in zip(
                    classes,
                    (summary["cohorts"][str(c)] for c in classes),
                )
            },
        },
    }
