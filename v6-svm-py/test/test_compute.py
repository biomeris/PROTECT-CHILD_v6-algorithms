"""
Run this script to test your compute functions locally (without building a Docker
image) using MockNetwork.

Run as:

    uv sync --group dev
    uv run python test/test_compute.py

Requires vantage6-algorithm-tools and scikit-learn. The script exits with a
non-zero status if any check fails.

Test for the federated Linear SVM: discovery and characterization of
cohort-specific DNA methylation patterns. The module reports the discovered
pattern and the evidence for it; it does not decide whether a pattern is an
episignature.

Input data (test/data/, created by test/generate_test_data.py):

    hospital_*.csv                   one mock hospital each: sample_id, cohort
                                     label and CpG beta values
    cpgs.txt                         the CpG list given to the SVM
    ground_truth.json                planted signal (only used for checks)

The algorithm is run once, without telling it the cohorts: it detects them
from the cohort column and runs the binary pipeline (two cohorts, one
hyperplane) or the one-vs-rest pipeline (three or more cohorts). The report
is adapted to the detected pipeline.

Checks (see section 15 of the specification):

- the vantage6 algorithm executes and detects the cohorts correctly;
- the model converges (federated Newton);
- hyperparameter selection matches a centralized reference that uses the
  same folds, standardization and class weights;
- the federated model matches scikit-learn's LinearSVC (same objective);
- the planted signal is recovered with the correct directions;
- LOHO and stability are reported.

The centralized scikit-learn analysis is ONLY a validation/reference
implementation. It is NOT part of the production federated workflow.
"""

import contextlib
import io
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from sklearn.metrics import balanced_accuracy_score
from sklearn.svm import LinearSVC

from vantage6.algorithm.mock.network import MockNetwork

from federated_svm.federated import _make_stratified_folds


# =====================================================================
# CONFIGURATION
# =====================================================================

DATA_DIR = Path(__file__).parent / "data"

# Filled in by load_data() from the hospital names in ground_truth.json, so
# the test uses every hospital the generator wrote.
HOSPITAL_FILES = []

OUTCOME_COLUMN = "cohort"

DATABASE_LABEL = "default"

N_FOLDS = 5

RANDOM_SEED = 42

MIN_SAMPLES = 5

C_GRID = [
    0.001,
    0.01,
    0.1,
    1.0,
    10.0,
    100.0,
]

N_TOP_CPGS_SHOWN = 10


# =====================================================================
# INPUT DATA
# =====================================================================

def load_data():
    """
    Load the hospital datasets, the CpG list and the ground truth.
    """

    if not (DATA_DIR / "ground_truth.json").exists():
        sys.exit(
            "Missing test data. Run: python test/generate_test_data.py"
        )

    ground_truth = json.loads(
        (DATA_DIR / "ground_truth.json").read_text()
    )

    HOSPITAL_FILES[:] = [
        f"{hospital}.csv"
        for hospital in ground_truth["hospitals"]
    ]

    missing = [
        name
        for name in HOSPITAL_FILES + ["cpgs.txt"]
        if not (DATA_DIR / name).exists()
    ]

    if missing:
        sys.exit(
            f"Missing test data {missing}. "
            "Run: python test/generate_test_data.py"
        )

    datasets = [
        pd.read_csv(DATA_DIR / name)
        for name in HOSPITAL_FILES
    ]

    cpgs = (DATA_DIR / "cpgs.txt").read_text().split()

    return datasets, cpgs, ground_truth


# =====================================================================
# FEDERATED RUN
# =====================================================================

def run_federated(datasets, cpgs, reference_cohort):
    """
    Run the vantage6 algorithm through MockNetwork. The cohorts are not
    given: the algorithm detects them. The reference cohort (e.g. the
    control group) is a study-design input.

    The vantage6 log output is captured and only shown if the run fails.
    """

    log = io.StringIO()

    try:
        with contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):

            # IMPORTANT:
            # "federated_svm" must match the algorithm package name
            # (see pyproject.toml and PKG_NAME in the Dockerfile).
            network = MockNetwork(
                module_name="federated_svm",
                datasets=[
                    {DATABASE_LABEL: {"database": dataframe}}
                    for dataframe in datasets
                ],
            )

            client = network.user_client

            dataframe_id = next(
                dataframe["id"]
                for dataframe in client.dataframe.list()
                if dataframe["name"] == DATABASE_LABEL
            )

            # The central function runs at one organization and creates
            # the subtasks for all organizations itself.
            task = client.task.create(
                method="central",
                arguments={
                    "features": cpgs,
                    "outcome_column": OUTCOME_COLUMN,
                    "reference_cohort": reference_cohort,
                    "C_grid": C_GRID,
                    "n_folds": N_FOLDS,
                    "random_seed": RANDOM_SEED,
                    "min_samples": MIN_SAMPLES,
                },
                organizations=[network.organization_ids[0]],
                name="Federated SVM test",
                databases=[
                    {"type": "dataframe", "dataframe_id": dataframe_id}
                ],
            )

            results = client.wait_for_results(task_id=task.get("id"))

    except SystemExit:
        print(log.getvalue()[-5000:])
        raise

    result = results[0]

    if "error" in result:
        print(log.getvalue()[-5000:])

    return result


# =====================================================================
# CENTRALIZED REFERENCE (SAME FOLDS, SAME OBJECTIVE)
# =====================================================================

def _node_arrays(datasets, cpgs, classes):
    """
    Per node: X, labels and fold ids, exactly as the nodes compute them.
    Like the nodes, a hospital with fewer than MIN_SAMPLES complete samples
    takes no part.
    """

    nodes = []

    for dataframe in datasets:

        sub = dataframe[cpgs + [OUTCOME_COLUMN]].dropna()
        sub = sub[sub[OUTCOME_COLUMN].isin(classes)]

        if len(sub) < MIN_SAMPLES:
            continue

        labels = sub[OUTCOME_COLUMN].to_numpy()

        nodes.append(
            (
                sub[cpgs].to_numpy(dtype=float),
                labels,
                _make_stratified_folds(labels, N_FOLDS, RANDOM_SEED),
            )
        )

    return nodes


def _targets(classes):

    return [classes[1]] if len(classes) == 2 else list(classes)


def _fit_reference(X, labels, classes, C):
    """
    scikit-learn LinearSVC with the same objective as the federated SVM:
    squared hinge, balanced class weights, standardized CpGs, one model per
    comparison. A large intercept_scaling makes the intercept (practically)
    unregularized.
    """

    mean = X.mean(axis=0)
    std = X.std(axis=0)
    std = np.where(std < 1e-12, 1.0, std)

    Xs = (X - mean) / std

    models = []

    for target in _targets(classes):

        model = LinearSVC(
            C=C,
            loss="squared_hinge",
            dual=False,
            class_weight="balanced",
            intercept_scaling=1000.0,
            tol=1e-10,
            max_iter=100000,
        )

        model.fit(Xs, labels == target)
        models.append(model)

    return models, mean, std


def _predict_reference(models, mean, std, X, classes):

    scores = np.column_stack(
        [
            model.decision_function((X - mean) / std)
            for model in models
        ]
    )

    if len(classes) == 2:
        return np.where(scores[:, 0] >= 0.0, classes[1], classes[0])

    return np.asarray(classes)[np.argmax(scores, axis=1)]


def run_sklearn_reference(datasets, cpgs, classes):
    """
    Centralized analogue of the federated workflow: the same local folds,
    pooled standardization per fold, balanced class weights and pooled
    cross-validation predictions.
    """

    nodes = _node_arrays(datasets, cpgs, classes)

    candidates = []

    for C in C_GRID:

        y_true = []
        y_pred = []

        for fold in range(N_FOLDS):

            X_train = np.vstack(
                [X[fold_ids != fold] for X, _, fold_ids in nodes]
            )
            y_train = np.concatenate(
                [labels[fold_ids != fold] for _, labels, fold_ids in nodes]
            )

            models, mean, std = _fit_reference(X_train, y_train, classes, C)

            for X, labels, fold_ids in nodes:

                validation = fold_ids == fold

                # Small hospitals can have empty folds, as at the nodes.
                if not validation.any():
                    continue

                y_true.append(labels[validation])
                y_pred.append(
                    _predict_reference(
                        models, mean, std, X[validation], classes
                    )
                )

        candidates.append(
            {
                "C": C,
                "balanced_accuracy": balanced_accuracy_score(
                    np.concatenate(y_true),
                    np.concatenate(y_pred),
                ),
            }
        )

    best_score = max(
        candidate["balanced_accuracy"]
        for candidate in candidates
    )

    best_C = min(
        candidate["C"]
        for candidate in candidates
        if candidate["balanced_accuracy"] >= best_score - 1e-12
    )

    return {
        "best_C": best_C,
        "candidates": candidates,
        "X": np.vstack([X for X, _, _ in nodes]),
        "labels": np.concatenate([labels for _, labels, _ in nodes]),
    }


# =====================================================================
# REPORT
# =====================================================================

def print_report(result):
    """
    Human-readable summary, adapted to the detected pipeline.
    """

    detection = result["cohort_detection"]
    binary = detection["pipeline"] == "binary_single_hyperplane"

    print("\nCOHORTS")
    print(f"  Detected (complete samples): {detection['detected']}")

    for cohort, reason in detection["excluded"].items():
        print(f"  Excluded {cohort}: {reason}")

    classes = result["classes"]

    if binary:
        print(
            f"  Pipeline: binary - one hyperplane, "
            f"'{classes[1]}' vs '{classes[0]}'"
        )
        if detection["reference_cohort"] is not None:
            print(f"  Reference cohort: {detection['reference_cohort']}")
    else:
        print(
            f"  Pipeline: multi-cohort - one-vs-rest, "
            f"{len(classes)} comparisons"
        )

    selection = result["model_selection"]

    print("\nMODEL SELECTION (pooled CV balanced accuracy)")

    for candidate in selection["candidates"]:
        marker = "  <- selected" if candidate["C"] == selection["best_C"] else ""
        print(
            f"  C={candidate['C']:<8} "
            f"BA={candidate['metrics']['balanced_accuracy']:.4f} "
            f"converged={candidate['n_models_converged']}"
            f"/{candidate['n_models']}{marker}"
        )

    loho = result["validation"]["loho"]

    if loho["available"]:
        print("\nLEAVE-ONE-HOSPITAL-OUT")
        if loho["note"]:
            print(f"  Note: {loho['note']}")
        for hospital in loho["results"]:
            if hospital["status"] == "success":
                print(
                    f"  held out {hospital['held_out_hospital']}: "
                    f"BA={hospital['metrics']['balanced_accuracy']:.4f} "
                    f"(n={hospital['n']}, C={hospital['selected_C']})"
                )
            else:
                print(
                    f"  held out {hospital['held_out_hospital']}: "
                    f"{hospital['status']} - {hospital['reason']}"
                )

    for comparison in result["comparisons"]:

        target = str(comparison["target_cohort"])

        if comparison["is_reference_cohort"]:
            print(f"\nCOHORT {target} vs rest (reference cohort)")
        elif comparison["pattern_of"] is not None:
            print(
                f"\nPATTERN OF {comparison['pattern_of']} "
                f"(relative to {comparison['relative_to']})"
            )
        else:
            print(f"\nCONTRAST: {comparison['comparison']}")

        cv = comparison["cv_metrics"]
        print(
            f"  CV: BA={cv['balanced_accuracy']:.4f} "
            f"sensitivity={cv['sensitivity']:.4f} "
            f"specificity={cv['specificity']:.4f}"
        )

        if comparison["loho_metrics"] is not None:
            print(
                "  LOHO: BA="
                f"{comparison['loho_metrics']['balanced_accuracy']:.4f}"
            )

        if comparison["stability"] is not None:
            stability = comparison["stability"]
            print(
                "  Stability: coefficient correlation="
                f"{stability['mean_coefficient_correlation']:.4f}, "
                f"top-{stability['top_k']} overlap="
                f"{stability['mean_top_k_jaccard']:.4f}"
            )

        # Binary: the two directions of the same hyperplane. Multi-cohort:
        # the CpGs pointing towards this cohort and towards the rest.
        directions = (
            [str(classes[1]), str(classes[0])]
            if binary
            else [target, "rest"]
        )

        for direction in directions:

            entries = [
                entry
                for entry in comparison["signature"]
                if entry["direction"] == direction
            ][:N_TOP_CPGS_SHOWN]

            print(f"  CpGs towards {direction}:")

            for entry in entries:
                difference = entry["methylation"]["mean_difference"]
                difference_text = (
                    "n/a (suppressed)"
                    if difference is None
                    else f"{difference:+.3f}"
                )
                print(
                    f"    {entry['rank']:>3}. {entry['cpg']}  "
                    f"coef={entry['coefficient']:+.3f}  "
                    f"delta_beta={difference_text}"
                )


def print_summary(result):
    """
    The algorithm's plain-language summary of the evidence. It describes
    the discovered patterns; judging whether a pattern is an episignature is
    left to the researcher.
    """

    print("\n" + "=" * 70)
    print("SUMMARY OF THE PATTERNS")
    print("=" * 70)

    for block in result["evidence_summary"]:
        print("\n" + "-" * 70)
        print(block)


# =====================================================================
# CHECKS
# =====================================================================

CHECKS = []


def check(name, condition, detail=""):

    CHECKS.append((name, bool(condition)))

    status = "PASS" if condition else "FAIL"
    suffix = f" ({detail})" if detail else ""

    print(f"  [{status}] {name}{suffix}")


def expected_directions(comparison, classes, ground_truth):
    """
    Planted CpGs relevant to a comparison and the cohort/side each one
    should point to (the side where it is more methylated).
    """

    binary = len(classes) == 2
    target = comparison["target_cohort"]
    expected = {}

    for cohort, cpg_deltas in ground_truth["differential_cpgs"].items():

        if binary:
            other = next(c for c in classes if c != cohort)
            for cpg, delta in cpg_deltas.items():
                expected[cpg] = cohort if delta > 0 else other

        elif cohort == target:
            for cpg, delta in cpg_deltas.items():
                expected[cpg] = cohort if delta > 0 else "rest"

    return {cpg: str(side) for cpg, side in expected.items()}


def run_checks(result, datasets, cpgs, ground_truth):

    print("\nCHECKS")

    # -------------------------------------------------------------
    # Cohort detection
    # -------------------------------------------------------------

    present = sorted(
        set().union(*(set(df[OUTCOME_COLUMN].dropna()) for df in datasets)),
        key=str,
    )

    check(
        "all cohorts in the data are detected",
        sorted(result["cohort_detection"]["detected"]) == sorted(
            str(cohort) for cohort in present
        ),
        f"{result['cohort_detection']['detected']}",
    )

    expected_pipeline = (
        "binary_single_hyperplane"
        if len(result["classes"]) == 2
        else "one_vs_rest"
    )

    check(
        "pipeline matches the number of cohorts",
        result["model"]["type"] == expected_pipeline
        and len(result["comparisons"])
        == (1 if len(result["classes"]) == 2 else len(result["classes"])),
        result["model"]["type"],
    )

    # Complete samples per hospital; hospitals below MIN_SAMPLES take no part.
    complete_per_hospital = [
        len(df[cpgs + [OUTCOME_COLUMN]].dropna())
        for df in datasets
    ]
    usable_hospitals = sum(n >= MIN_SAMPLES for n in complete_per_hospital)
    complete = sum(n for n in complete_per_hospital if n >= MIN_SAMPLES)

    check(
        "samples with missing values are excluded",
        result["n_total"] == complete,
        f"n_total={result['n_total']}",
    )

    # -------------------------------------------------------------
    # Convergence
    # -------------------------------------------------------------

    check("final model converged", result["model"]["converged"])

    check(
        "all cross-validation models converged",
        all(
            candidate["n_models_converged"] == candidate["n_models"]
            for candidate in result["model_selection"]["candidates"]
        ),
    )

    check(
        "every C in the grid was evaluated",
        len(result["model_selection"]["candidates"]) == len(C_GRID),
    )

    # -------------------------------------------------------------
    # Centralized reference
    # -------------------------------------------------------------

    classes = result["classes"]

    reference = run_sklearn_reference(datasets, cpgs, classes)

    federated_ba = {
        candidate["C"]: candidate["metrics"]["balanced_accuracy"]
        for candidate in result["model_selection"]["candidates"]
    }

    max_ba_difference = max(
        abs(federated_ba[candidate["C"]] - candidate["balanced_accuracy"])
        for candidate in reference["candidates"]
    )

    check(
        "CV balanced accuracy matches reference for every C",
        max_ba_difference < 0.01,
        f"max difference {max_ba_difference:.4f}",
    )

    check(
        "selected C matches reference",
        result["model_selection"]["best_C"] == reference["best_C"],
        f"federated {result['model_selection']['best_C']}, "
        f"reference {reference['best_C']}",
    )

    reference_models, _, _ = _fit_reference(
        reference["X"],
        reference["labels"],
        classes,
        result["model"]["C"],
    )

    relative_difference = max(
        np.max(np.abs(np.asarray(comparison["model"]["weights"])
                      - model.coef_[0]))
        / np.max(np.abs(model.coef_[0]))
        for comparison, model in zip(result["comparisons"], reference_models)
    )

    check(
        "coefficients match LinearSVC (same objective)",
        relative_difference < 1e-3,
        f"max relative difference {relative_difference:.2e}",
    )

    # -------------------------------------------------------------
    # Signal recovery
    # -------------------------------------------------------------

    for comparison in result["comparisons"]:

        expected = expected_directions(comparison, classes, ground_truth)

        if not expected:
            continue

        top = comparison["signature"][:len(expected)]
        recovered = [entry for entry in top if entry["cpg"] in expected]

        check(
            f"{comparison['comparison']}: planted CpGs ranked on top",
            len(recovered) >= 0.75 * len(expected),
            f"{len(recovered)}/{len(expected)} in the top {len(expected)}",
        )

        wrong = [
            entry["cpg"]
            for entry in recovered
            if entry["direction"] != expected[entry["cpg"]]
        ]

        check(
            f"{comparison['comparison']}: recovered CpGs point to the "
            "more methylated side",
            not wrong,
            f"wrong: {wrong}" if wrong else "",
        )

    # -------------------------------------------------------------
    # Attribution of the pattern to cohorts
    # -------------------------------------------------------------

    signal_cohorts = sorted(ground_truth["differential_cpgs"], key=str)

    attributed = sorted(
        (
            str(comparison["pattern_of"])
            for comparison in result["comparisons"]
            if comparison["pattern_of"] is not None
        ),
        key=str,
    )

    if len(classes) == 2:
        check(
            "pattern is reported for the non-reference cohort",
            attributed == signal_cohorts,
            f"pattern of {attributed}",
        )
    else:
        check(
            "every non-reference cohort has its own comparison",
            attributed == signal_cohorts,
            f"comparisons for {attributed}",
        )

    # -------------------------------------------------------------
    # Validation output
    # -------------------------------------------------------------

    loho = result["validation"]["loho"]

    check(
        "LOHO evaluated every hospital with enough complete samples",
        loho["available"] and loho["n_successful"] == usable_hospitals,
        f"{loho.get('n_successful')} of {len(datasets)} hospitals "
        f"({usable_hospitals} with at least {MIN_SAMPLES} complete samples)",
    )

    check(
        "stability is reported for every comparison",
        all(
            comparison["stability"] is not None
            for comparison in result["comparisons"]
        ),
    )


# =====================================================================
# MAIN
# =====================================================================

def main():

    print("=" * 70)
    print("FEDERATED LINEAR SVM TEST")
    print("=" * 70)

    datasets, cpgs, ground_truth = load_data()

    for name, dataframe in zip(HOSPITAL_FILES, datasets):
        print(
            f"{name}: {len(dataframe)} samples, "
            f"{dataframe[OUTCOME_COLUMN].value_counts().to_dict()}"
        )

    print(f"{len(cpgs)} CpGs in cpgs.txt")

    # In a real study the reference cohort (e.g. controls) comes from the
    # study design. Here it is the reference used to generate the data.
    reference_cohort = ground_truth["reference_cohort"]

    print(f"Reference cohort: {reference_cohort}")

    result = run_federated(datasets, cpgs, reference_cohort)

    if "error" in result:
        print(f"\nERROR: {result['error']}")
        sys.exit(1)

    print_report(result)

    run_checks(result, datasets, cpgs, ground_truth)

    failed = [name for name, passed in CHECKS if not passed]

    print(f"\n{len(CHECKS) - len(failed)}/{len(CHECKS)} checks passed")

    if failed:
        print("FAILED:")
        for name in failed:
            print(f"  - {name}")

    print_summary(result)

    print("=" * 70)

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
