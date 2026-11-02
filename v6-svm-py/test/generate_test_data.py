"""
Generate synthetic methylation data for testing the federated SVM.

The data mimic the input of the SVM module: for every sample, a cohort label
(defined in a previous step of the analysis) and the methylation beta values
of a list of CpGs (for example the CpGs selected by Elastic Net). They are
designed to resemble Illumina EPICv2 array data:

- Bimodal beta values: each CpG's baseline methylation is drawn from a
  mixture of a hypomethylated peak (~0.1-0.2), a small intermediate
  component (~0.5) and a hypermethylated peak (~0.8-0.9), each modelled with
  a beta distribution. Sample values are then drawn from a beta distribution
  around that baseline.
- Differentially methylated CpGs: about a quarter of the CpGs (n_cpgs // 4,
  split over the non-reference cohorts) carry a difference of 0.10-0.35 in
  beta, a size typical of reported episignatures.
- Three hospitals of unequal size (45, 30 and 15 samples) with a small batch
  effect each, to stress-test leave-one-hospital-out validation.
- Unequal cohort proportions within each hospital (e.g. 0.25 / 0.35 / 0.40,
  rotated between hospitals), to exercise the balanced class weights.
- About 1.5% missing values (probe failures), mostly concentrated in a few
  low-quality samples, as on real arrays, plus scattered failures.

Files written to test/data/:

    hospital_1.csv, hospital_2.csv, hospital_3.csv
        One row per sample: sample_id, cohort, and one column per CpG with
        beta values in [0, 1]. Some values are missing.
    cpgs.txt
        The CpG list given to the SVM (one CpG per line).
    ground_truth.json
        The planted differentially methylated CpGs and the hospital names.
        Only used by the test; not an input of the algorithm.

Run as:

    python test/generate_test_data.py
    python test/generate_test_data.py --cpgs 80 --cohorts early_rejection no_rejection

The default cohorts are early_rejection, late_rejection and no_rejection. The
last cohort is the reference: every other cohort gets its own set of
differentially methylated CpGs relative to it.

Remove this this file when the SVM module is ready to be tested with real data.
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


DATA_DIR = Path(__file__).parent / "data"

SEED = 2024

# Number of samples per hospital (unequal, as in real federated studies).
HOSPITALS = {
    "hospital_1": 45,
    "hospital_2": 30,
    "hospital_3": 15,
}

DEFAULT_COHORTS = [
    "early_rejection",
    "late_rejection",
    "no_rejection",
]

# Cohort proportions within a hospital for three cohorts. They are rotated
# between hospitals so that each hospital has a different majority cohort.
# For another number of cohorts, unequal proportions are derived in the same
# spirit (see _cohort_proportions).
COHORT_PROPORTIONS = [0.25, 0.35, 0.40]

DEFAULT_N_CPGS = 50

# Mixture model of baseline methylation: (weight, beta-distribution a, b).
# Hypomethylated peak (mean ~0.14), intermediate (mean 0.5) and
# hypermethylated peak (mean ~0.86).
BASELINE_MIXTURE = [
    (0.45, 2.0, 12.0),
    (0.10, 10.0, 10.0),
    (0.45, 12.0, 2.0),
]

# Beta-distribution precision of the sample values: higher means less
# biological noise around each CpG's cohort mean.
PRECISION = 30.0

# Size of the planted methylation differences (absolute delta beta).
DELTA_RANGE = (0.10, 0.35)

# Fraction of all CpGs that are differentially methylated (split over the
# non-reference cohorts).
DIFFERENTIAL_FRACTION = 0.25

# Standard deviation of the per-hospital batch effect (logit scale).
BATCH_SD = 0.10

# Fraction of missing values (probe failures) over all values.
MISSING_RATE = 0.015

# Probe failures on arrays are dominated by a few low-quality samples (e.g.
# poor bisulfite conversion) that lose many probes, while good samples only
# have scattered failures. This fraction of the missing values falls in
# LOW_QUALITY_SAMPLE_FRACTION of the samples; the rest is spread at random.
# (Uniform missingness at 1.5% would make about half of the samples
# incomplete, and the SVM uses complete samples only.)
LOW_QUALITY_MISSING_SHARE = 0.75
LOW_QUALITY_SAMPLE_FRACTION = 0.05


def _logit(p):
    return np.log(p / (1.0 - p))


def _expit(x):
    return 1.0 / (1.0 + np.exp(-x))


def _cohort_proportions(n_cohorts, hospital_index):
    """
    Unequal cohort proportions for one hospital, rotated between hospitals.
    """

    if n_cohorts == len(COHORT_PROPORTIONS):
        proportions = np.asarray(COHORT_PROPORTIONS, dtype=float)
    else:
        proportions = np.linspace(1.0, 1.6, n_cohorts)

    proportions = proportions / proportions.sum()

    return np.roll(proportions, hospital_index)


def _cohort_sizes(n_samples, proportions):
    """
    Integer cohort sizes that sum to n_samples (largest remainder method).
    """

    exact = proportions * n_samples
    sizes = np.floor(exact).astype(int)

    remaining = n_samples - sizes.sum()
    order = np.argsort(-(exact - sizes))
    sizes[order[:remaining]] += 1

    return sizes


def generate(n_cpgs, cohorts, seed=SEED):

    if not 15 <= n_cpgs <= 100:
        raise ValueError("The number of CpGs must be between 15 and 100.")

    if len(cohorts) < 2:
        raise ValueError("At least two cohorts are required.")

    rng = np.random.default_rng(seed)

    # Illumina-like CpG identifiers.
    cpgs = [
        f"cg{number:08d}"
        for number in rng.choice(10 ** 8, size=n_cpgs, replace=False)
    ]

    # Baseline methylation from the bimodal mixture model.
    weights = np.array([component[0] for component in BASELINE_MIXTURE])
    components = rng.choice(
        len(BASELINE_MIXTURE),
        size=n_cpgs,
        p=weights / weights.sum(),
    )

    baseline = np.array([
        rng.beta(BASELINE_MIXTURE[c][1], BASELINE_MIXTURE[c][2])
        for c in components
    ])

    baseline = np.clip(baseline, 0.02, 0.98)

    # Planted differences: each non-reference cohort gets its own CpGs.
    reference_cohort = cohorts[-1]
    signal_cohorts = cohorts[:-1]

    n_differential = max(
        3,
        int(n_cpgs * DIFFERENTIAL_FRACTION) // len(signal_cohorts),
    )

    shuffled = rng.permutation(n_cpgs)

    cohort_means = {
        cohort: baseline.copy()
        for cohort in cohorts
    }

    differential = {}

    for cohort_index, cohort in enumerate(signal_cohorts):

        indices = shuffled[
            cohort_index * n_differential:(cohort_index + 1) * n_differential
        ]

        differential[cohort] = {}

        for index in indices:

            delta = rng.uniform(*DELTA_RANGE)

            # Shift towards the side with room to move.
            if baseline[index] > 0.5:
                delta = -delta

            # Keep the shifted mean inside the valid beta range.
            shifted = float(np.clip(baseline[index] + delta, 0.02, 0.98))
            delta = shifted - baseline[index]

            cohort_means[cohort][index] = shifted
            differential[cohort][cpgs[index]] = round(float(delta), 4)

    hospital_frames = {}

    for hospital_index, (hospital, n_samples) in enumerate(HOSPITALS.items()):

        # Unequal cohort sizes, then shuffled.
        sizes = _cohort_sizes(
            n_samples,
            _cohort_proportions(len(cohorts), hospital_index),
        )

        labels = np.repeat(cohorts, sizes)
        rng.shuffle(labels)

        batch = rng.normal(0.0, BATCH_SD, size=n_cpgs)

        values = np.empty((n_samples, n_cpgs))

        for row, label in enumerate(labels):

            mean = _expit(_logit(cohort_means[label]) + batch)

            values[row] = rng.beta(
                mean * PRECISION,
                (1.0 - mean) * PRECISION,
            )

        values = np.round(values, 4)

        # Probe failures: concentrated in a few low-quality samples, plus
        # scattered failures in the other samples.
        n_missing = int(round(MISSING_RATE * values.size))
        n_low_quality = max(1, int(round(LOW_QUALITY_SAMPLE_FRACTION * n_samples)))

        low_quality_rows = rng.choice(n_samples, size=n_low_quality, replace=False)

        n_concentrated = min(
            int(round(LOW_QUALITY_MISSING_SHARE * n_missing)),
            n_low_quality * n_cpgs,
        )

        concentrated = rng.choice(
            n_low_quality * n_cpgs,
            size=n_concentrated,
            replace=False,
        )
        values[
            low_quality_rows[concentrated // n_cpgs],
            concentrated % n_cpgs,
        ] = np.nan

        scattered = rng.choice(
            values.size,
            size=n_missing - n_concentrated,
            replace=False,
        )
        values.flat[scattered] = np.nan

        frame = pd.DataFrame(values, columns=cpgs)
        frame.insert(0, "cohort", labels)
        frame.insert(
            0,
            "sample_id",
            [f"H{hospital_index + 1}_{row + 1:03d}" for row in range(n_samples)],
        )

        hospital_frames[hospital] = frame

    ground_truth = {
        "description": (
            "Synthetic data. Planted methylation differences (delta beta) "
            "of each cohort relative to the reference cohort."
        ),
        "seed": seed,
        "cohorts": cohorts,
        "reference_cohort": reference_cohort,
        "hospitals": HOSPITALS,
        "differential_cpgs": differential,
    }

    return hospital_frames, cpgs, ground_truth


def write(hospital_frames, cpgs, ground_truth, data_dir=DATA_DIR):

    data_dir.mkdir(parents=True, exist_ok=True)

    # Remove hospital files from a previous run with more hospitals.
    for old_file in data_dir.glob("hospital_*.csv"):
        if old_file.stem not in hospital_frames:
            old_file.unlink()

    for hospital, frame in hospital_frames.items():
        frame.to_csv(data_dir / f"{hospital}.csv", index=False)

    (data_dir / "cpgs.txt").write_text("\n".join(cpgs) + "\n")

    (data_dir / "ground_truth.json").write_text(
        json.dumps(ground_truth, indent=2) + "\n"
    )


def main():

    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])

    parser.add_argument(
        "--cpgs",
        type=int,
        default=DEFAULT_N_CPGS,
        help="number of CpGs (15-100)",
    )

    parser.add_argument(
        "--cohorts",
        nargs="+",
        default=DEFAULT_COHORTS,
        help="cohort labels; the last one is the reference cohort",
    )

    parser.add_argument("--seed", type=int, default=SEED)

    arguments = parser.parse_args()

    hospital_frames, cpgs, ground_truth = generate(
        arguments.cpgs,
        arguments.cohorts,
        arguments.seed,
    )

    write(hospital_frames, cpgs, ground_truth)

    for hospital, frame in hospital_frames.items():
        complete = int(frame[cpgs].notna().all(axis=1).sum())
        print(
            f"{hospital}: {len(frame)} samples ({complete} complete), "
            f"{frame['cohort'].value_counts().to_dict()}"
        )

    print(f"{len(cpgs)} CpGs, data written to {DATA_DIR}")


if __name__ == "__main__":
    main()
