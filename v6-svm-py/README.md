# v6-svm-py

Federated linear SVM for the discovery and characterization of cohort-specific
DNA methylation patterns (vantage6 v5). It answers: "Do these predefined cohorts
show a discriminative methylation pattern, and which CpGs characterize it?" It
reports the evidence; it does not decide whether a pattern is an episignature.

The method and its validation are described in `docs/` and checked by
`test/test_compute.py` (federated == scikit-learn's LinearSVC, planted signal
recovered).

## Input

- `features`: the CpGs to analyse, e.g. the `selected_cpgs` of
  `v6-elastic-net-py`. Only the list is used, never coefficients. Or
  `features_task_id`: a finished Elastic Net task, whose selected CpGs (and, if
  `classes` is not given, cohorts) are read from its result. The result states
  where the CpGs came from (`input`). With neither, the most recent finished
  Elastic Net task of this session is used (for `apply_model`: the most recent
  SVM task).
- `organizations_to_include` (optional): the hospitals to use; all by default.
- `outcome_column`: the column with each sample's cohort.
- `classes` (optional): the cohorts to compare. Two cohorts give one hyperplane;
  more give one-vs-rest. By default all cohorts with at least `min_samples`
  samples.

The data at each hospital can be:

- **wide**: one row per sample, one column per CpG plus the cohort column; or
- **long**, as stored by `v6-extractionIDAT-py`: one row per sample and probe
  (`probe_id`, `sample_label`, `cohort`, `beta`, `m_value`). The node pivots it
  for the requested CpGs only. `sample_column` and `value_column` (`beta` or
  `m_value`) name the columns.

Every hospital needs all the CpGs. A hospital that lacks some (e.g. removed by
its quality control) is left out and listed in the result's `skipped`, with the
reason. With CpGs from `v6-elastic-net-py` this does not happen: it only selects
CpGs available in every hospital.

## Applying a trained model

`apply_model` applies a finished SVM model, unchanged, to the samples of chosen
hospitals and reports the confusion matrix and balanced accuracy (95% interval)
per hospital and pooled. The model comes from a finished SVM task
(`model_task_id`) or as JSON (`model`: features, classes, weights, biases, mean,
std). With `organizations_to_include` on both steps it gives a fair evaluation:
train the pipeline on some hospitals, apply the model to a hospital that took no
part. A model trained on hospitals 1-2 and applied to hospital 3 gives exactly
the SVM's leave-one-hospital-out result for hospital 3 (tested).

If HQ refuses to create the per-hospital subtasks (e.g. the dataframe does not
exist at one of the hospitals), the task stops at once with HQ's message.

## Federated rounds

The SVM is fitted by federated Newton steps; every step is one task at every
hospital. A run consists of the discovery on all hospitals and, for
leave-one-hospital-out (LOHO) validation, one discovery per held-out hospital.
These experiments share the federated rounds: their models are fitted and
evaluated in the same tasks, and each job lists the hospitals whose data it
uses (a hospital skips jobs that are not its own, using its organization id).
The results are identical to running the experiments one after another.

On the test data this reduced the number of tasks from 108 to 48 (two cohorts)
and from 292 to 101 (three cohorts). On a real node every task restarts the
algorithm and reloads the hospital's data, so the number of tasks drives the run
time. For a quick run use `run_loho=false` and a shorter `C_grid`.

## Output

Cohort detection, the model (C, standardization, class weights), model
selection, validation (pooled cross-validation and LOHO), one entry per
comparison (ranked CpGs, direction per cohort, metrics, stability), a
plain-language summary of the evidence, data for visualization, and `skipped`:
the hospitals that could not take part in a step, with the reason.

## Evidence summary

`evidence_summary` describes each comparison in plain language: the cohorts the
pattern separates, the CpGs that contribute most (with their observed
methylation direction), the evidence and what it does not mean. It applies no
accuracy threshold and gives no episignature verdict.

- Key CpGs: with two cohorts they are grouped by the cohort they push towards
  (the two sides of one hyperplane). In one-vs-rest "the rest" is a mix of
  cohorts, so they are grouped by their observed methylation in the target
  cohort ("Higher / Lower methylation in X", with delta beta and coefficient):
  a CpG with lower methylation characterizes the cohort as much as one with
  higher methylation.
- Balanced accuracies come with a 95% interval: each cohort's accuracy gets a
  Beta(correct + 1, wrong + 1) posterior and the balanced accuracy is their
  mean (Brodersen et al., 2010). The text reads e.g. "The SVM separated lipid
  from the other cohorts with a cross-validated balanced accuracy of 0.967
  (95% CI 0.82-0.98)". If the interval includes 0.5 it says that
  the data do not show a separation better than chance (0.5 is chance level by
  definition, not a chosen threshold). Pooled cross-validation predictions are
  not fully independent, so the interval is approximate.
- `features_selected_on_same_samples`: whether the CpGs were selected on these
  same samples (e.g. Elastic Net on the same hospitals). `true` adds that the
  accuracies are optimistic and that generalization needs samples left out of
  the CpG selection too; `false` states that they were selected independently;
  unset says that this is unknown. It changes only the text.

## Open privacy points

To be decided by the project before use on patient data:

- **Second-order statistics.** Each Newton step returns, per hospital, the
  Hessian (CpG x CpG) of the objective over its active samples. With more CpGs
  than patients per hospital this reveals much about each patient, and many
  steps with different weights reveal more. Keep the number of CpGs well below
  the number of patients per hospital, or add noise.
- **Fold differencing.** Hospitals return sums over all samples and over the
  training samples of each fold. Their difference is the sum over a validation
  fold, which in a small hospital (e.g. 6-12 samples, 5 folds) holds 1-3
  patients, below `min_samples`. Fewer folds for small hospitals would avoid it.
- `partial_cohorts` returns the exact number of samples of every cohort, also
  of cohorts below `min_samples`.

## Tests

```bash
pip install -e . scikit-learn
python test/test_compute.py   # the method: federated == centralized, planted signal
python test/test_sandbox.py   # long input, batched LOHO == separate runs,
                              # missing CpGs, real vantage6 client, PKG_NAME,
                              # summary wording and interval
```

## Build

```bash
docker build -t svm:1.5.0 .
```

The Dockerfile's `PKG_NAME` is the module folder, `federated_svm`.
