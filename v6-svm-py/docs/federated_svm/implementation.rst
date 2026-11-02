Implementation
==============

Overview
--------

The SVM is an independent federated module for the discovery and characterization
of cohort-specific DNA methylation patterns. It helps a researcher answer: *do
these predefined cohorts show a discriminative methylation pattern, and which CpGs
characterize it?* It receives a set of CpGs (for example selected by Elastic Net)
and the cohort label of each sample. Samples stay at the hospitals; only
aggregates are exchanged.

The module reports the discovered pattern and the evidence for it. It does not
decide whether a pattern is an episignature: that characterization and validation
is done downstream by the researcher.

The cohorts are detected automatically from the cohort column across all
hospitals (or restricted with ``classes``). Two cohorts give one hyperplane
(``classes[1]`` vs ``classes[0]``), reported in two directions. More than two
cohorts give one-vs-rest comparisons, each with its own ranked CpGs and evidence.
Cohorts with fewer than ``min_samples`` complete samples are excluded and
reported.

The model minimizes the L2-regularized squared-hinge SVM objective on globally
standardized CpGs with globally balanced class weights::

    F(w, b) = 0.5 * ||w||^2 + C * sum_i s_i * max(0, 1 - y_i (w.x_i + b))^2

This is the same objective as scikit-learn's ``LinearSVC`` (squared hinge), so
``C`` has the same meaning. It is optimized exactly with federated Newton steps.

Central (``central``)
---------------------
The central part is responsible for the orchestration and aggregation of the algorithm.

1. **Discovery.** Federated standardization and class counts, cross-validated
   selection of ``C`` by balanced accuracy (ties go to the smaller ``C``) and
   training of the final model on all hospitals. All candidate models (C values x
   folds x comparisons) are optimized together, so the number of federated rounds
   depends on the number of Newton iterations only.
2. **Evidence.**

   - Cross-validation: stratified folds assigned locally at each hospital;
     confusion matrices pooled over folds and hospitals.
   - Leave-one-hospital-out (LOHO): for each hospital, the complete discovery is
     repeated without it and the model is evaluated once on it.
   - Stability: correlation, top-k overlap and sign agreement between the final
     coefficients and the models trained on resampled data (CV folds and LOHO).

3. **Output.** Ranked CpG contributions with direction, cohort methylation means
   and stability, per-comparison metrics, LOHO results, a plain-language summary
   of the evidence, and data for visualization (decision-score histograms per
   cohort, cohort methylation profiles). No thresholds are applied and no
   episignature verdict is returned.

Validation results are conditional on the supplied CpGs: if the CpGs were selected
using all hospitals, LOHO inside the SVM does not validate the complete selection +
SVM pipeline. That requires orchestrating the selection step per held-out hospital.

Federated functions
-------------------
Federated functions are the computations that are executed on each node. The federated
functions have access to the data that is stored on the node. These functions are
executed in parallel on each node.

All functions only use complete samples of the analysed cohorts; samples of other
cohorts at the same hospital are ignored. A node only returns aggregates pooled
over at least ``min_samples`` samples.

``partial_cohorts``
~~~~~~~~~~~~~~~~~~~

The cohort labels present at the node and the number of complete samples in each.
Used to detect the cohorts and choose the pipeline.

``partial_summary``
~~~~~~~~~~~~~~~~~~~

Per training partition (all data and each fold): ``n``, ``sum(X)``, ``sum(X^2)``
and cohort counts. For all data, per-cohort ``sum(X)`` and ``sum(X^2)``; cohorts
with fewer than ``min_samples`` samples at the node are suppressed.

``partial_svm``
~~~~~~~~~~~~~~~

For a batch of models: the local squared-hinge loss and its gradient and
generalized Hessian with respect to ``(w, b)``. The central part adds the
regularization term and ``C`` and takes a Newton step with Armijo line search.

``partial_evaluate``
~~~~~~~~~~~~~~~~~~~~

Applies models to validation folds (pooled over folds) or to all local samples, and
returns the cohort confusion matrix, a 2x2 matrix per comparison and, optionally,
decision-score histograms per cohort.
