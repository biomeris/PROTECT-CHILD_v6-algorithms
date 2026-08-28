Privacy
=======

What is shared
---------------

At no point does raw patient-level data (individual time-to-event values,
event indicators, or covariate vectors) leave a data station. Every federated
round returns only:

- Unique event times and their local counts (``get_unique_event_times``) -
  not per-patient times.
- The locally-observed levels of categorical covariates
  (``get_categorical_levels``) - a small, low-cardinality set of category
  labels, not per-patient values.
- A summed covariate vector over local event subjects (``compute_summed_z``)
  - an aggregate sum, not individual values.
- Per-event-time risk-set aggregate sums :math:`S_0, S_1, S_2`
  (``perform_iteration``) - sums (and sums-of-outer-products) over
  potentially many local subjects, not individual values.

The only thing broadcast back to nodes each round is the current, low-
dimensional model parameter vector :math:`\beta` - never any other node's
aggregate data.

This "sufficient statistics" approach follows WebDISCO (Lu et al., 2015),
the standard method for federated/distributed Cox model fitting without
patient-level data sharing.

N-threshold guard
------------------

Each node refuses to answer ``get_unique_event_times`` (and is excluded from
the analysis) if its local event count is at or below
``COXPH_MINIMUM_EVENTS`` (default 10, overridable via the
``COXPH_MINIMUM_EVENTS`` environment variable at the node), preventing the
computation from ever running on a very small or potentially identifiable
local cohort.

Minimum risk-set-size guard
----------------------------

The N-threshold guard bounds each node's *total* local event count, but not
the size of an individual risk set at a specific, often extreme, event time
(e.g. the very last observed event, where only one or two subjects anywhere
in the federation may still be "at risk"). At such a time, the exchanged
:math:`S_0, S_1, S_2` aggregates approach being a direct, reversible function
of that handful of subjects' covariates.

``perform_iteration`` therefore also reports the local head-count of each
risk set (not exp-weighted, so it carries no information about :math:`\beta`
or the covariate values themselves - it's a count only). Centrally, these are
summed into the true *global* risk-set size per event time. If the smallest
global risk set used anywhere in the fit falls below
``COXPH_MINIMUM_RISK_SET_SIZE`` (default 3), the returned result carries an
explicit warning identifying this so the requester can judge whether to trust
or discard the fit - the model is still returned rather than blocked outright,
since the appropriate response (accept the risk, aggregate time bins more
coarsely, exclude the tail, etc.) is a policy decision, not one this algorithm
can make unilaterally.

Residual risks
--------------

- **Differencing attacks**: a node operator who changes their local dataset
  between repeated runs of the same task could in principle infer sensitive
  information from the difference between two rounds' aggregates. This is a
  known residual risk of aggregate-sharing designs in general and is not
  specific to this algorithm.
- **Reconstruction / model inversion**: given only sums, counts and
  sums-of-outer-products (never raw records), reconstructing individual
  patient data from the exchanged aggregates is considered low-risk in
  practice for risk sets above ``COXPH_MINIMUM_RISK_SET_SIZE`` - see the
  minimum risk-set-size guard above for the small-risk-set case.
