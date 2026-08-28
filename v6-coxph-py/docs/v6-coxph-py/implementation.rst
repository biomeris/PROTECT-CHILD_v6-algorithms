Implementation
==============

The model
---------

The Cox Proportional Hazards (CoxPH) model relates a set of covariates
:math:`x_i` for subject :math:`i` to their hazard of an event at time
:math:`t` via

.. math::

    h(t \mid x_i) = h_0(t) \cdot \exp(\beta' x_i)

The regression coefficients :math:`\beta` are estimated by maximizing **Cox's
partial likelihood**, which conditions out the unknown baseline hazard
:math:`h_0(t)` and, at each observed event time :math:`t_k`, only compares the
covariate vector of the subject(s) who had the event against the covariate
vectors of everyone still "at risk" at that time (the *risk set*
:math:`R(t_k)`, i.e. everyone not yet failed or censored):

.. math::

    L(\beta) = \sum_{\text{events } i} \left[ \beta' x_i -
        \log \left( \sum_{j \in R(t_i)} \exp(\beta' x_j) \right) \right]

Tied event times are handled with **Breslow's method**: events that occur at
the same observed time are grouped, and the per-time event count (``freq``)
is used as a multiplier on the risk-set-based terms of the score and Hessian,
rather than computing a distinct term per tied subject.

:math:`\beta` is found via **Newton-Raphson** iteration:

.. math::

    \beta_{\text{new}} = \beta_{\text{old}} - H(\beta_{\text{old}})^{-1}
        \cdot U(\beta_{\text{old}})

where :math:`U(\beta)` is the score (gradient) and :math:`H(\beta)` is the
Hessian of :math:`L(\beta)`. Both are built, at every unique event time
:math:`t_k`, from three risk-set sums:

.. math::

    S_0(\beta, t_k) &= \sum_{j \in R(t_k)} \exp(\beta' x_j) \\
    S_1(\beta, t_k) &= \sum_{j \in R(t_k)} x_j \exp(\beta' x_j) \\
    S_2(\beta, t_k) &= \sum_{j \in R(t_k)} x_j x_j' \exp(\beta' x_j)

After convergence, standard errors are the square roots of the diagonal of
the observed Fisher information (:math:`(-H)^{-1}`), from which Z-values,
two-sided p-values, 95% confidence intervals, an overall Wald chi-square test
and the AIC are derived exactly as a non-federated CoxPH fit would.

Federated decomposition
------------------------

This is a distributed, WebDISCO-style (Lu et al., 2015) fit: every round, each
data station returns only aggregate statistics - sums, counts and
sums-of-products over its own subjects - never individual records. The
central function (``v6-coxph-py/central.py``) orchestrates a fixed sequence of
subtasks against the federated functions (``v6-coxph-py/federated.py``), each
executed once per participating organization:

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Round
     - Runs on
     - What it computes
   * - ``get_unique_event_times``
     - each node
     - Local ``(time, event count)`` pairs for its own subjects who had the
       event; refuses (privacy guard) if the local event count is at or below
       the minimum threshold. Central unions and sums these by time to obtain
       the global failure-time set with Breslow tie counts, retrying up to 3
       times while excluding organizations that fail the threshold.
   * - ``get_categorical_levels``
     - each node
     - The locally-observed levels of any ``category``-dtype explanatory
       variable. Central unions these across nodes to build one consistent
       one-hot encoding (indicator columns, dropped reference level) that
       every subsequent round uses.
   * - ``compute_summed_z``
     - each node
     - Local ``sum(x_i)`` over its own event subjects. Central sums these into
       ``z_sum``, the :math:`\beta`-independent linear term of :math:`U(\beta)`
       and :math:`L(\beta)`. Computed once, since it does not depend on
       :math:`\beta`.
   * - ``perform_iteration``
     - each node, repeated every Newton-Raphson epoch
     - Local contribution to :math:`S_0, S_1, S_2` at every global unique
       event time, using the current broadcast :math:`\beta` (local risk set =
       local subjects with ``time >= t_k``). Central sums these into the
       global :math:`S_0, S_1, S_2`, derives :math:`U(\beta)`/:math:`H(\beta)`
       (``compute_derivatives``), and takes one Newton-Raphson step. Repeats
       until ``max|Δβ| <= tolerance``, a NaN is detected, or the epoch limit is
       reached.

The final inference step (standard errors, Wald test, AIC, confidence
intervals, perfect-separation warnings) uses only the already-aggregated
statistics from the last iteration and needs no further network round.
