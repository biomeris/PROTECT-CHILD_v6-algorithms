Implementation
==============

Overview
--------

Bonferroni correction controls the family-wise error rate when testing multiple
hypotheses. Given ``m`` p-values :math:`p_1, \dots, p_m`, each is adjusted as:

.. math::

   p_{\text{adjusted},i} = \min(1,\ p_i \cdot m)

A hypothesis is rejected at significance level ``alpha`` when
:math:`p_{\text{adjusted},i} \le \alpha`. This matches the convention used by
``statsmodels.stats.multitest.multipletests`` (``<=``, not a strict ``<``).

Central (``compute_bonferroni_correction``)
--------------------------------------------

Unlike every other algorithm in this repository, the central function here
dispatches **no subtasks** to the nodes and does not take an ``AlgorithmClient``
argument at all. That is a deliberate consequence of what Bonferroni correction
actually is: deterministic arithmetic over a set of p-values, not a statistic
computed from patient-level data. The p-values are supplied directly as task
arguments - typically copied from the output of another federated test that *was*
run across the nodes (e.g. ``v6-fisher-exact-test-py``'s ``p_value`` field, or
per-site p-values from ``v6-t-test-py``).

The central function:

1. Normalizes ``p_values`` (a ``dict[label, float]`` or ``list[float]``) into a
   labelled dict.
2. Validates every p-value is a finite real number in ``[0, 1]``, that ``alpha``
   is strictly between 0 and 1, and that ``num_tests`` (defaulting to
   ``len(p_values)``) is a finite integer no smaller than the number of p-values
   supplied.
3. Applies the correction formula above to each entry and returns
   ``{p_value, p_adjusted, reject}`` per label, alongside the ``alpha`` and
   ``num_tests`` that were used.

Federated functions
--------------------

None. There is nothing to compute at the nodes (see "Central" above).
