v6-manova-py
==================

Description
-----------

Federated one-way multivariate analysis of variance (MANOVA). Tests whether
the mean vector of several numeric outcomes differs between groups, reporting
Wilks' lambda, Pillai's trace, the Hotelling-Lawley trace and Roy's greatest
root with their F approximations. Only per-group counts, sums and
cross-product matrices leave a data station; the result is identical to a
MANOVA on the pooled data.

Authors
-------

Claire Ai (Georgia Tech), for the PROTECT-CHILD consortium (WP5, Task 5.2)

Source code
-----------

See ``v6-manova-py/central.py`` (orchestration and test statistics, run on one
node), ``v6-manova-py/federated.py`` (per-node summary, run on every
participating data station), ``v6-manova-py/manova_math.py`` (the pure math
shared by both) and the ``Dockerfile`` at the algorithm root.


Contents
--------

.. toctree::
   :maxdepth: 2
   :hidden:

   self

.. toctree::
    :maxdepth: 2

    v6-manova-py/implementation
    v6-manova-py/usage
    v6-manova-py/privacy
    v6-manova-py/validation
    v6-manova-py/references
