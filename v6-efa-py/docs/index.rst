v6-efa-py
==================

Description
-----------

Federated exploratory factor analysis (EFA). Builds the pooled Pearson
correlation matrix of the selected variables, reports sampling adequacy (KMO)
and Bartlett's test of sphericity, extracts factors (minres, maximum
likelihood or principal) and rotates them (varimax, promax or oblimin). Only
the number of records, column sums and a cross-product matrix leave a data
station; the result is identical to an EFA on the pooled data.

Authors
-------

Claire Ai (Georgia Tech), for the PROTECT-CHILD consortium (WP5, Task 5.2)

Source code
-----------

See ``v6-efa-py/central.py`` (orchestration and factor analysis, run on one
node), ``v6-efa-py/federated.py`` (per-node moments, run on every
participating data station), ``v6-efa-py/efa_math.py`` (the pure math shared by
both) and the ``Dockerfile`` at the algorithm root.


Contents
--------

.. toctree::
   :maxdepth: 2
   :hidden:

   self

.. toctree::
    :maxdepth: 2

    v6-efa-py/implementation
    v6-efa-py/usage
    v6-efa-py/privacy
    v6-efa-py/validation
    v6-efa-py/references
