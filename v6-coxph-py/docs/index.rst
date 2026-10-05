v6-coxph-py
==================

Description
-----------

Federated Cox Proportional Hazards regression, fit via Newton-Raphson over
WebDISCO-style risk-set aggregates. Only aggregate statistics ever leave a
data station - individual patient records and covariate vectors are never
shared.

Authors
-------

H. Sathu

Source code
-----------

See ``v6-coxph-py/central.py`` (orchestration, run on one node) and
``v6-coxph-py/federated.py`` (per-node computations, run on every
participating data station) in this repository, and the ``Dockerfile`` at the
repository root for the container build.


Contents
--------

.. toctree::
   :maxdepth: 2
   :hidden:

   self

.. toctree::
    :maxdepth: 2

    v6-coxph-py/implementation
    v6-coxph-py/usage
    v6-coxph-py/privacy
    v6-coxph-py/validation
    v6-coxph-py/references
