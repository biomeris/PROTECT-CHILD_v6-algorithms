v6-bonferroni-py
==================

Description
-----------

Bonferroni correction for multiple hypothesis testing. Given a family of ``m``
p-values, each is adjusted as ``p_adjusted = min(1, p * m)`` and compared against
a chosen significance level ``alpha``. This is a purely arithmetic, central-only
algorithm - it does not dispatch any computation to the nodes, since it operates
on p-values that were already produced by another federated test (e.g.
``v6-fisher-exact-test-py``, ``v6-t-test-py``, ``v6-anova-py``).

Authors
-------

H. Sathu

Source code
-----------

The source code and Dockerfile for this algorithm live in the ``v6-bonferroni-py``
directory of the
`PROTECT-CHILD_v6-algorithms <https://github.com/biomeris/PROTECT-CHILD_v6-algorithms>`_
repository.

Contents
--------

.. toctree::
   :maxdepth: 2
   :hidden:

   self

.. toctree::
    :maxdepth: 2

    v6-bonferroni-py/implementation
    v6-bonferroni-py/usage
    v6-bonferroni-py/privacy
    v6-bonferroni-py/references
