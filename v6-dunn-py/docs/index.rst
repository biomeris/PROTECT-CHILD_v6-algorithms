v6-dunn-py
==================

Description
-----------

Federated Dunn's post-hoc test: pairwise comparisons of mean ranks between
groups, with Bonferroni, Holm or Benjamini-Hochberg adjustment, reported
together with the Kruskal-Wallis test. Global ranks are approximated in three
rounds without any node sharing an individual value, following the GT
federated Kruskal-Wallis algorithm (``v6-kruskal-wallis-py``).

Authors
-------

Claire Ai (Georgia Tech), for the PROTECT-CHILD consortium (WP5, Task 5.2)

The rank-binning protocol is ported from ``v6-kruskal-wallis-py`` (Y. Zhong).

Source code
-----------

See ``v6-dunn-py/central.py`` (orchestration and tests, run on one node),
``v6-dunn-py/federated.py`` (the three per-node rounds),
``v6-dunn-py/dunn_math.py`` (the pure math shared by both) and the
``Dockerfile`` at the algorithm root.


Contents
--------

.. toctree::
   :maxdepth: 2
   :hidden:

   self

.. toctree::
    :maxdepth: 2

    v6-dunn-py/implementation
    v6-dunn-py/usage
    v6-dunn-py/privacy
    v6-dunn-py/validation
    v6-dunn-py/references
