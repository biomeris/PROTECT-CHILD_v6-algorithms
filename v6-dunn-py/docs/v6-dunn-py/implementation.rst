Implementation
==============

Overview
--------

Dunn's test compares every pair of groups :math:`i, j` by their mean ranks
:math:`\bar R_i, \bar R_j`:

.. math::

    z_{ij} = \frac{\bar R_i - \bar R_j}
                  {\sqrt{\sigma^2 \left(\frac{1}{n_i} + \frac{1}{n_j}\right)}},
    \qquad
    \sigma^2 = \frac{N(N+1)}{12} - \frac{\sum (t^3 - t)}{12(N - 1)}

with two-sided normal p-values, adjusted over all :math:`k(k-1)/2` pairs. The
tie-corrected :math:`\sigma^2` is exactly the variance of the mid-ranks,
:math:`\sum (R - \bar R)^2 / (N - 1)`. The Kruskal-Wallis statistic uses the
same quantities: :math:`H = \sum_g n_g (\bar R_g - \bar R)^2 / \sigma^2`.

So both tests need, per group, only the count, the sum of ranks and the sum of
squared ranks. The hard part is the ranks themselves: they are global, and
computing them exactly would require pooling the values.

Global ranks without sharing values
-----------------------------------

The protocol of ``v6-kruskal-wallis-py`` is reused unchanged:

Round 1 (``compute_local_moments``)
    Each node returns the count, sum and sum of squares of its eligible values,
    with no group breakdown. The central function pools them into a mean and
    standard deviation and lays out a 200-bin equal-width grid over
    mean ± 4 sd.

Round 2 (``compute_local_histogram``)
    Each node counts its values per grid bin, without group labels; values
    outside the grid fall into the outer bins. Non-empty cells below
    ``DUNN_MINIMUM_CELL_COUNT`` are merged into a neighbour before the counts
    leave the node. The central function sums the histograms and merges
    adjacent bins into *rank bins*, each holding at least
    ``DUNN_MINIMUM_CELL_COUNT`` × (number of nodes) records and at least
    :math:`N / 50` records. Every rank bin gets the global mid-rank of the
    records it holds.

Round 3 (``compute_local_rank_scores``)
    Each node scores every record with the mid-rank of its rank bin and
    returns, per group, the count, sum of scores and sum of squared scores.

Records sharing a rank bin are treated as tied. With one record per bin the
scores are the true ranks and the result is exact (``test/test_dunn.py``
checks this); in practice the bins are coarser and the result is an
approximation whose accuracy grows with :math:`N` (see :doc:`validation`).

Central (``dunn``)
------------------

1. Validates the arguments.
2. Runs the three rounds, each as one subtask per organization so every
   result can be attributed to its organization. Organizations below the
   record threshold in round 1 are excluded from all later rounds.
3. Pools the per-group score sums and computes the Kruskal-Wallis test and all
   pairwise Dunn's z-tests, with signed :math:`z` (positive when
   ``group_1`` ranks higher).
4. Adjusts the p-values (``bonferroni``, ``holm``, ``fdr_bh`` or ``none``,
   identical to ``statsmodels.stats.multitest.multipletests``).

A warning is added when fewer than 10 rank bins could be formed.

All math lives in ``dunn_math.py`` without vantage6 decorators, so it can be
unit tested directly.
