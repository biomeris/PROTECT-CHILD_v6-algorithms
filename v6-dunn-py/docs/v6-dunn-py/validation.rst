Validation
==========

References: ``scikit_posthocs.posthoc_dunn`` (0.17, tie-corrected) and
``scipy.stats.kruskal`` on the pooled data; ``statsmodels`` ``multipletests``
for the p-value adjustments.

Exactness of the formulas (``test/test_dunn.py``)
-------------------------------------------------

Fed the true mid-ranks of the pooled data, the score-based tests equal the
references to :math:`10^{-10}`, with and without ties, for every
``p_adjust`` method. With one record per rank bin (no cell floor), the full
three-round pipeline is exact too.

Accuracy of the binned ranks
----------------------------

Simulation: 3 groups with location shifts of 0.5 sd, split over 2-4 nodes,
100 runs per row, default thresholds. "Flips" are pairwise comparisons whose
significance at :math:`\alpha = 0.05` (unadjusted) differs from the exact
test.

.. list-table::
    :header-rows: 1

    * - Total N
      - Rank bins (median)
      - H / exact H (10th-90th pct.)
      - Flips (continuous / discrete data)
    * - 40
      - 3
      - 0.36-1.31
      - 15% / 20%
    * - 90
      - 4-5
      - 0.79-1.04
      - 5% / 7%
    * - 210
      - 7-12
      - 0.93-1.03
      - 2% / 1%
    * - 450
      - 9-24
      - 0.99-1.01
      - 0% / 0%
    * - 1200
      - 10-37
      - 0.99-1.00
      - 0% / 0%

At tens of records the cell floor forces very few, wide rank bins, and the
result should be read as indicative; the algorithm warns below 10 rank bins.
This is the privacy/accuracy trade-off of not pooling values, as documented
for ``v6-kruskal-wallis-py``. ``test/test_dunn.py`` checks that at
:math:`N = 450` (5 seeds, continuous and discrete) H is within 5% and every
Bonferroni-adjusted decision matches.

End-to-end (``test/test_compute.py``)
-------------------------------------

The three synthetic datasets in ``test/`` (130, 180 and 150 rows; groups A-D
with right-skewed scores, B and C shifted up; two missing values each) are run
through the vantage6 ``MockNetwork``. After deletion :math:`N = 454`
(A 113, B 119, C 128, D 94) and 21 rank bins are formed.

Kruskal-Wallis: H = 166.99 federated vs. 167.30 exact.

.. list-table::
    :header-rows: 1

    * - Pair
      - z
      - Bonferroni p (federated)
      - Bonferroni p (exact)
    * - A-B
      - -6.31
      - 1.64e-09
      - 1.63e-09
    * - A-C
      - -11.26
      - 1.22e-28
      - 1.24e-28
    * - A-D
      - -0.42
      - 1
      - 1
    * - B-C
      - -4.90
      - 5.65e-06
      - 5.71e-06
    * - B-D
      - 5.58
      - 1.44e-07
      - 1.26e-07
    * - C-D
      - 10.27
      - 6.05e-24
      - 4.83e-24

For Bonferroni, Holm and Benjamini-Hochberg the test asserts identical
decisions, adjusted p-values within a factor of about 3, and H within 5%. The
same file tests node exclusion, small-group suppression, the coarse-ranks
warning and argument validation.

Run all tests with:

.. code-block:: bash

    uv run pytest test
