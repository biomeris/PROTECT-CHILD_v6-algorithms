Validation
==========

Results are compared with ``factor_analyzer`` 0.5.1 fitted on the pooled raw
data. ``factor_analyzer`` follows R's ``psych::fa``; it is installed only for
testing and needs ``scikit-learn < 1.8``.

End-to-end (``test/test_compute.py``)
-------------------------------------

The three synthetic datasets in ``test/`` (70, 110 and 90 rows; variables
``x1``-``x8``; two missing values each) were simulated from two factors
correlated at 0.3, with ``x1``-``x4`` loading on one and ``x5``-``x8`` on the
other. They are run through the vantage6 ``MockNetwork``.

With the defaults (minres, promax, Kaiser criterion), after listwise deletion
:math:`N = 264`:

- eigenvalues 3.063, 2.089, 0.644, ... → 2 factors;
- KMO 0.793; Bartlett :math:`\chi^2 = 715.8`, 28 df, :math:`p < 10^{-100}`;
- the factor correlation is 0.232.

.. list-table::
    :header-rows: 1

    * - Variable
      - F1
      - F2
      - Communality
    * - x1
      - 0.789
      - -0.081
      - 0.628
    * - x2
      - 0.715
      - -0.054
      - 0.515
    * - x3
      - 0.767
      - 0.088
      - 0.596
    * - x4
      - 0.618
      - 0.044
      - 0.384
    * - x5
      - -0.042
      - 0.820
      - 0.674
    * - x6
      - 0.110
      - 0.721
      - 0.532
    * - x7
      - -0.070
      - 0.681
      - 0.468
    * - x8
      - 0.012
      - 0.661
      - 0.438

Loadings and communalities match ``factor_analyzer`` to :math:`10^{-5}` for
minres + promax, ML + varimax and minres + oblimin. The correlation matrix
matches ``pandas.DataFrame.corr`` to :math:`10^{-12}`; KMO and Bartlett match
``factor_analyzer``. The same file tests the Kaiser default, node exclusion,
``rotation="none"`` and argument validation.

Math (``test/test_efa.py``)
---------------------------

- Every method (minres, ML, principal) × every rotation (none, varimax,
  promax, oblimin) × 1, 2 or 3 factors, on federated moments from three
  nodes: loadings, communalities, sums of squared loadings and (for oblique
  rotations) the structure matrix match ``factor_analyzer`` to
  :math:`10^{-6}` (:math:`10^{-5}` for ML), after aligning factor order and
  sign.
- The pooled correlation matrix, KMO and Bartlett's test are exact.
- A known two-factor structure is recovered from a large simulated sample.
- Missing values, the record threshold, column validation and a constant
  column.

Known differences from ``factor_analyzer``
------------------------------------------

- After an oblique rotation, ``factor_analyzer`` reorders the loadings by
  variance but not ``phi_``. This implementation reorders both.
- ``factor_analyzer`` does not re-sort ``principal`` factors after rotation;
  this implementation sorts every method's factors by variance.
- ``factor_analyzer`` imputes missing values with the median; this
  implementation drops incomplete rows.

Run all tests with:

.. code-block:: bash

    uv run pytest test
