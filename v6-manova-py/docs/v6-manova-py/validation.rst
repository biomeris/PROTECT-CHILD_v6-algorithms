Validation
==========

Every federated result is compared with ``statsmodels.multivariate.manova.MANOVA``
fitted on the pooled data (``y1 + ... ~ group``, statistics for the ``group``
term).

End-to-end (``test/test_compute.py``)
-------------------------------------

The three synthetic datasets in ``test/`` (37, 60 and 41 rows; groups A, B and
C; outcomes ``y1``-``y3``; two missing values each) are run through the
vantage6 ``MockNetwork``. After listwise deletion, :math:`N = 132`
(A: 49, B: 43, C: 40).

.. list-table::
    :header-rows: 1

    * - Test
      - Value (federated = pooled)
      - F
      - p
    * - Wilks' lambda
      - 0.8681503144
      - 3.101088
      - 0.00599
    * - Pillai's trace
      - 0.1353243809
      - 3.096431
      - 0.00605
    * - Hotelling-Lawley trace
      - 0.1478718469
      - 3.117877
      - 0.00637
    * - Roy's greatest root
      - 0.1121995974
      - 4.787183
      - 0.00340

Value, degrees of freedom, F and p agree to a relative tolerance of
:math:`10^{-9}`. The same file tests node exclusion, small-group suppression,
``organizations_to_include`` and argument validation.

Math (``test/test_manova.py``)
------------------------------

- 27 configurations: 2, 3 or 5 groups × 2, 3 or 5 outcomes × 1, 3 or 4 nodes
  with random uneven splits. All four statistics, their degrees of freedom, F
  values and p-values match ``statsmodels`` to a relative tolerance of
  :math:`10^{-7}`.
- With one outcome, every F equals ``scipy.stats.f_oneway``.
- Groups absent at some nodes, missing values, and integer vs. float group
  labels across nodes.
- Guards and errors: small-group suppression, the record threshold (counted
  after suppression), a single group, and collinear outcomes.

Run all tests with:

.. code-block:: bash

    uv run pytest test
