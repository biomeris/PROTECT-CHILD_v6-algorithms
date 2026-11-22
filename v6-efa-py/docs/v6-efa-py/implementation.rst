Implementation
==============

Overview
--------

Every step of an EFA after data collection works on the :math:`p \times p`
correlation matrix :math:`R` and the sample size :math:`N`. Both can be built
exactly from per-node moments: the count :math:`n`, the column-sum vector
:math:`s` and the cross-product matrix :math:`Q = X^\top X`. With
:math:`S = \sum s`, :math:`Q = \sum Q_\text{node}` and :math:`N = \sum n`:

.. math::

    C = \frac{Q - S S^\top / N}{N - 1}, \qquad
    R = D^{-1/2} C D^{-1/2}, \quad D = \operatorname{diag}(C)

A single round of communication is enough, and the federated EFA equals an
EFA on the pooled data.

Central (``efa``)
-----------------

1. Validates the arguments.
2. Sends one ``compute_local_moments`` subtask per organization, so each
   result can be attributed to the organization that produced it, and excludes
   organizations that report too few records.
3. Builds :math:`R` and computes:

   - the eigenvalues of :math:`R` (for a scree plot, and the Kaiser
     criterion used when ``n_factors`` is not given);
   - the Kaiser-Meyer-Olkin measure, overall and per item, from the partial
     correlations :math:`-R^{-1}_{ij} / \sqrt{R^{-1}_{ii} R^{-1}_{jj}}`;
   - Bartlett's test of sphericity,
     :math:`\chi^2 = -\left(N - 1 - \tfrac{2p + 5}{6}\right)\ln|R|` with
     :math:`p(p-1)/2` degrees of freedom.

4. Extracts the factors:

   - ``minres``: minimizes the squared off-diagonal residuals of
     :math:`R - \Lambda\Lambda^\top` over the uniquenesses;
   - ``ml``: maximizes the normal likelihood over the uniquenesses;
   - ``principal``: principal-component loadings
     :math:`V_k \Lambda_k^{1/2}` from the eigendecomposition of :math:`R`.

   For ``minres`` and ``ml`` the uniquenesses start from
   :math:`1 / R^{-1}_{ii}` (one minus the squared multiple correlation) and
   are optimized with L-BFGS-B within :math:`[0.005, 1]`, as in
   ``factor_analyzer`` and R's ``psych::fa``.

5. Rotates the loadings: ``varimax`` (Kaiser-normalized), ``promax``
   (power 4, on Kaiser-normalized varimax loadings) or ``oblimin``
   (:math:`\gamma = 0`, gradient projection). Oblique rotations also return
   the factor correlation matrix :math:`\Phi` and the structure matrix
   :math:`\Lambda\Phi`.
6. Flips each factor so its loadings sum to a positive number and orders the
   factors by decreasing sum of squared loadings (reordering :math:`\Phi` to
   match).

Warnings are added to the result when the overall KMO is below 0.6, when the
optimizer does not converge, when a uniqueness hits its lower bound (a
Heywood case), and when the number of factors leaves the model with negative
degrees of freedom.

Federated functions
-------------------

``compute_local_moments``
~~~~~~~~~~~~~~~~~~~~~~~~~

1. Checks that the columns exist and are numeric.
2. Drops rows with a missing value in any selected column (listwise
   deletion).
3. Applies the ``EFA_MINIMUM_NUMBER_OF_RECORDS`` guard.
4. Returns :math:`n`, :math:`s` and :math:`X^\top X`.

All math lives in ``efa_math.py`` without vantage6 decorators, so it can be
unit tested directly. It has no dependency beyond numpy and scipy;
``factor_analyzer`` is used only as a test reference.
