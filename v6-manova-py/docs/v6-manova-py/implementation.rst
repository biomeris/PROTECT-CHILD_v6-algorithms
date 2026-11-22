Implementation
==============

Overview
--------

A one-way MANOVA with :math:`k` groups, :math:`p` outcomes and :math:`N`
complete cases compares the between-group sums of squares and cross-products
(SSCP) matrix :math:`B` with the within-group SSCP matrix :math:`W`. All four
test statistics are functions of the eigenvalues :math:`\lambda_i` of
:math:`W^{-1}B`:

.. math::

    \text{Wilks' } \Lambda = \prod_i \frac{1}{1 + \lambda_i} \qquad
    \text{Pillai's } V = \sum_i \frac{\lambda_i}{1 + \lambda_i} \qquad
    \text{Hotelling-Lawley } U = \sum_i \lambda_i \qquad
    \text{Roy's } \theta = \max_i \lambda_i

Both matrices can be written in terms of per-group sufficient statistics: the
count :math:`n_g`, the column-sum vector :math:`s_g` and the cross-product
matrix :math:`Q_g = X_g^\top X_g`. With :math:`S = \sum_g s_g` and
:math:`Q = \sum_g Q_g`:

.. math::

    W &= Q - \sum_g \frac{s_g s_g^\top}{n_g} \\
    B &= \sum_g \frac{s_g s_g^\top}{n_g} - \frac{S S^\top}{N}

Each term is a sum over data stations, so the federated computation is exact:
it needs a single round of communication and no iteration.

Central (``manova``)
--------------------

1. Validates the arguments (at least two distinct outcome columns, group
   column not among them).
2. Sends one ``compute_local_summary`` subtask per organization, so that each
   result can be attributed to the organization that produced it.
3. Excludes organizations that report too few records, and records which
   groups each organization suppressed (see :doc:`privacy`).
4. Sums :math:`n_g`, :math:`s_g` and :math:`Q_g` per group across
   organizations, builds :math:`W` and :math:`B`, and solves the symmetric
   generalized eigenproblem :math:`B x = \lambda W x`.
5. Computes the four statistics and their F approximations, using the same
   formulas as SAS ``PROC GLM`` and ``statsmodels`` (Rao's F for Wilks'
   lambda; Roy's F is an upper bound).

The function raises an ``InputError`` if fewer than two groups remain, if the
error degrees of freedom :math:`N - k` are smaller than :math:`p`, or if
:math:`W` is singular (an outcome is constant within groups, or the outcomes
are linearly dependent).

Federated functions
-------------------

``compute_local_summary``
~~~~~~~~~~~~~~~~~~~~~~~~~

1. Checks that the columns exist and that the outcomes are numeric.
2. Drops rows with a missing group or outcome (listwise deletion, as
   ``statsmodels`` does by default).
3. Normalizes group labels to strings (``1`` and ``1.0`` both become ``"1"``)
   so that labels match across nodes whose column dtypes differ.
4. Suppresses groups below ``MANOVA_MINIMUM_GROUP_SIZE`` and applies the
   ``MANOVA_MINIMUM_NUMBER_OF_RECORDS`` guard.
5. Returns :math:`n_g`, :math:`s_g` and :math:`Q_g` for each remaining group.

All math lives in ``manova_math.py`` without vantage6 decorators, so it can be
unit tested directly.
