Privacy
=======

Guards
------

``EFA_MINIMUM_NUMBER_OF_RECORDS`` (default 10)
    If a node has this many complete cases or fewer, it shares nothing and is
    excluded from the analysis. Set in ``globals.py``; overridable per node
    through an environment variable of the same name. The default is a
    placeholder pending the consortium's minimum-records policy.

Data sharing
------------

Each node sends its number of complete cases :math:`n`, the column sums of
the selected variables (:math:`p` numbers) and the cross-product matrix
:math:`X^\top X` (:math:`p(p+1)/2` distinct numbers). No individual records
leave the node, and nothing is broken down by subgroup.

The central function returns pooled aggregates only: the correlation matrix,
loadings and derived statistics. It does not return means. When only one
organization contributes, the correlation matrix describes that
organization's data, and the result carries a warning.

Vulnerabilities to known attacks
--------------------------------

.. list-table::
    :widths: 25 10 65
    :header-rows: 1

    * - Attack
      - Risk eliminated?
      - Risk analysis
    * - Reconstruction
      - ⚠
      - The moments hold :math:`p + p(p+1)/2` numbers. With the record
        threshold well above that count, they do not determine the
        individual records; choose the threshold with the number of columns
        in mind.
    * - Differencing
      - ❌
      - Running the analysis twice on cohorts that differ by one patient
        (e.g. through ``organizations_to_include``, a changed local dataset,
        or a changed column list that alters listwise deletion) reveals that
        patient's contribution. This is a residual risk of aggregate-sharing
        designs in general.
    * - Deep Leakage from Gradients (DLG)
      - ✔
      - No gradients are shared.
    * - Generative Adversarial Networks (GAN)
      - ✔
      - No generative model is trained.
    * - Model Inversion
      - ✔
      - The model (loadings) is fitted on pooled correlations only.
    * - Watermark Attack
      - ✔
      - Not applicable.
