Privacy
=======

Guards
------

Both thresholds are set in ``globals.py`` and can be overridden per node via
environment variables of the same name.

``MANOVA_MINIMUM_GROUP_SIZE`` (default 5)
    A group with fewer local complete cases is left out of that node's
    summary. With one record, the group sum *is* the record; with very few,
    the sum and :math:`X^\top X` come close to determining the records. The
    suppressed group labels (not their counts) are reported in the result, so
    the analyst knows the pooled analysis omits them.

``MANOVA_MINIMUM_NUMBER_OF_RECORDS`` (default 10)
    If a node has this many complete cases or fewer, counted after small
    groups are suppressed, it shares nothing and is excluded from the
    analysis.

The defaults are placeholders pending the consortium's minimum-records
policy.

Data sharing
------------

Each node sends, per group, the number of complete cases :math:`n_g`, the
column sums of the outcomes (:math:`p` numbers) and the cross-product matrix
:math:`X_g^\top X_g` (:math:`p(p+1)/2` distinct numbers). No individual
records leave the node.

The central function returns pooled aggregates only: test statistics, group
sizes and means, and the :math:`B` and :math:`W` matrices. When only one
organization contributes, these describe that organization's data, and the
result carries a warning.

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
      - A group's sum and :math:`X^\top X` hold :math:`p + p(p+1)/2` numbers.
        When :math:`n_g` is not much larger than that, they constrain the
        records strongly. The minimum group size guard bounds this; choose it
        with the number of outcomes in mind.
    * - Differencing
      - ❌
      - Running the analysis twice on cohorts that differ by one patient
        (e.g. through ``organizations_to_include`` or a changed local
        dataset) reveals that patient's contribution. This is a residual
        risk of aggregate-sharing designs in general.
    * - Deep Leakage from Gradients (DLG)
      - ✔
      - No gradients are shared.
    * - Generative Adversarial Networks (GAN)
      - ✔
      - No model is trained.
    * - Model Inversion
      - ✔
      - No model is trained.
    * - Watermark Attack
      - ✔
      - No model is trained.
