Privacy
=======

Guards
------

All thresholds are set in ``globals.py``; node-side ones are overridable per
node through environment variables of the same name. The defaults match
``v6-kruskal-wallis-py`` and are placeholders pending the consortium's
minimum-records policy.

``DUNN_MINIMUM_NUMBER_OF_RECORDS`` (default 10)
    A node with this many eligible records or fewer shares nothing and is
    excluded from all rounds.

``DUNN_MINIMUM_GROUP_SIZE`` (default 5)
    A group with fewer records at a node is left out of all three rounds at
    that node. The suppressed labels (not their counts) are reported.

``DUNN_MINIMUM_CELL_COUNT`` (default 5)
    No non-empty histogram cell below this count leaves a node. Every rank bin
    also holds at least this many records per contributing node.

Data sharing
------------

No round ever transmits an individual value:

- Round 1: count, sum and sum of squares of the eligible values (3 numbers,
  no group breakdown, no minimum or maximum).
- Round 2: record counts per grid bin, without group labels, with small cells
  merged away.
- Round 3: per group, the count, sum and sum of squares of the rank-bin
  scores (3 numbers per group).

The central function returns group sizes, mean ranks and test statistics.

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
      - The round-2 histogram locates values to within a grid cell holding at
        least ``DUNN_MINIMUM_CELL_COUNT`` records. Round-3 score sums are over
        groups of at least ``DUNN_MINIMUM_GROUP_SIZE`` records and only reveal
        rank bins, not values.
    * - Differencing
      - ❌
      - Running the analysis twice on cohorts that differ by one patient
        (e.g. through ``organizations_to_include`` or a changed local
        dataset) can reveal that patient's histogram cell and group. This is
        a residual risk of aggregate-sharing designs in general.
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
      - Not applicable.
