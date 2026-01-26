Privacy
=======

.. _privacy-guards:

Guards
------

There are several guards in place to protect sharing too much information on individual
records:

- **Minimum number of data rows to participate**: A node will only participate if it
  contains at least `n` data rows. This is to prevent nodes with very little data from
  participating in the computation. By default, the minimum number of data rows is set
  to 5. Node administrators can change this minimum by adding the following to their
  node configuration file:

  .. code:: yaml

    algorithm_env:
      SUMMARY_MINIMUM_ROWS: 5

- **Thresholding**: For categorical variables, the algorithm will count how often each
  unique value occurs. If the number of unique values is below a certain threshold, the
  algorithm will not share the count. The default value for this threshold is 5. Node
  administrators can change this threshold by adding the following to their node
  configuration file:

  .. code:: yaml

    algorithm_env:
      SUMMARY_PRIVACY_THRESHOLD: 5

- **Setting the allowed columns**: The node administrator can set on which
  columns they want to allow or disallow computation by
  adding the following to the node configuration file:

  .. code:: yaml

    algorithm_env:
      SUMMARY_ALLOWED_COLUMNS: "ageGroup,isOverweight"
      SUMMARY_DISALLOWED_COLUMNS: "age,weight"

  This configuration will ensure that only the columns `ageGroup` and `isOverweight`
  are allowed to be used in the computations. The columns `age`
  and `weight` are disallowed and will not be used. Usually, there
  should either be an allowed or disallowed list, but not both: if there is an explicit
  allowed list, all other columns are automatically disallowed.

- **Setting allowed statistics**: Some node administrators may not be comfortable with
  sharing certain statistics. By default, all statistics are allowed. Node
  administrators can turn off the sharing of certain statistics by
  adding the following to the node configuration file:

  .. code:: yaml

    algorithm_env:
      SUMMARY_ALLOW_MIN: false
      SUMMARY_ALLOW_MAX: false
      SUMMARY_ALLOW_STD: false
      SUMMARY_ALLOW_SUM: false
      SUMMARY_ALLOW_COUNT: false
      SUMMARY_ALLOW_MISSING: false
      SUMMARY_ALLOW_VARIANCE: false
      SUMMARY_ALLOW_COUNTS_UNIQUE_VALUES: false
      SUMMARY_ALLOW_NUM_COMPLETE_ROWS: false

  By default, all statistics are allowed to be shared.

Data sharing
------------

The intermediately shared data is very similar to the final results, except that they
represent the data of a single data station rather than the combined data of all data
stations. The shared data is:

- **For numerical columns**: The count, sum, minimum, maximum, variance sum, and
  number of missing values.
- **For categorical columns**: The count, number of missing values, and the count of
  unique values (if above the threshold).
- The number of complete rows in the dataset.

Vulnerabilities to known attacks
--------------------------------

.. Table below lists some well-known attacks. You could fill in this table to show
.. which attacks would be possible in your system.

.. list-table::
    :widths: 25 10 65
    :header-rows: 1

    * - Attack
      - Risk eliminated?
      - Risk analysis
    * - Reconstruction
      - ✔
      -
    * - Differencing
      - ❌
      - May be possible by making smart selection with preprocessing, or by sending
        multiple tasks before and after data is updated.
    * - Deep Leakage from Gradients (DLG)
      - ✔
      -
    * - Generative Adversarial Networks (GAN)
      - ✔
      -
    * - Model Inversion
      - ✔
      -
    * - Watermark Attack
      - ✔
      -