Privacy
=======

Guards
------

- **Minimum number of records**: A node will only participate if it
  contains at least `n` records in the local dataset. This is to prevent nodes with very
  little data from participating in the computation. By default, the minimum number of 
  data rows is set to 3. Node administrators can change this minimum by adding the 
  following to their node configuration file:

  .. code:: yaml

    algorithm_env:
      T_TEST_MINIMUM_NUMBER_OF_RECORDS: 3

Data sharing
------------

The intermediate shared data for each data station is:

- **mean** of a numerical column;
- **number of observations**;
- **sample variance**.

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
      - It is potentially possible to single out a patient by selecting subgroups of patients.
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