Privacy
=======

The algorithm builds a cohort inside each node's OMOP CDM database and shares
only how many subjects it holds. The cohort itself (person ids and dates) is
written to the ``cohort`` table of the node's results schema and stays there.

.. _privacy-guards:

Guards
------

- **Minimum count**: a count from 1 up to the node's minimum is never shared.
  ``generate_cohort`` withholds it before it reaches the result row or the
  algorithm log, so it does not leave the node even when the node shares its
  algorithm logs. The row then holds ``count: null``, ``below_minimum: true``
  and the minimum used. A count of zero is shared. The default minimum is 5.
  Node administrators can set their own by adding the following to their node
  configuration file:

  .. code:: yaml

    algorithm_env:
      COHORT_BUILDER_MINIMUM_COUNT: 5

  If the value is not a positive integer, the node refuses to generate the
  cohort and shares no count.

- **No requester SQL by default**: the ``sql`` input runs the requester's own
  SQL, which can make the count reveal any value (see *Reconstruction*
  below). Nodes refuse it unless their administrator turns it on:

  .. code:: yaml

    algorithm_env:
      COHORT_BUILDER_ALLOW_SQL: true

  ``atlas_json`` is always accepted: CirceR writes the SQL from it.

- **Only the cohort result leaves the node**: ``cohort_count`` can be pointed
  at any dataframe in the session. It returns only the fields of a cohort
  result (listed below), refuses a dataframe that is not one, and applies the
  node's minimum again.

- **Totals respect withheld counts**: ``central_function`` only sees the rows
  the nodes return, so it never learns a withheld count. It gives an exact
  ``total_count`` only when no node withheld its count; otherwise it gives
  ``total_range``, the lowest and highest possible total.

Data sharing
------------

The requester sends each node a cohort definition: an ATLAS cohort expression
or OHDSI CohortGenerator SQL, with a cohort name and id.

Each node returns one row:

- ``status`` (``success`` or ``error``) and, on failure, ``message``;
- ``count``, or ``null`` with ``below_minimum: true`` and ``minimum_count``;
- ``cohort_id``, ``cohort_name`` and ``input_type`` (``sql`` or
  ``atlas_json``), as sent;
- ``organization_id`` and ``node_id``;
- ``database``, ``cdm_schema`` and ``results_schema``: the names of the
  node's database and schemas.

Error messages come from the request validation, R or the database. They
describe the failure, not patient records, but a database error can quote
part of the query that failed.

``central_function`` returns these rows, the errors, and ``total_count`` or
``total_range``.

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
      - Possible only at nodes that set ``COHORT_BUILDER_ALLOW_SQL: true``.
        The ``sql`` input runs the requester's SQL with the node's database
        credentials, so a query can insert into the cohort table as many rows
        as a value it wants to learn (for example one patient's year of
        birth), and the count then reveals that value, above any minimum. The
        ATLAS input does not give the requester this control: CirceR writes
        the SQL from the concept sets and criteria. Whatever the setting,
        node administrators should give the algorithm's database user
        read-only access to the CDM schema and write access only to the
        results schema.
    * - Differencing
      - ❌
      - Possible by requesting two cohorts that differ in one criterion, or
        the same cohort before and after the data is updated, and subtracting
        the counts. The minimum count only hides counts from 1 to the minimum
        at each node; it does not stop this.
    * - Deep Leakage from Gradients (DLG)
      - ✔
      - No model is trained and no gradient is shared.
    * - Generative Adversarial Networks (GAN)
      - ✔
      - No model is trained.
    * - Model Inversion
      - ✔
      - No model is trained.
    * - Watermark Attack
      - ✔
      - No model is trained.
