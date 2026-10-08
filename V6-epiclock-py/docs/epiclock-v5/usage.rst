How to use
==========

Input arguments
---------------

- ``lista_relojes`` (list of strings, optional): pyaging clocks to calculate. If not
  given, the default clocks are used: ``horvath2013``, ``hannum``, ``pcphenoage``
  and ``pedbe``. Any other pyaging clock can be chosen, e.g. ``dnamphenoage``,
  ``zhangen``, ``weidner``, ``lin``, ``bocklandt`` or ``corticalclock``.
- ``cohort_column`` (string, default ``"cohort"``): name of the column in each node's
  data that holds the cohort of each sample. Results are computed per cohort.
- ``min_samples`` (integer, default ``3``, at least 2): minimum number of samples a
  cohort needs on a node to be included. Smaller cohorts are dropped on the node.
- ``coverage_threshold`` (float, default ``0.9``): minimum coverage (share of a
  clock's CpGs present in a node's data) for a result to be flagged ``coverage_ok``.
  EPIC v2 identifiers are converted automatically. It only sets the flag; no clock
  or result is removed. Every result reports ``coverage_min`` and ``coverage_max``
  over the contributing nodes.

EPIC v2 data: suffixes are stripped, replicate probes averaged and CpGs renamed in
EPIC v2 mapped to their 450K / EPIC v1 identifier automatically, using the mapping
packaged with the algorithm. ``MANIFEST_PATH`` (a newer manifest) overrides it;
``EPICLOCK_LEGACY_MAP=none`` switches it off.

Python client example
---------------------

To understand the information below, you should be familiar with the vantage6
framework. If you are not, please read the `documentation <https://docs.vantage6.ai>`_
first, especially the part about the
`Python client <https://docs.vantage6.ai/en/main/user/pyclient.html>`_.

.. TODO Update the code below and explain input

.. TODO Optionally/alternatively, explain how to run via the vantage6 UI

.. code-block:: python

  from vantage6.client import Client

  server_url = "http://localhost:7601/api"
  auth_url = "http://localhost:8080"
  collaboration_id = 1
  organization_ids = [2]

  # Create connection with the vantage6 server
  client = Client(server_url, auth_url)
  client.authenticate()

  input_ = {
    "method": "central_function",
    "arguments": {
        "lista_relojes": ["horvath2013", "hannum", "pcphenoage"],
        "cohort_column": "cohort",
        "min_samples": 3,
    },
    "output_format": "json"
  }

  my_task = client.task.create(
      collaboration=collaboration_id,
      organizations=organization_ids,
      name="epiclock-v5",
      description="estimation of DNAm age",
      image="ghcr.io/vantage6/algorithm/epiclock-v5",
      input_=input_,
      databases=[{"label": "default"}],
  )

  task_id = my_task.get("id")
  results = client.wait_for_results(task_id)