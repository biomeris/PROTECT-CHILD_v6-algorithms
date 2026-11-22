How to use
==========

Input arguments
---------------

``group_column`` (str)
    Column defining the groups. Values are compared as strings, with
    integer-valued floats normalized (``1.0`` → ``"1"``).

``column`` (str)
    The numeric column whose ranks are compared.

``p_adjust`` (str)
    ``"bonferroni"`` (default, as in Dunn, 1964), ``"holm"``, ``"fdr_bh"``
    (Benjamini-Hochberg) or ``"none"``.

``alpha`` (float)
    Significance level for the ``reject`` flags. Defaults to 0.05.

``organizations_to_include`` (list[int], optional)
    Organizations to include. Defaults to every organization in the
    collaboration.

Rows with a missing group or value are dropped at each node.

Python client example
---------------------

To understand the information below, you should be familiar with the vantage6
framework. If you are not, please read the `documentation <https://docs.vantage6.ai>`_
first, especially the part about the
`Python client <https://docs.vantage6.ai/en/main/user/pyclient.html>`_.

.. code-block:: python

  from vantage6.client import Client

  server_url = "http://localhost:7601/api"
  auth_url = "http://localhost:8080"
  collaboration_id = 1
  organization_ids = [2]  # the organization that runs the central function

  # Create connection with the vantage6 server
  client = Client(server_url, auth_url)
  client.authenticate()

  input_ = {
    "method": "dunn",
    "arguments": {
        "group_column": "group",
        "column": "score",
        "p_adjust": "holm",
    },
    "output_format": "json"
  }

  my_task = client.task.create(
      collaboration=collaboration_id,
      organizations=organization_ids,
      name="v6-dunn-py",
      description="Federated Dunn's post-hoc test",
      image="v6-dunn-py",
      input_=input_,
      databases=[{"label": "default"}],
  )

  task_id = my_task.get("id")
  results = client.wait_for_results(task_id)

Return value
------------

A dict with:

- ``comparisons`` - one entry per pair of groups: ``group_1``, ``group_2``,
  ``mean_rank_difference``, ``z``, ``p_value``, ``p_adjusted`` and
  ``reject``. ``None`` if no organization met the minimum number of records.
- ``kruskal_wallis`` - ``h_statistic``, ``df`` and ``p_value``.
- ``group_sizes`` and ``mean_ranks`` - per group.
- ``n_total``, ``n_groups``, ``n_rank_bins`` (the resolution of the
  approximate ranks).
- ``included_organizations`` / ``excluded_organizations`` and
  ``suppressed_groups`` (per organization ID, the groups left out locally for
  being too small).
- ``warnings`` - e.g. when fewer than 10 rank bins could be formed.

Because the ranks are binned, read the p-values as close approximations at
hundreds of records and as indicative at tens of records (see
:doc:`validation`).
