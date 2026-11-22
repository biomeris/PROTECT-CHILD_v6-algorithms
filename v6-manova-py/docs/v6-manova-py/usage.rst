How to use
==========

Input arguments
---------------

``group_column`` (str)
    Column defining the groups (the MANOVA factor). Any type; values are
    compared as strings, with integer-valued floats normalized (``1.0`` →
    ``"1"``).

``outcome_columns`` (list[str])
    The numeric dependent variables. At least two; for a single outcome use a
    one-way ANOVA.

``organizations_to_include`` (list[int], optional)
    Organizations to include. Defaults to every organization in the
    collaboration.

Rows with a missing group or outcome value are dropped at each node.

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
    "method": "manova",
    "arguments": {
        "group_column": "group",
        "outcome_columns": ["y1", "y2", "y3"],
        "organizations_to_include": None,  # all organizations
    },
    "output_format": "json"
  }

  my_task = client.task.create(
      collaboration=collaboration_id,
      organizations=organization_ids,
      name="v6-manova-py",
      description="Federated one-way MANOVA",
      image="v6-manova-py",
      input_=input_,
      databases=[{"label": "default"}],
  )

  task_id = my_task.get("id")
  results = client.wait_for_results(task_id)

Return value
------------

A dict with:

- ``tests`` - for each of ``"Wilks' lambda"``, ``"Pillai's trace"``,
  ``"Hotelling-Lawley trace"`` and ``"Roy's greatest root"``: ``value``,
  ``num_df``, ``den_df``, ``f_value`` and ``p_value``. ``None`` if no
  organization met the minimum number of records.
- ``n_total``, ``n_groups``, ``df_hypothesis`` (:math:`k-1`), ``df_error``
  (:math:`N-k`).
- ``group_sizes`` and ``group_means`` - pooled over the included
  organizations, keyed by group label.
- ``sscp_between`` and ``sscp_within`` - the :math:`B` and :math:`W` matrices,
  in the order of ``outcome_columns``.
- ``included_organizations`` / ``excluded_organizations`` - which
  organizations contributed, and which were excluded for having too few
  records.
- ``suppressed_groups`` - per organization ID, the groups left out locally for
  being too small.
- ``warnings`` - e.g. when only one organization contributed.
