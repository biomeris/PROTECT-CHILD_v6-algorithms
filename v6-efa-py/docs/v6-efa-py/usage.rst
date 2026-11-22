How to use
==========

Input arguments
---------------

``columns`` (list[str])
    The numeric variables to factor-analyse. At least 3.

``n_factors`` (int, optional)
    Number of factors, from 1 to ``len(columns) - 1``. Defaults to the Kaiser
    criterion (number of eigenvalues of :math:`R` greater than 1). Check the
    returned ``eigenvalues`` (scree) before settling on a number.

``method`` (str)
    ``"minres"`` (default), ``"ml"`` or ``"principal"``.

``rotation`` (str or None)
    ``"promax"`` (default), ``"oblimin"``, ``"varimax"`` or ``None`` /
    ``"none"``. Promax and oblimin let factors correlate; varimax keeps them
    uncorrelated.

``organizations_to_include`` (list[int], optional)
    Organizations to include. Defaults to every organization in the
    collaboration.

Rows with a missing value in any selected column are dropped at each node.

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
    "method": "efa",
    "arguments": {
        "columns": ["x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8"],
        "n_factors": 2,
        "method": "minres",
        "rotation": "promax",
    },
    "output_format": "json"
  }

  my_task = client.task.create(
      collaboration=collaboration_id,
      organizations=organization_ids,
      name="v6-efa-py",
      description="Federated exploratory factor analysis",
      image="v6-efa-py",
      input_=input_,
      databases=[{"label": "default"}],
  )

  task_id = my_task.get("id")
  results = client.wait_for_results(task_id)

Return value
------------

A dict with:

- ``loadings`` - per column, its loadings on ``factor_names`` (``F1``,
  ``F2``, ...). For oblique rotations these are the pattern loadings. ``None``
  if no organization met the minimum number of records.
- ``structure`` and ``factor_correlations`` - the structure matrix and
  :math:`\Phi` for oblique rotations, otherwise ``None``.
- ``communalities`` and ``uniquenesses`` - per column.
- ``variance`` - ``ss_loadings``, ``proportion`` and ``cumulative`` per
  factor.
- ``eigenvalues`` - of the correlation matrix, in decreasing order.
- ``kmo`` - ``overall`` and ``per_item``.
- ``bartlett`` - ``statistic``, ``df`` and ``p_value``.
- ``correlation_matrix`` - the pooled :math:`R`, in the order of ``columns``.
- ``n_total``, ``n_factors``, ``n_factors_source`` (``"user"`` or
  ``"kaiser"``), ``method``, ``rotation``, ``converged``.
- ``included_organizations`` / ``excluded_organizations``.
- ``warnings`` - e.g. low KMO, a Heywood case, or non-convergence.
