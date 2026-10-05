How to use
==========

Input arguments
---------------

- ``p_values`` (``dict[str, float]`` or ``list[float]``, required): the p-values
  to correct. Prefer a dict keyed by a label for the hypothesis/variable being
  tested, so the correction results stay traceable to their source.
- ``alpha`` (``float``, default ``0.05``): the family-wise significance level.
- ``num_tests`` (``int``, optional): the number of tests ``m`` to correct for.
  Defaults to ``len(p_values)``. Must be an integer ``>= len(p_values)``.

Python client example
---------------------

To understand the information below, you should be familiar with the vantage6
framework. If you are not, please read the `documentation <https://docs.vantage6.ai>`_
first, especially the part about the
`Python client <https://docs.vantage6.ai/en/main/user/pyclient.html>`_.

This example chains the correction onto the output of
``v6-fisher-exact-test-py``, but any source of p-values works the same way.

.. code-block:: python

  from vantage6.client import Client

  server_url = "http://localhost:7601/api"
  auth_url = "http://localhost:8080"
  collaboration_id = 1
  organization_ids = [2]

  # Create connection with the vantage6 server
  client = Client(server_url, auth_url)
  client.authenticate()

  # Suppose we already ran v6-fisher-exact-test-py once per outcome variable
  # and collected the p-values ourselves:
  p_values = {
      "outcome_a": 0.01,
      "outcome_b": 0.20,
      "outcome_c": 0.001,
  }

  input_ = {
      "method": "compute_bonferroni_correction",
      "arguments": {
          "p_values": p_values,
          "alpha": 0.05,
      },
      "output_format": "json",
  }

  my_task = client.task.create(
      collaboration=collaboration_id,
      organizations=organization_ids,
      name="v6-bonferroni-py",
      description="Bonferroni correction for multiple hypothesis testing",
      image="v6-bonferroni-py",
      input_=input_,
  )

  task_id = my_task.get("id")
  results = client.wait_for_results(task_id)

Because this algorithm performs no node-side computation, it can be run against
a single organization - there is no need to request it from every organization
in the collaboration, and no database needs to be attached to the task.
