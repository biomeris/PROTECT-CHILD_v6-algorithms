How to use
==========

Input arguments
---------------

- ``cohort_column`` (string, default ``"cohort"``): name of the column in each node's
  data that holds the cohort of each sample. Results are computed per cohort.
- ``min_samples`` (integer, default ``2``): minimum number of samples a cohort needs on
  a node to be included. Smaller cohorts are dropped on the node.
- ``idat_dir`` (optional): passed on to the nodes as ``arg1`` (currently unused).

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
        "cohort_column": "cohort",
        "min_samples": 2,
    },
    "output_format": "json"
  }

  my_task = client.task.create(
      collaboration=collaboration_id,
      organizations=organization_ids,
      name="globalIDAT",
      description="Global beta and m values",
      image="ghcr.io/vantage6/algorithm/globalIDAT",
      input_=input_,
      databases=[{"label": "default"}],
  )

  task_id = my_task.get("id")
  results = client.wait_for_results(task_id)