How to use
==========

Input arguments
---------------

.. describe the input arguments:
.. ['arg1']

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
        "arg1": "my_value",
    },
    "output_format": "json"
  }

  my_task = client.task.create(
      collaboration=collaboration_id,
      organizations=organization_ids,
      name="preprocessIDAT",
      description="Preprocessing of IDAT files to obtain beta-values",
      image="ghcr.io/vantage6/algorithm/preprocessIDAT",
      input_=input_,
      databases=[{"label": "default"}],
  )

  task_id = my_task.get("id")
  results = client.wait_for_results(task_id)