How to use
==========

Input arguments
---------------

.. describe the input arguments:
.. ['organizations_to_include', 'group_column', 'outcome_column', 'alternative']

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
        "organizations_to_include": "my_value",
        "group_column": "my_value",
        "outcome_column": "my_value",
        "alternative": "my_value",
    },
    "output_format": "json"
  }

  my_task = client.task.create(
      collaboration=collaboration_id,
      organizations=organization_ids,
      name="v6-fisher-exact-test-py",
      description="Fisher's exact test to evaluate the significance of associations between two categorical variables in a 2×2 contingency table",
      image="v6-fisher-exact-test-py",
      input_=input_,
      databases=[{"label": "default"}],
  )

  task_id = my_task.get("id")
  results = client.wait_for_results(task_id)