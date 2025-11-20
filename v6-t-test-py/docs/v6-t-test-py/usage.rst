How to use
==========

Input arguments
---------------

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Argument
     - Type
     - Description
   * - ``organizations_to_include``
     - List of integers
     - Which organizations to include in the computation. These must be exactly 2.
   * - ``columns``
     - List of strings
     - The columns to compute the mean and sample variance for. The columns must be
     numeric. If not provided, all numeric columns are included.

Python client example
---------------------

To understand the information below, you should be familiar with the vantage6
framework. If you are not, please read the `documentation <https://docs.vantage6.ai>`_
first, especially the part about the
`Python client <https://docs.vantage6.ai/en/main/user/pyclient.html>`_.

.. code-block:: python

  from vantage6.client import Client

  server = 'http://localhost'
  port = 7601
  api_path = '/api'
  private_key = None
  username = 'root'
  password = 'password'
  collaboration_id = 1
  organizations_to_include = [1,2]

  # Create connection with the vantage6 server
  client = Client(server, port, api_path)
  client.setup_encryption(private_key)
  client.authenticate(username, password)

  input_ = {
    'method': 'central',
    'kwargs': {
        'organizations_to_include': organizations_to_include,
        'columns': ['age', 'Height'],
    },
    'output_format': 'json'
  }

  my_task = client.task.create(
      collaboration=collaboration_id,
      organizations=organization_ids,
      name='v6-t-test-py',
      description='Independent Samples t-test: This test compares the means of two independent groups to see if there is a significant difference between them.',
      image='v6-t-test-py',
      input_=input_,
      databases=[{"label": "default"}],
  )

  task_id = my_task.get('id')
  results = client.wait_for_results(task_id)