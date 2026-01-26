How to use
==========

Input arguments
---------------

.. list-table::
   :widths: 20 20 60
   :header-rows: 1

   * - Argument
     - Type
     - Description
   * - ``columns``
     - List of column names (strings)
     - The columns for which to get a data summary. If not provided, all columns will
       be used.
   * - ``numeric_columns``
     - List of column names (strings)
     - Indicate which of the columns are numeric. If not provided, the algorithm
       will infer the type of the columns.
   * - ``organizations_to_include``
     - List of integers
     - Which organizations to include in the computation.

Python client example
---------------------

To understand the information below, you should be familiar with the vantage6
framework. If you are not, please read the `documentation <https://docs.vantage6.ai>`_
first, especially the part about the
`Python client <https://docs.vantage6.ai/en/main/user/pyclient.html>`_.

.. code-block:: python

  from vantage6.client import Client

  server = 'http://localhost'
  port = 5000
  api_path = '/api'
  private_key = None
  username = 'root'
  password = 'password'

  # Create connection with the vantage6 server
  client = Client(server, port, api_path)
  client.setup_encryption(private_key)
  client.authenticate(username, password)

  input_ = {
    'method': 'summary',
    'kwargs': {
        'columns': ["age", "isOverweight"],
    }
  }

  my_task = client.task.create(
      collaboration=1,
      organizations=[1],
      name='Compute data summary',
      description='Create a data summary',
      image='',
      input=input_,
      databases=[
          {'label': 'default'}
      ]
  )

  task_id = my_task.get('id')
  results = client.wait_for_results(task_id)