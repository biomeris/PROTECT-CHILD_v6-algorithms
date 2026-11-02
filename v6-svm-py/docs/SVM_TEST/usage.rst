How to use
==========

Input arguments
---------------

.. describe the input arguments:
.. ['features', 'outcome_column', 'classes', 'reference_cohort', 'C_grid',
..  'n_folds', 'max_rounds',
..  'tolerance', 'random_seed', 'min_samples', 'run_loho', 'stability_top_k']

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
    "method": "central",
    "arguments": {
        # CpG list, e.g. the output of the Elastic Net step
        "features": ["cg80019459", "cg20259425", "cg68756138"],
        # Column with the cohort label of each sample. The cohorts are
        # detected automatically; pass "classes" to restrict them.
        "outcome_column": "cohort",
        # Reference group (e.g. controls): the pattern is then reported
        # for the other cohort(s) relative to it.
        "reference_cohort": "no_rejection",
    },
    "output_format": "json"
  }

  my_task = client.task.create(
      collaboration=collaboration_id,
      organizations=organization_ids,
      name="SVM_TEST",
      description="svm algorithm for episignature discovery",
      image="ghcr.io/vantage6/algorithm/svm_test",
      input_=input_,
      databases=[{"label": "default"}],
  )

  task_id = my_task.get("id")
  results = client.wait_for_results(task_id)