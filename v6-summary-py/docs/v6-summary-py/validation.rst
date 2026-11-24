Validation
==========

A `test script <https://github.com/vantage6/v6-summary-py/blob/main/test/test.py>`_ is
available in the ``test`` directory. Install dev dependencies and run pytest:

.. code-block:: bash

    uv sync --group dev
    uv run pytest test/test.py -v

The tests use the vantage6 ``MockNetwork`` to simulate a multi-node collaboration with
in-memory dataframes.
