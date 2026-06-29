"""
This file contains all data extraction algorithm functions.

Data extraction functions can be used to extract data from the databases present at a
vantage6 node. The extracted data are stored at the node and can then be used in
subsequent analyses.
"""
import os
from typing import Any
import pandas as pd

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.action import data_extraction

@data_extraction
def data_extraction_function(
    connection_details: dict, arg1
) -> Any:
    """ This function extracts data from ... to the vantage6 node """
    # TODO this is a simple example to show you how to write a data extraction function.
    # Replace it by your own code.
    database_uri = connection_details["uri"]

    # Example for using environment variables:
    # if the node configuration looks like this:
    # databases:
    #   serviceBased:
    #     - name: my_postgres_db
    #       uri: postgresql://postgres:password@localhost:5432/my_postgres_db
    #       type: other
    #       env:
    #         USER: postgres
    #         PASSWORD: password
    # then you can retrieve the environment variables:
    user = os.getenv("user")
    password = os.getenv("password")

    # Example for using the database URI:
    df = pd.read_csv(database_uri)

    # or, if you e.g. have a SQL database, maybe something like this:
    # df = pd.read_sql_query(
    #     "SELECT * FROM my_table", database_uri, user=user, password=password
    # )

    # Return results to the vantage6 server.
    return df

# TODO Feel free to add more data extraction functions here.