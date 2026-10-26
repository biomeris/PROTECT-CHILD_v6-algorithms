"""
This file contains all data extraction algorithm functions.

Data extraction functions can be used to extract data from the databases present at a
vantage6 node. The extracted data are stored at the node and can then be used in
subsequent analyses.
"""

import traceback
from typing import Any
from jinja2 import Template
import os
from importlib.resources import as_file, files

from ohdsi import database_connector, common
from vantage6.algorithm.tools.util import info, error
from vantage6.algorithm.decorator.action import data_extraction


@data_extraction
def data_extraction_function(connection_details: dict) -> Any:
    info("Setting up connection to database")

    info(f'DBMS: {connection_details.get("DBMS")}')
    info(f'URI: {connection_details["uri"]}')
    info(f'CDM_DATABASE: {connection_details.get("CDM_DATABASE")}')
    info(f'USER: {connection_details.get("USER")}')

    connection = database_connector.connect(
        dbms=connection_details.get("DBMS"),
        connection_string=connection_details["uri"],
        user=connection_details.get("USER"),
        password=connection_details.get("PASSWORD"),
    )

    info(f"Loading SQL file: test-query.sql.j2")
    ref = files("v6-test-omop").joinpath("sql", "test-query.sql.j2")
    try:
        with as_file(ref) as sql_path:
            sql_template = Template(open(sql_path).read())
    except Exception as e:
        error(f"Failed to read SQL file: {e}")
        traceback.print_exc()
        raise e

    info("Loading environment variables")
    cdm_schema = connection_details.get("CDM_SCHEMA")

    info("Rendering SQL template")
    rendered_sql = sql_template.render(
        cdm_schema=cdm_schema,
    )

    info("Executing SQL")
    try:
        df = database_connector.query_sql(connection, rendered_sql)
        df = common.convert_from_r(df)
        for col in df.columns:
            df[col] = df[col].apply(
                lambda x: None if type(x).__name__.startswith("NA") else x
            )
    except Exception as e:
        error(f"Failed to execute SQL: {e}")
        traceback.print_exc()
        with open("errorReportSql.txt", "r") as f:
            error(f.read())
        raise e

    # Return results to the vantage6 server.
    return df
