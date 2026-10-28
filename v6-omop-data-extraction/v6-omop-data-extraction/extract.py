import traceback
from typing import Any
from jinja2 import Template
from importlib.resources import as_file, files

import numpy as np
import pandas as pd
import pyarrow as pa

from ohdsi import database_connector, common
from rpy2.rinterface_lib.sexp import (
    NACharacterType,
    NAComplexType,
    NAIntegerType,
    NALogicalType,
    NARealType,
)
from .converters import (
    to_boolean as _to_boolean,
    # to_category as _to_category,
    to_int64 as _to_int64,
)
from vantage6.algorithm.tools.util import info, error
from vantage6.algorithm.decorator.action import data_extraction

_R_NA_TYPES = (NACharacterType, NAComplexType, NAIntegerType, NALogicalType, NARealType)


@data_extraction
def cohort_data_extraction(connection_details: dict, cohort_id: int) -> Any:
    """
    Extract data for patients belonging to a specified cohort from the local
    OMOP Common Data Model (CDM) database.

    This function serves as a data extraction step within a federated analytics
    workflow. It connects to the local OMOP database, retrieves records associated
    with the selected cohort, and stores the resulting dataset at the node. The
    extracted data can subsequently be used by analytics algorithms executed within
    the same federated environment.

    Parameters
    ----------
    connection_details : dict
        Database connection configuration, including credentials and schema
        information required to access the OMOP CDM instance.
    cohort_id : int
        Identifier of the cohort whose patient data should be extracted.

    Returns
    -------
    pd.DataFrame
        A dataframe containing data for patients included in the specified cohort.
    """

    info("Setting up connection to database")

    connection = database_connector.connect(
        dbms=connection_details.get("DBMS"),
        connection_string=connection_details["uri"],
        user=connection_details.get("USER"),
        password=connection_details.get("PASSWORD"),
    )

    info(f"Retrieving variables for cohort: {cohort_id}")

    info(f"Loading SQL file: query_demo.sql.j2")
    ref = files("v6-omop-data-extraction").joinpath("sql", "query_demo.sql.j2")
    try:
        with as_file(ref) as sql_path:
            sql_template = Template(open(sql_path).read())
    except Exception as e:
        error(f"Failed to read SQL file: {e}")
        traceback.print_exc()
        raise e

    info("Loading environment variables")
    cdm_schema = connection_details.get("CDM_SCHEMA")
    results_schema = connection_details.get("RESULTS_SCHEMA")

    info("Rendering SQL template")
    rendered_sql = sql_template.render(
        cdm_schema=cdm_schema,
        results_schema=results_schema,
        cohort_id=cohort_id,
    )

    info("Executing SQL")
    try:
        df = database_connector.query_sql(connection, rendered_sql)
    except Exception as e:
        error(f"Failed to execute SQL: {e}")
        traceback.print_exc()
        with open("errorReportSql.txt", "r") as f:
            error(f.read())
        raise e

    info("Converting dataframe to pandas")
    try:
        converted_df = common.convert_from_r(df)
    except Exception as e:
        error(f"Failed to convert dataframe: {e}")
        traceback.print_exc()
        raise e

    converted_df = converted_df.applymap(
        lambda val: np.nan if isinstance(val, _R_NA_TYPES) else val
    )

    # Eventuale check su duplicati (vedi IDEA4RC)

    # Remove the patient_id column.
    converted_df = converted_df.drop(columns=["person_id"])
    info("Removed patient_id column from dataframe")

    # info(f"Converting column types")
    converted_df = convert_columns(converted_df)

    # Return results to the vantage6 server.
    return pa.Table.from_pandas(converted_df)


def convert_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function converts the base columns of the dataframe to the correct types.
    """

    # df = _to_category(
    #     df,
    #     [
    #         "sex",
    #         "concomitant_disease_1",
    #         "concomitant_disease_2",
    #         "concomitant_disease_3",
    #         "concomitant_disease_4",
    #         "concomitant_disease_5",
    #         "clinical_event",
    #     ],
    # )

    df = _to_int64(
        df,
        [
            "year_of_birth",
        ],
    )

    df = _to_boolean(
        df,
        [
            "concomitant_disease_yn",
            "clinical_event_yn",
        ],
    )

    return df
