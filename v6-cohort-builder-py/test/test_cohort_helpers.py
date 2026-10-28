import importlib
import json

import pandas as pd
import pytest


cohort = importlib.import_module("v6-cohort-builder-py.cohort")


def test_validate_request_accepts_sql_and_generates_id():
    request = cohort.validate_cohort_request(
        cohort_name="Death cohort",
        sql="SELECT * FROM @cdm_database_schema.person;",
        cohort_id=None,
        atlas_json=None,
        overwrite=False,
        id_factory=lambda: 12345,
    )

    assert request.cohort_name == "Death cohort"
    assert request.sql == "SELECT * FROM @cdm_database_schema.person;"
    assert request.atlas_json is None
    assert request.input_type == "sql"
    assert request.cohort_id == 12345
    assert request.overwrite is False


def test_validate_request_accepts_atlas_json_dict():
    atlas_json = {"ConceptSets": [], "PrimaryCriteria": {"CriteriaList": []}}

    request = cohort.validate_cohort_request(
        cohort_name="Atlas cohort",
        sql=None,
        atlas_json=atlas_json,
        cohort_id=77,
        overwrite=True,
        id_factory=lambda: 12345,
    )

    assert request.sql is None
    assert json.loads(request.atlas_json) == atlas_json
    assert request.input_type == "atlas_json"
    assert request.cohort_id == 77
    assert request.overwrite is True


@pytest.mark.parametrize(
    "kwargs, message",
    [
        (
            {"cohort_name": "", "sql": "select 1", "atlas_json": None},
            "cohort_name is required",
        ),
        (
            {"cohort_name": "x", "sql": None, "atlas_json": None},
            "Provide exactly one",
        ),
        (
            {"cohort_name": "x", "sql": "select 1", "atlas_json": {}},
            "Provide exactly one",
        ),
        (
            {"cohort_name": "x", "sql": " ", "atlas_json": None},
            "sql must be a non-empty string",
        ),
        (
            {"cohort_name": "x", "sql": "select 1", "atlas_json": None, "cohort_id": 0},
            "cohort_id must be a positive integer",
        ),
        (
            {"cohort_name": "x", "sql": "select 1", "atlas_json": None, "cohort_id": True},
            "cohort_id must be a positive integer",
        ),
        (
            {"cohort_name": "x", "sql": "select 1", "atlas_json": None, "overwrite": "yes"},
            "overwrite must be a boolean",
        ),
        (
            {"cohort_name": "x", "sql": None, "atlas_json": "{"},
            "atlas_json must be valid JSON",
        ),
    ],
)
def test_validate_request_rejects_invalid_inputs(kwargs, message):
    defaults = {
        "cohort_id": None,
        "overwrite": False,
        "id_factory": lambda: 12345,
    }
    defaults.update(kwargs)

    with pytest.raises(cohort.CohortInputError, match=message):
        cohort.validate_cohort_request(**defaults)


def test_normalize_atlas_json_reports_non_serializable_values():
    with pytest.raises(cohort.CohortInputError, match="atlas_json must be valid JSON"):
        cohort.normalize_atlas_json({"ConceptSets": [object()]})


def _connection_details() -> dict:
    """What the @data_extraction decorator hands the function: DATABASE_URI and
    DATABASE_TYPE as uri/type, and every DB_PARAM_<NAME> as <NAME>."""
    return {
        "uri": "jdbc:postgresql://db:5432/omop",
        "type": "OMOP",
        "DBMS": "postgresql",
        "USER": "user",
        "PASSWORD": "secret",
        "CDM_DATABASE": "omop",
        "CDM_SCHEMA": "cdm",
        "RESULTS_SCHEMA": "results",
    }


def test_parse_omop_config_reads_the_connection_details():
    config = cohort.parse_omop_config(
        _connection_details(), {"ORGANIZATION_ID": "3", "NODE_ID": "9"}
    )

    assert config.uri == "jdbc:postgresql://db:5432/omop"
    assert config.database_type == "OMOP"
    assert config.dbms == "postgresql"
    assert config.user == "user"
    assert config.password == "secret"
    assert config.cdm_database == "omop"
    assert config.cdm_schema == "cdm"
    assert config.results_schema == "results"
    assert config.organization_id == 3
    assert config.node_id == 9


def test_parse_omop_config_reports_missing_keys():
    details = _connection_details()
    del details["PASSWORD"]

    with pytest.raises(cohort.NodeConfigError, match="configuration: PASSWORD$"):
        cohort.parse_omop_config(details, {})


def test_parse_omop_config_reports_invalid_optional_ids():
    with pytest.raises(cohort.NodeConfigError, match="ORGANIZATION_ID must be an integer"):
        cohort.parse_omop_config(_connection_details(), {"ORGANIZATION_ID": "abc"})


def test_extract_subject_count_from_cohort_counts_dataframe():
    df = pd.DataFrame(
        {
            "cohortDefinitionId": [55],
            "numberRecords": [88],
            "numberSubjects": [42],
        }
    )

    assert cohort.extract_subject_count(df, 55) == 42


def test_extract_subject_count_accepts_alternate_column_names():
    df = pd.DataFrame(
        {
            "cohortId": [55],
            "subjectCount": ["42"],
        }
    )

    assert cohort.extract_subject_count(df, 55) == 42


def test_extract_subject_count_accepts_ohdsi_cohort_generator_columns():
    df = pd.DataFrame(
        {
            "cohortId": [55.0],
            "cohortEntries": [88.0],
            "cohortSubjects": [42.0],
        }
    )

    assert cohort.extract_subject_count(df, 55) == 42


def test_extract_subject_count_wraps_invalid_cohort_id_values():
    df = pd.DataFrame(
        {
            "cohortDefinitionId": ["not-an-id"],
            "numberSubjects": [42],
        }
    )

    with pytest.raises(cohort.CohortInputError, match="cohort id"):
        cohort.extract_subject_count(df, 55)


def test_extract_subject_count_wraps_invalid_subject_count_values():
    df = pd.DataFrame(
        {
            "cohortDefinitionId": [55],
            "numberSubjects": ["not-a-count"],
        }
    )

    with pytest.raises(cohort.CohortInputError, match="subject count"):
        cohort.extract_subject_count(df, 55)


def test_aggregate_results_returns_total_only_when_all_nodes_succeed():
    response = cohort.aggregate_central_results(
        expected_org_ids=[1, 2],
        raw_results=[
            {
                "organization_id": 1,
                "status": "success",
                "count": 10,
                "database": "site_a",
                "cdm_schema": "cdm",
                "results_schema": "results",
            },
            {
                "organization_id": 2,
                "status": "success",
                "count": 12,
                "database": "site_b",
                "cdm_schema": "cdm",
                "results_schema": "results",
            },
        ],
    )

    assert response["total_count"] == 22
    assert response["total_range"] == [22, 22]
    assert response["errors"] == []
    assert response["nodes"] == [
        {
            "organization_id": 1,
            "status": "success",
            "count": 10,
            "database": "site_a",
            "cdm_schema": "cdm",
            "results_schema": "results",
        },
        {
            "organization_id": 2,
            "status": "success",
            "count": 12,
            "database": "site_b",
            "cdm_schema": "cdm",
            "results_schema": "results",
        },
    ]


def test_aggregate_results_reports_the_cohort_id_the_nodes_share():
    rows = [
        {"organization_id": 1, "status": "success", "cohort_id": 55, "count": 10},
        {"organization_id": 2, "status": "success", "cohort_id": 55, "count": 12},
    ]

    assert cohort.aggregate_central_results(
        expected_org_ids=[1, 2], raw_results=rows
    )["cohort_id"] == 55

    rows[1]["cohort_id"] = 56
    assert cohort.aggregate_central_results(
        expected_org_ids=[1, 2], raw_results=rows
    )["cohort_id"] is None


def test_aggregate_results_shows_the_cohort_id_each_node_used():
    response = cohort.aggregate_central_results(
        expected_org_ids=[1, 2],
        raw_results=[
            {"organization_id": 1, "status": "success", "cohort_id": 55, "count": 10},
            {"organization_id": 2, "status": "success", "cohort_id": 56, "count": 12},
        ],
    )

    assert [
        (node["organization_id"], node["cohort_id"]) for node in response["nodes"]
    ] == [(1, 55), (2, 56)]


def test_aggregate_results_keeps_partial_success_and_null_total():
    response = cohort.aggregate_central_results(
        expected_org_ids=[1, 2, 3],
        raw_results=[
            {"organization_id": 1, "status": "success", "count": 10},
            {"organization_id": 2, "status": "error", "message": "conflict"},
        ],
    )

    assert response["total_count"] is None
    assert response["nodes"] == [{"organization_id": 1, "status": "success", "count": 10}]
    assert response["errors"] == [
        {"organization_id": 2, "status": "error", "message": "conflict"},
        {"organization_id": 3, "status": "error", "message": "No result returned by organization 3"},
    ]


def test_aggregate_results_handles_error_without_organization_id():
    response = cohort.aggregate_central_results(
        expected_org_ids=[1],
        raw_results=[
            {
                "organization_id": None,
                "status": "error",
                "message": "missing password",
            }
        ],
    )

    assert response["nodes"] == []
    assert response["errors"] == [
        {"organization_id": None, "status": "error", "message": "missing password"},
        {
            "organization_id": 1,
            "status": "error",
            "message": "No result returned by organization 1",
        },
    ]
    assert response["total_count"] is None


def test_execute_cohort_generation_uses_ohdsi_wrappers_and_returns_count(monkeypatch):
    calls = []

    class FakeConnection:
        def execute(self, sql):
            calls.append(("execute", sql))
            return pd.DataFrame({"row_count": [0]})

    class FakeDatabaseConnector:
        def create_connection_details(self, **kwargs):
            calls.append(("create_connection_details", kwargs))
            return {"details": kwargs}

        def connect(self, connection_details):
            calls.append(("connect", connection_details))
            return FakeConnection()

        def disconnect(self, connection):
            calls.append(("disconnect", connection))

    class FakeCohortGenerator:
        def get_cohort_table_names(self):
            calls.append(("get_cohort_table_names",))
            return {"cohort": "cohort"}

        def create_cohort_tables(self, **kwargs):
            calls.append(("create_cohort_tables", kwargs))

        def generate_cohort_set(self, **kwargs):
            calls.append(("generate_cohort_set", kwargs))

        def get_cohort_counts(self, **kwargs):
            calls.append(("get_cohort_counts", kwargs))
            return "r-counts"

    class FakeCommon:
        def convert_to_r(self, value):
            calls.append(("convert_to_r", value))
            return "r-cohort-definition-set"

        def convert_from_r(self, value):
            calls.append(("convert_from_r", value))
            assert value == "r-counts"
            return pd.DataFrame(
                {
                    "cohortDefinitionId": [55],
                    "numberSubjects": [42],
                }
            )

    monkeypatch.setattr(
        cohort,
        "_load_ohdsi_modules",
        lambda: (FakeCommon(), FakeCohortGenerator(), FakeDatabaseConnector()),
    )
    config = cohort.OmopConfig(
        uri="jdbc:postgresql://db:5432/omop",
        database_type="OMOP",
        dbms="postgresql",
        user="user",
        password="secret",
        cdm_database="omop",
        cdm_schema="cdm",
        results_schema="results",
    )

    result = cohort.execute_cohort_generation(
        config=config,
        sql="select * from @cdm_database_schema.person",
        cohort_name="Death cohort",
        cohort_id=55,
        overwrite=False,
    )

    assert result == {
        "database": "omop",
        "cdm_schema": "cdm",
        "results_schema": "results",
        "count": 42,
    }
    assert calls[0] == (
        "create_connection_details",
        {
            "dbms": "postgresql",
            "connection_string": "jdbc:postgresql://db:5432/omop",
            "user": "user",
            "password": "secret",
        },
    )
    assert calls[1] == (
        "connect",
        {
            "details": {
                "dbms": "postgresql",
                "connection_string": "jdbc:postgresql://db:5432/omop",
                "user": "user",
                "password": "secret",
            }
        },
    )
    assert calls[2] == ("get_cohort_table_names",)
    assert calls[3][0] == "create_cohort_tables"
    assert calls[3][1]["connection"].__class__ is FakeConnection
    assert calls[3][1]["cohort_database_schema"] == "results"
    assert calls[3][1]["cohort_table_names"] == {"cohort": "cohort"}
    assert calls[4][0] == "execute"
    assert "results.cohort" in calls[4][1]
    assert "cohort_definition_id = 55" in calls[4][1]
    assert calls[5][0] == "convert_to_r"
    assert list(calls[5][1].columns) == ["cohortId", "cohortName", "sql"]
    assert calls[5][1].to_dict(orient="records") == [
        {
            "cohortId": 55,
            "cohortName": "Death cohort",
            "sql": "select * from @cdm_database_schema.person",
        }
    ]
    assert calls[6] == (
        "generate_cohort_set",
        {
            "connection_details": {
                "details": {
                    "dbms": "postgresql",
                    "connection_string": "jdbc:postgresql://db:5432/omop",
                    "user": "user",
                    "password": "secret",
                }
            },
            "cdm_database_schema": "cdm",
            "cohort_database_schema": "results",
            "cohort_table_names": {"cohort": "cohort"},
            "cohort_definition_set": "r-cohort-definition-set",
        },
    )
    assert calls[7] == (
        "get_cohort_counts",
        {
            "connection_details": {
                "details": {
                    "dbms": "postgresql",
                    "connection_string": "jdbc:postgresql://db:5432/omop",
                    "user": "user",
                    "password": "secret",
                }
            },
            "cohort_database_schema": "results",
            "cohort_table": "cohort",
            "cohort_ids": [55],
        },
    )
    assert calls[8] == ("convert_from_r", "r-counts")
    assert calls[9][0] == "disconnect"


def test_load_ohdsi_modules_uses_python_ohdsi_common(monkeypatch):
    # The algorithm-ohdsi-base image carries FeatureExtraction, which
    # ohdsi.common loads on import, so the package's own converters are used.
    import sys
    import types

    package = types.ModuleType("ohdsi")
    package.__path__ = []
    fakes = {}
    for name in ("common", "cohort_generator", "database_connector"):
        fakes[name] = types.ModuleType(f"ohdsi.{name}")
        setattr(package, name, fakes[name])
        monkeypatch.setitem(sys.modules, f"ohdsi.{name}", fakes[name])
    monkeypatch.setitem(sys.modules, "ohdsi", package)

    common, cohort_generator, database_connector = cohort._load_ohdsi_modules()

    assert common is fakes["common"]
    assert cohort_generator is fakes["cohort_generator"]
    assert database_connector is fakes["database_connector"]


def test_minimum_count_defaults_to_five():
    assert cohort.minimum_count({}) == 5


def test_minimum_count_reads_the_node_setting():
    assert cohort.minimum_count({"COHORT_BUILDER_MINIMUM_COUNT": "10"}) == 10


@pytest.mark.parametrize("value", ["abc", "0", "-3"])
def test_minimum_count_rejects_invalid_node_settings(value):
    with pytest.raises(cohort.NodeConfigError, match="COHORT_BUILDER_MINIMUM_COUNT"):
        cohort.minimum_count({"COHORT_BUILDER_MINIMUM_COUNT": value})


@pytest.mark.parametrize("count", [1, 4])
def test_withhold_small_count_hides_counts_below_the_minimum(count):
    row = cohort.withhold_small_count({"status": "success", "count": count}, minimum=5)

    assert row["count"] is None
    assert row["below_minimum"] is True
    assert row["minimum_count"] == 5


@pytest.mark.parametrize("count", [0, 5, 42])
def test_withhold_small_count_shares_zero_and_counts_at_the_minimum(count):
    row = cohort.withhold_small_count({"status": "success", "count": count}, minimum=5)

    assert row["count"] == count
    assert row["below_minimum"] is False


def test_withhold_small_count_keeps_a_count_already_withheld():
    row = cohort.withhold_small_count(
        {"status": "success", "count": None, "below_minimum": True}, minimum=5
    )

    assert row["count"] is None
    assert row["below_minimum"] is True


def test_sql_is_not_allowed_by_default():
    assert cohort.sql_allowed({}) is False


@pytest.mark.parametrize("value", ["true", "True", "1", "yes"])
def test_sql_is_allowed_when_the_node_opts_in(value):
    assert cohort.sql_allowed({"COHORT_BUILDER_ALLOW_SQL": value}) is True


@pytest.mark.parametrize("value", ["false", "0", "no"])
def test_sql_stays_off_when_the_node_says_no(value):
    assert cohort.sql_allowed({"COHORT_BUILDER_ALLOW_SQL": value}) is False


def test_sql_allowed_rejects_an_unclear_node_setting():
    with pytest.raises(cohort.NodeConfigError, match="COHORT_BUILDER_ALLOW_SQL"):
        cohort.sql_allowed({"COHORT_BUILDER_ALLOW_SQL": "maybe"})
