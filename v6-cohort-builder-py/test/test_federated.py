"""Tests for the vantage6 5 entry points.

Cohort generation now runs as a data-extraction step (extract.py) because
that is the only step vantage6 5 gives database access to; the federated step
(federated.py) only reads the resulting row back. Both are exercised here with
the OHDSI calls stubbed out, so the tests need neither R nor a database.
"""

import importlib

import pandas as pd
import pytest


cohort = importlib.import_module("v6-cohort-builder-py.cohort")
extract = importlib.import_module("v6-cohort-builder-py.extract")
federated = importlib.import_module("v6-cohort-builder-py.federated")


@pytest.fixture(autouse=True)
def _allow_sql(monkeypatch):
    """Most tests here send SQL; nodes accept it only when they opt in."""
    monkeypatch.setenv("COHORT_BUILDER_ALLOW_SQL", "true")


def undecorated(func):
    """The vantage6 decorators validate FUNCTION_ACTION and the node's
    environment; the tests want the function underneath them."""
    return getattr(func, "__wrapped__", func)


# What the @data_extraction decorator passes as connection_details.
CONNECTION_DETAILS = {
    "uri": "jdbc:postgresql://db:5432/omop",
    "type": "OMOP",
    "DBMS": "postgresql",
    "USER": "omop_admin",
    "PASSWORD": "change-me-before-deploying",
    "CDM_DATABASE": "omop_cdm",
    "CDM_SCHEMA": "cdm",
    "RESULTS_SCHEMA": "results",
}


def _config(connection_details=None, environ=None) -> "cohort.OmopConfig":
    return cohort.OmopConfig(
        uri="jdbc:postgresql://db:5432/omop",
        database_type="OMOP",
        dbms="postgresql",
        user="omop_admin",
        password="change-me-before-deploying",
        cdm_database="omop_cdm",
        cdm_schema="cdm",
        results_schema="results",
        organization_id=3,
        node_id=9,
    )


def test_generate_cohort_returns_success_row(monkeypatch):
    monkeypatch.setattr(extract, "parse_omop_config", _config)
    monkeypatch.setattr(
        extract,
        "execute_cohort_generation",
        lambda config, sql, cohort_name, cohort_id, overwrite: {
            "database": config.cdm_database,
            "cdm_schema": config.cdm_schema,
            "results_schema": config.results_schema,
            "count": 42,
        },
    )

    row = extract._run_generate_cohort(
        CONNECTION_DETAILS,
        sql="select * from cohort_sql",
        cohort_name="Death cohort",
        cohort_id=55,
        overwrite=True,
    )

    assert row == {
        "organization_id": 3,
        "node_id": 9,
        "status": "success",
        "cohort_id": 55,
        "cohort_name": "Death cohort",
        "input_type": "sql",
        "database": "omop_cdm",
        "cdm_schema": "cdm",
        "results_schema": "results",
        "count": 42,
        "message": None,
        "below_minimum": False,
        "minimum_count": 5,
    }


def test_generate_cohort_converts_atlas_json_before_generating(monkeypatch):
    seen = {}
    monkeypatch.setattr(extract, "parse_omop_config", _config)
    monkeypatch.setattr(extract, "atlas_json_to_sql", lambda atlas: "SELECT 1 -- from atlas")

    def fake_execute(config, sql, cohort_name, cohort_id, overwrite):
        seen["sql"] = sql
        return {"count": 7}

    monkeypatch.setattr(extract, "execute_cohort_generation", fake_execute)

    row = extract._run_generate_cohort(
        CONNECTION_DETAILS,
        cohort_name="Atlas cohort",
        atlas_json={"ConceptSets": []},
        cohort_id=1,
    )

    assert seen["sql"] == "SELECT 1 -- from atlas"
    assert row["input_type"] == "atlas_json"
    assert row["count"] == 7


def test_generate_cohort_reports_config_errors_in_the_row(monkeypatch):
    monkeypatch.setattr(
        extract,
        "parse_omop_config",
        lambda *_: (_ for _ in ()).throw(cohort.NodeConfigError("missing password")),
    )

    row = extract._run_generate_cohort(
        CONNECTION_DETAILS, sql="select 1", cohort_name="Death cohort", cohort_id=55
    )

    # An extraction step that raises leaves no dataframe behind, so the
    # failure has to travel inside the row.
    assert row["status"] == "error"
    assert row["cohort_id"] == 55
    assert "missing password" in row["message"]
    assert row["organization_id"] is None


def test_generate_cohort_reports_execution_errors_in_the_row(monkeypatch):
    monkeypatch.setattr(extract, "parse_omop_config", _config)
    monkeypatch.setattr(
        extract,
        "execute_cohort_generation",
        lambda **_: (_ for _ in ()).throw(RuntimeError("cohort already exists")),
    )

    row = extract._run_generate_cohort(
        CONNECTION_DETAILS, sql="select 1", cohort_name="Death cohort", cohort_id=55
    )

    assert row["status"] == "error"
    assert row["organization_id"] == 3
    assert "cohort already exists" in row["message"]


def test_generate_cohort_rejects_invalid_requests_in_the_row(monkeypatch):
    monkeypatch.setattr(extract, "parse_omop_config", _config)

    row = extract._run_generate_cohort(
        CONNECTION_DETAILS, cohort_name="", sql="select 1", cohort_id=1
    )

    assert row["status"] == "error"
    assert "cohort_name" in row["message"]


def test_generate_cohort_wraps_the_row_in_a_dataframe(monkeypatch):
    seen = {}

    def fake_run(connection_details, **kw):
        seen["connection_details"] = connection_details
        return {"status": "success", "count": 3, "cohort_id": kw["cohort_id"]}

    monkeypatch.setattr(extract, "_run_generate_cohort", fake_run)

    frame = undecorated(extract.generate_cohort)(
        CONNECTION_DETAILS, cohort_name="c", sql="select 1", cohort_id=9
    )

    assert seen["connection_details"] is CONNECTION_DETAILS
    assert isinstance(frame, pd.DataFrame)
    assert len(frame) == 1
    assert frame.iloc[0]["count"] == 3


def test_generate_cohort_connects_with_the_connection_details_it_is_given(monkeypatch):
    # The node also exports these as DB_PARAM_* variables; they must not be
    # what the algorithm reads.
    for name in ("DATABASE_URI", "DATABASE_TYPE", "DB_PARAM_USER", "DB_PARAM_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    seen = {}

    def fake_execute(config, sql, cohort_name, cohort_id, overwrite):
        seen["config"] = config
        return {"count": 1}

    monkeypatch.setattr(extract, "execute_cohort_generation", fake_execute)

    row = extract._run_generate_cohort(
        CONNECTION_DETAILS, sql="select 1", cohort_name="c", cohort_id=1
    )

    assert row["status"] == "success"
    config = seen["config"]
    assert config.uri == "jdbc:postgresql://db:5432/omop"
    assert config.user == "omop_admin"
    assert config.password == "change-me-before-deploying"
    assert config.cdm_database == "omop_cdm"
    assert config.results_schema == "results"


def test_generate_cohort_withholds_counts_below_the_minimum(monkeypatch):
    monkeypatch.delenv("COHORT_BUILDER_MINIMUM_COUNT", raising=False)
    logged = []
    monkeypatch.setattr(extract, "info", logged.append)
    monkeypatch.setattr(
        extract, "execute_cohort_generation", lambda **_: {"count": 3}
    )

    row = extract._run_generate_cohort(
        CONNECTION_DETAILS, sql="select 1", cohort_name="c", cohort_id=1
    )

    assert row["status"] == "success"
    assert row["count"] is None
    assert row["below_minimum"] is True
    assert row["minimum_count"] == 5
    # Algorithm logs can be shared with HQ, so they must not carry it either.
    assert not any("3" in message for message in logged)


def test_generate_cohort_uses_the_node_minimum(monkeypatch):
    monkeypatch.setenv("COHORT_BUILDER_MINIMUM_COUNT", "2")
    monkeypatch.setattr(
        extract, "execute_cohort_generation", lambda **_: {"count": 3}
    )

    row = extract._run_generate_cohort(
        CONNECTION_DETAILS, sql="select 1", cohort_name="c", cohort_id=1
    )

    assert row["count"] == 3
    assert row["minimum_count"] == 2


def test_generate_cohort_does_not_run_with_an_invalid_minimum(monkeypatch):
    monkeypatch.setenv("COHORT_BUILDER_MINIMUM_COUNT", "abc")
    ran = []
    monkeypatch.setattr(
        extract, "execute_cohort_generation", lambda **_: ran.append(1) or {"count": 3}
    )

    row = extract._run_generate_cohort(
        CONNECTION_DETAILS, sql="select 1", cohort_name="c", cohort_id=1
    )

    assert row["status"] == "error"
    assert row["count"] is None
    assert ran == []


def test_generate_cohort_refuses_sql_unless_the_node_allows_it(monkeypatch):
    monkeypatch.delenv("COHORT_BUILDER_ALLOW_SQL")
    ran = []
    monkeypatch.setattr(
        extract, "execute_cohort_generation", lambda **_: ran.append(1) or {"count": 7}
    )

    row = extract._run_generate_cohort(
        CONNECTION_DETAILS, sql="select 1", cohort_name="c", cohort_id=1
    )

    assert row["status"] == "error"
    assert "COHORT_BUILDER_ALLOW_SQL" in row["message"]
    assert ran == []


def test_early_error_rows_say_which_node_they_come_from(monkeypatch):
    # The central step matches rows to organizations by this field; without
    # it the node is reported twice, once with its error and once as silent.
    monkeypatch.setenv("ORGANIZATION_ID", "3")
    monkeypatch.setenv("NODE_ID", "9")
    monkeypatch.delenv("COHORT_BUILDER_ALLOW_SQL")

    row = extract._run_generate_cohort(
        CONNECTION_DETAILS, sql="select 1", cohort_name="c", cohort_id=1
    )

    assert row["status"] == "error"
    assert row["organization_id"] == 3
    assert row["node_id"] == 9


def test_generate_cohort_accepts_atlas_json_when_sql_is_not_allowed(monkeypatch):
    monkeypatch.delenv("COHORT_BUILDER_ALLOW_SQL")
    monkeypatch.setattr(extract, "atlas_json_to_sql", lambda atlas: "SELECT 1")
    monkeypatch.setattr(extract, "execute_cohort_generation", lambda **_: {"count": 7})

    row = extract._run_generate_cohort(
        CONNECTION_DETAILS, atlas_json={"ConceptSets": []}, cohort_name="c", cohort_id=1
    )

    assert row["status"] == "success"
    assert row["count"] == 7


def test_cohort_count_returns_the_extracted_row_as_plain_json():
    frame = pd.DataFrame([{
        "organization_id": 3, "status": "success", "cohort_id": 55, "count": 42,
        "message": None,
    }])

    result = federated._run_cohort_count(frame)

    assert result["count"] == 42
    assert result["organization_id"] == 3
    # numpy scalars must not leak into the JSON result.
    assert type(result["count"]) is int
    assert result["message"] is None


def test_cohort_count_handles_an_empty_session():
    result = federated._run_cohort_count(pd.DataFrame())

    assert result["status"] == "error"


def test_cohort_count_returns_only_cohort_result_fields():
    frame = pd.DataFrame([{
        "organization_id": 3, "status": "success", "cohort_id": 55, "count": 42,
        "message": None, "person_id": 1234, "year_of_birth": 2015,
    }])

    result = federated._run_cohort_count(frame)

    assert "person_id" not in result
    assert "year_of_birth" not in result


def test_cohort_count_refuses_a_dataframe_that_is_not_a_cohort_result():
    # e.g. a person table some other extraction left in the session
    frame = pd.DataFrame([{"person_id": 1234, "gender_concept_id": 8507}])

    result = federated._run_cohort_count(frame)

    assert result["status"] == "error"
    assert "1234" not in str(result)


def test_cohort_count_withholds_small_counts_it_receives(monkeypatch):
    monkeypatch.delenv("COHORT_BUILDER_MINIMUM_COUNT", raising=False)
    frame = pd.DataFrame([{"status": "success", "cohort_id": 55, "count": 2}])

    result = federated._run_cohort_count(frame)

    assert result["count"] is None
    assert result["below_minimum"] is True


def test_legacy_generate_cohort_count_explains_why_it_cannot_run():
    result = undecorated(federated.generate_cohort_count)(
        sql="select 1", cohort_name="c", cohort_id=1
    )

    assert result["status"] == "error"
    assert "generate_cohort" in result["message"]


def test_normalize_jdbc_uri_completes_host_only_uris():
    assert (
        cohort.normalize_jdbc_uri("jdbc:postgresql://omop-postgres.omop.svc", "omop_cdm")
        == "jdbc:postgresql://omop-postgres.omop.svc:5432/omop_cdm"
    )
    assert (
        cohort.normalize_jdbc_uri("jdbc:postgresql://db:5433/other", "omop_cdm")
        == "jdbc:postgresql://db:5433/other"
    )
