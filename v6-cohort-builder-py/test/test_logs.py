"""The two kinds of output a run leaves, as in the other PROTECT-CHILD
algorithms.

* The algorithm log (vantage6 ``info``/``warn``/``error``) is for the
  technical team: the step that failed, the exact error and its traceback.
* The ``status`` table in the central result is for the clinician: one
  plain-language sentence, with no technical terms.

Neither may contain a password or a count the node withheld.
"""

import importlib

import pytest


cohort = importlib.import_module("v6-cohort-builder-py.cohort")
extract = importlib.import_module("v6-cohort-builder-py.extract")
federated = importlib.import_module("v6-cohort-builder-py.federated")
central = importlib.import_module("v6-cohort-builder-py.central")

from test_central import FakeClient  # noqa: E402
from test_federated import CONNECTION_DETAILS, _config  # noqa: E402


@pytest.fixture
def log(monkeypatch):
    """Every line the algorithm logs, as (level, message)."""
    lines = []
    for module in (extract, federated, central):
        for level in ("info", "warn", "error"):
            if hasattr(module, level):
                monkeypatch.setattr(
                    module, level,
                    lambda message, level=level: lines.append((level, message)),
                )
    return lines


def _fail_with(monkeypatch, exception):
    monkeypatch.setattr(extract, "parse_omop_config", _config)
    monkeypatch.setattr(
        extract, "execute_cohort_generation",
        lambda **_: (_ for _ in ()).throw(exception),
    )


def _generate(**kwargs):
    arguments = {"atlas_json": {"ConceptSets": []}, "cohort_name": "LT", "cohort_id": 55}
    arguments.update(kwargs)
    return extract._run_generate_cohort(CONNECTION_DETAILS, **arguments)


# Technical log at the node -------------------------------------------------


def test_a_failed_generation_logs_the_step_the_error_and_the_traceback(monkeypatch, log):
    monkeypatch.setattr(extract, "atlas_json_to_sql", lambda atlas: "SELECT 1")
    _fail_with(monkeypatch, RuntimeError("relation results.cohort does not exist"))

    _generate()

    errors = [message for level, message in log if level == "error"]
    assert any(
        "generate cohort" in message
        and "relation results.cohort does not exist" in message
        for message in errors
    )
    assert any("Traceback" in message for message in errors)


def test_a_missing_setting_is_logged_as_a_configuration_error(monkeypatch, log):
    extract._run_generate_cohort(
        {**CONNECTION_DETAILS, "RESULTS_SCHEMA": ""},
        atlas_json={"ConceptSets": []}, cohort_name="LT", cohort_id=55,
    )

    errors = [message for level, message in log if level == "error"]
    assert any("read node settings" in message and "RESULTS_SCHEMA" in message for message in errors)


def test_the_node_logs_who_it_is_and_what_it_was_asked(monkeypatch, log):
    monkeypatch.setattr(extract, "atlas_json_to_sql", lambda atlas: "SELECT 1")
    monkeypatch.setattr(extract, "parse_omop_config", _config)
    monkeypatch.setattr(extract, "execute_cohort_generation", lambda **_: {"count": 42})

    _generate()

    infos = " | ".join(message for level, message in log if level == "info")
    assert "organization 3" in infos and "node 9" in infos
    assert "cohort 55 'LT'" in infos and "atlas_json" in infos
    assert "42 subjects" in infos


def test_replacing_an_existing_cohort_is_logged_as_a_warning(monkeypatch, log):
    monkeypatch.setattr(extract, "atlas_json_to_sql", lambda atlas: "SELECT 1")
    monkeypatch.setattr(extract, "parse_omop_config", _config)
    monkeypatch.setattr(extract, "execute_cohort_generation", lambda **_: {"count": 42})

    _generate(overwrite=True)

    assert any(level == "warn" and "overwrite" in message for level, message in log)


@pytest.mark.parametrize("count", [1, 3, 4])
def test_a_withheld_count_never_reaches_the_log(monkeypatch, log, count):
    monkeypatch.setattr(extract, "atlas_json_to_sql", lambda atlas: "SELECT 1")
    monkeypatch.setattr(extract, "parse_omop_config", _config)
    monkeypatch.setattr(extract, "execute_cohort_generation", lambda **_: {"count": count})

    _generate()

    assert not any(f"{count} subject" in message for _, message in log)


@pytest.mark.parametrize(
    "exception",
    [RuntimeError("boom"), cohort.NodeConfigError("bad"), cohort.CohortInputError("bad")],
)
def test_the_password_never_reaches_the_log(monkeypatch, log, exception):
    _fail_with(monkeypatch, exception)

    _generate()

    assert log
    assert not any(CONNECTION_DETAILS["PASSWORD"] in message for _, message in log)


# What kind of failure, so the central step can explain it --------------------


@pytest.mark.parametrize(
    "exception, kind",
    [
        (cohort.CohortInputError("bad atlas"), "invalid_request"),
        (cohort.NodeConfigError("missing"), "node_configuration"),
        (cohort.CohortExistsError("exists"), "cohort_exists"),
        (RuntimeError("jdbc refused"), "technical"),
    ],
)
def test_an_error_row_says_what_kind_of_failure_it_was(monkeypatch, exception, kind):
    monkeypatch.setattr(extract, "atlas_json_to_sql", lambda atlas: "SELECT 1")
    _fail_with(monkeypatch, exception)

    row = _generate()

    assert row["status"] == "error"
    assert row["error_kind"] == kind


def test_an_invalid_request_is_an_invalid_request_error():
    row = extract._run_generate_cohort(
        CONNECTION_DETAILS, atlas_json="{not json", cohort_name="LT", cohort_id=55
    )

    assert row["error_kind"] == "invalid_request"


def test_a_cohort_that_exists_raises_cohort_exists_error():
    assert issubclass(cohort.CohortExistsError, RuntimeError)


def test_cohort_count_passes_the_error_kind_on():
    import pandas as pd

    row = federated._run_cohort_count(pd.DataFrame([{
        "status": "error", "count": None, "cohort_id": 55,
        "message": "x", "error_kind": "cohort_exists",
    }]))

    assert row["error_kind"] == "cohort_exists"


def test_cohort_count_logs_why_it_has_no_row(log):
    import pandas as pd

    row = federated._run_cohort_count(pd.DataFrame())

    assert row["error_kind"] == "missing_cohort"
    assert any(level == "error" and "No cohort result" in message for level, message in log)


# Plain-language summary for the clinician ------------------------------------


def _summary(result):
    assert len(result["status"]) == 1
    return result["status"][0]["outcome"], result["status"][0]["message"]


def test_summary_gives_the_exact_total():
    client = FakeClient([
        {"organization_id": 1, "status": "success", "count": 120},
        {"organization_id": 2, "status": "success", "count": 38},
    ])

    outcome, message = _summary(central._run_central_function(client))

    assert outcome == "completed"
    assert message == "158 patients across 2 hospitals meet the cohort criteria."


def test_summary_gives_a_range_when_a_hospital_hides_a_small_count():
    client = FakeClient([
        {"organization_id": 1, "status": "success", "count": 158},
        {"organization_id": 2, "status": "success", "count": None,
         "below_minimum": True, "minimum_count": 5},
    ])

    outcome, message = _summary(central._run_central_function(client))

    assert outcome == "completed"
    assert message == (
        "Between 159 and 162 patients across 2 hospitals meet the cohort criteria. "
        "At 1 hospital fewer than 5 patients match, so the exact number is hidden "
        "to protect patient privacy."
    )


def test_summary_counts_the_hospitals_that_answered_and_explains_the_others():
    client = FakeClient([
        {"organization_id": 1, "status": "success", "count": 11},
        {"organization_id": 2, "status": "success", "count": 27},
        {"organization_id": 3, "status": "error", "error_kind": "node_configuration",
         "message": "Missing required OMOP configuration: RESULTS_SCHEMA"},
    ])
    client.organization.list = lambda: [{"id": 1}, {"id": 2}, {"id": 3}]

    outcome, message = _summary(central._run_central_function(client))

    assert outcome == "partial"
    assert message == (
        "At the 2 hospitals that answered, 38 patients meet the cohort criteria. "
        "1 hospital could not take part: its data connection is not set up correctly. "
        "The technical team can see the details in the log."
    )


@pytest.mark.parametrize(
    "kind, reason",
    [
        ("invalid_request", "the cohort definition could not be read. "
         "Please rebuild it in the cohort builder and try again."),
        ("cohort_exists", "a cohort with this number already exists there. "
         "Use a new cohort number, or allow replacing the existing one."),
        ("technical", "a technical problem stopped the calculation. "
         "The technical team can see the details in the log."),
    ],
)
def test_summary_explains_why_no_hospital_could_answer(kind, reason):
    client = FakeClient([
        {"organization_id": 1, "status": "error", "error_kind": kind, "message": "x"},
        {"organization_id": 2, "status": "error", "error_kind": kind, "message": "x"},
    ])

    outcome, message = _summary(central._run_central_function(client))

    assert outcome == "failed"
    assert message == f"No hospital could calculate the cohort: {reason}"


def test_summary_treats_a_hospital_that_never_answered_as_a_technical_problem():
    client = FakeClient(
        [{"organization_id": 1, "status": "success", "count": 7}, None],
        snapshots=[["completed", "crashed"]],
    )

    outcome, message = _summary(central._run_central_function(client, poll_interval=0))

    assert outcome == "partial"
    assert "a technical problem stopped the calculation" in message


def test_summary_explains_unknown_hospitals():
    result = central._run_central_function(FakeClient([]), organizations_to_include=[1, 3])

    assert "3" in result["error"]
    assert _summary(result) == (
        "failed",
        "Some of the selected hospitals are not part of this collaboration. "
        "Check the hospital selection and try again.",
    )


def test_summary_explains_that_the_cohort_was_not_generated_yet():
    client = FakeClient([])
    client.task.create = lambda **kwargs: {
        "msg": "The dataframe you selected is not present for the following organizations: 3."
    }

    result = central._run_central_function(client)

    assert "not present for the following organizations: 3" in result["error"]
    assert _summary(result) == (
        "failed",
        "The cohort has not been generated at every selected hospital yet. "
        "Run 'Generate cohort' first, then ask for the total again.",
    )


def test_central_logs_each_failing_hospital_with_its_exact_error(log):
    client = FakeClient([
        {"organization_id": 1, "status": "success", "count": 7},
        {"organization_id": 2, "status": "error", "error_kind": "technical",
         "message": "jdbc connection refused"},
    ])

    central._run_central_function(client)

    assert ("error", "organization 2: jdbc connection refused") in log
    assert any(level == "info" and "1 of 2 organizations" in message for level, message in log)


def test_the_summary_has_no_technical_terms():
    client = FakeClient([
        {"organization_id": 1, "status": "error", "error_kind": "node_configuration",
         "message": "Missing required OMOP configuration: RESULTS_SCHEMA"},
    ])
    client.organization.list = lambda: [{"id": 1}]

    _, message = _summary(central._run_central_function(client))

    for term in ("RESULTS_SCHEMA", "OMOP", "error_kind", "organization", "Traceback"):
        assert term not in message
