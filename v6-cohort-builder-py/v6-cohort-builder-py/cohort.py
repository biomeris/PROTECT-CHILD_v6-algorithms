"""Pure helpers for cohort request validation and result aggregation."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import pandas as pd


class CohortInputError(ValueError):
    """Raised when a central cohort generation request is invalid."""


class NodeConfigError(RuntimeError):
    """Raised when a node is missing required OMOP configuration."""


# Counts from 1 up to this minimum never leave a node: a cohort of two
# children at one hospital can identify them. Node administrators set their
# own minimum with ``algorithm_env: {COHORT_BUILDER_MINIMUM_COUNT: <n>}`` in
# the node configuration, as with the other PROTECT-CHILD algorithms.
MINIMUM_COUNT_ENV = "COHORT_BUILDER_MINIMUM_COUNT"
DEFAULT_MINIMUM_COUNT = 5

# The sql input runs the requester's own SQL with the node's credentials, so
# a query can make the count encode any value it reads (one patient's year of
# birth, say), whatever the minimum. Nodes accept it only when their
# administrator opts in with ``algorithm_env: {COHORT_BUILDER_ALLOW_SQL: true}``;
# ATLAS JSON, from which CirceR writes the SQL, is always accepted.
ALLOW_SQL_ENV = "COHORT_BUILDER_ALLOW_SQL"
_TRUE_VALUES = {"true", "1", "yes"}
_FALSE_VALUES = {"false", "0", "no"}

# Everything a node's cohort outcome may contain. The federated step returns
# only these fields, whatever else the dataframe it is given holds.
RESULT_FIELDS = (
    "organization_id",
    "node_id",
    "status",
    "cohort_id",
    "cohort_name",
    "input_type",
    "database",
    "cdm_schema",
    "results_schema",
    "count",
    "message",
    "below_minimum",
    "minimum_count",
)


@dataclass(frozen=True)
class CohortRequest:
    cohort_name: str
    sql: str | None
    atlas_json: str | None
    input_type: str
    cohort_id: int
    overwrite: bool


@dataclass(frozen=True)
class OmopConfig:
    uri: str
    database_type: str
    dbms: str
    user: str
    password: str
    cdm_database: str
    cdm_schema: str
    results_schema: str
    organization_id: int | None = None
    node_id: int | None = None


def generate_cohort_id() -> int:
    return int(time.time())


def normalize_atlas_json(atlas_json: str | dict[str, Any]) -> str:
    if isinstance(atlas_json, str):
        if not atlas_json.strip():
            raise CohortInputError("atlas_json must be valid JSON")
        try:
            json.loads(atlas_json)
        except json.JSONDecodeError as exc:
            raise CohortInputError("atlas_json must be valid JSON") from exc
        return atlas_json

    if isinstance(atlas_json, dict):
        try:
            return json.dumps(atlas_json)
        except (TypeError, ValueError) as exc:
            raise CohortInputError("atlas_json must be valid JSON") from exc

    raise CohortInputError("atlas_json must be a JSON string or dictionary")


def validate_cohort_request(
    *,
    cohort_name: str,
    sql: str | None,
    atlas_json: str | dict[str, Any] | None,
    cohort_id: int | None,
    overwrite: bool,
    id_factory=generate_cohort_id,
) -> CohortRequest:
    if not isinstance(cohort_name, str) or not cohort_name.strip():
        raise CohortInputError("cohort_name is required")

    has_sql = sql is not None
    has_atlas_json = atlas_json is not None
    if has_sql == has_atlas_json:
        raise CohortInputError("Provide exactly one of sql or atlas_json")

    if not isinstance(overwrite, bool):
        raise CohortInputError("overwrite must be a boolean")

    if cohort_id is None:
        cohort_id = id_factory()

    if isinstance(cohort_id, bool) or not isinstance(cohort_id, int) or cohort_id <= 0:
        raise CohortInputError("cohort_id must be a positive integer")

    if has_sql:
        if not isinstance(sql, str) or not sql.strip():
            raise CohortInputError("sql must be a non-empty string")
        return CohortRequest(
            cohort_name=cohort_name,
            sql=sql,
            atlas_json=None,
            input_type="sql",
            cohort_id=cohort_id,
            overwrite=overwrite,
        )

    return CohortRequest(
        cohort_name=cohort_name,
        sql=None,
        atlas_json=normalize_atlas_json(atlas_json),
        input_type="atlas_json",
        cohort_id=cohort_id,
        overwrite=overwrite,
    )


def atlas_json_to_sql(atlas_json: str) -> str:
    from ohdsi.circe import (  # pylint: disable=import-outside-toplevel
        build_cohort_query,
        cohort_expression_from_json,
        create_generate_options,
    )

    cohort_expression = cohort_expression_from_json(atlas_json)
    options = create_generate_options()
    query = build_cohort_query(cohort_expression, options)

    if isinstance(query, str):
        return query
    if isinstance(query, bytes):
        return query.decode()
    if isinstance(query, Iterable):
        return "\n".join(str(part) for part in query)
    return str(query)


def _load_ohdsi_modules() -> tuple[Any, Any, Any]:
    from ohdsi import common  # pylint: disable=import-outside-toplevel
    from ohdsi import cohort_generator  # pylint: disable=import-outside-toplevel
    from ohdsi import database_connector  # pylint: disable=import-outside-toplevel

    return common, cohort_generator, database_connector


def execute_cohort_generation(
    *,
    config: OmopConfig,
    sql: str,
    cohort_name: str,
    cohort_id: int,
    overwrite: bool,
) -> dict[str, Any]:
    common, cohort_generator, database_connector = _load_ohdsi_modules()
    connection = None

    connection_details = database_connector.create_connection_details(
        dbms=config.dbms,
        connection_string=normalize_jdbc_uri(config.uri, config.cdm_database),
        user=config.user,
        password=config.password,
    )

    try:
        connection = database_connector.connect(connection_details)
        table_names = cohort_generator.get_cohort_table_names()
        cohort_generator.create_cohort_tables(
            connection=connection,
            cohort_database_schema=config.results_schema,
            cohort_table_names=table_names,
        )

        existing_rows = _get_existing_cohort_row_count(
            common=common,
            database_connector=database_connector,
            connection=connection,
            results_schema=config.results_schema,
            cohort_id=cohort_id,
        )
        if existing_rows and not overwrite:
            raise RuntimeError(
                f"Cohort {cohort_id} already exists in {config.results_schema}.cohort; "
                "rerun with overwrite=True to replace it"
            )
        if existing_rows:
            _delete_existing_cohort_rows(
                database_connector=database_connector,
                connection=connection,
                results_schema=config.results_schema,
                cohort_id=cohort_id,
            )

        cohort_definition_set = pd.DataFrame(
            [
                {
                    "cohortId": cohort_id,
                    "cohortName": cohort_name,
                    "sql": sql,
                }
            ],
            columns=["cohortId", "cohortName", "sql"],
        )
        r_cohort_definition_set = common.convert_to_r(cohort_definition_set)

        cohort_generator.generate_cohort_set(
            connection_details=connection_details,
            cdm_database_schema=config.cdm_schema,
            cohort_database_schema=config.results_schema,
            cohort_table_names=table_names,
            cohort_definition_set=r_cohort_definition_set,
        )
        r_counts = cohort_generator.get_cohort_counts(
            connection_details=connection_details,
            cohort_database_schema=config.results_schema,
            cohort_table="cohort",
            cohort_ids=[cohort_id],
        )
        cohort_counts = common.convert_from_r(r_counts)
        count = extract_subject_count(cohort_counts, cohort_id)

        return {
            "database": config.cdm_database,
            "cdm_schema": config.cdm_schema,
            "results_schema": config.results_schema,
            "count": count,
        }
    finally:
        if connection is not None:
            database_connector.disconnect(connection)


def parse_omop_config(
    connection_details: Mapping[str, str],
    environ: Mapping[str, str] | None = None,
) -> OmopConfig:
    """Read the OMOP connection from the ``connection_details`` the
    ``@data_extraction`` decorator passes in.

    The decorator fills that dict from the node's database configuration:
    ``uri`` and ``type``, plus every ``DB_PARAM_<NAME>`` as ``<NAME>`` (the
    ``env`` block of the node's database entry). The organization and node
    ids are not database settings, so they still come from the environment.
    """
    environ = os.environ if environ is None else environ
    required_keys = [
        "uri",
        "type",
        "DBMS",
        "USER",
        "PASSWORD",
        "CDM_DATABASE",
        "CDM_SCHEMA",
        "RESULTS_SCHEMA",
    ]
    missing = [key for key in required_keys if not connection_details.get(key)]
    if missing:
        raise NodeConfigError(f"Missing required OMOP configuration: {', '.join(missing)}")

    return OmopConfig(
        uri=connection_details["uri"],
        database_type=connection_details["type"],
        dbms=connection_details["DBMS"],
        user=connection_details["USER"],
        password=connection_details["PASSWORD"],
        cdm_database=connection_details["CDM_DATABASE"],
        cdm_schema=connection_details["CDM_SCHEMA"],
        results_schema=connection_details["RESULTS_SCHEMA"],
        organization_id=_optional_int(environ, "ORGANIZATION_ID"),
        node_id=_optional_int(environ, "NODE_ID"),
    )


def cohort_result_row(
    *,
    config: OmopConfig | None,
    cohort_id: int | None,
    cohort_name: str,
    input_type: str | None,
    status: str,
    count: int | None = None,
    message: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """One flat, JSON-friendly row describing a node's cohort outcome.

    The extraction step returns this as a dataframe and the federated step
    hands it back unchanged, so it has to be flat and serialisable.
    """
    return {
        "organization_id": config.organization_id if config else None,
        "node_id": config.node_id if config else None,
        "status": status,
        "cohort_id": cohort_id,
        "cohort_name": cohort_name,
        "input_type": input_type,
        "database": extra.get("database", config.cdm_database if config else None),
        "cdm_schema": extra.get("cdm_schema", config.cdm_schema if config else None),
        "results_schema": extra.get(
            "results_schema", config.results_schema if config else None
        ),
        "count": count,
        "message": message,
    }


def node_identity(environ: Mapping[str, str] | None = None) -> dict[str, int | None]:
    """This node's organization and node ids, for a row written before the
    OMOP connection was read.

    Unreadable ids come back as None instead of raising, since this runs
    while another error is being reported.
    """
    environ = os.environ if environ is None else environ
    identity: dict[str, int | None] = {}
    for field, key in (("organization_id", "ORGANIZATION_ID"), ("node_id", "NODE_ID")):
        try:
            identity[field] = _optional_int(environ, key)
        except NodeConfigError:
            identity[field] = None
    return identity


def minimum_count(environ: Mapping[str, str] | None = None) -> int:
    """The node's minimum shareable count (``COHORT_BUILDER_MINIMUM_COUNT``).

    An invalid setting raises instead of falling back to the default, so a
    node whose administrator mistyped it shares nothing rather than counts
    under a threshold they did not choose.
    """
    environ = os.environ if environ is None else environ
    raw = environ.get(MINIMUM_COUNT_ENV)
    if raw in (None, ""):
        return DEFAULT_MINIMUM_COUNT
    try:
        value = int(raw)
    except ValueError as exc:
        raise NodeConfigError(f"{MINIMUM_COUNT_ENV} must be a positive integer") from exc
    if value < 1:
        raise NodeConfigError(f"{MINIMUM_COUNT_ENV} must be a positive integer")
    return value


def sql_allowed(environ: Mapping[str, str] | None = None) -> bool:
    """Whether this node accepts the ``sql`` input (``COHORT_BUILDER_ALLOW_SQL``).

    Off unless set to true; a value that is neither true nor false raises, so
    a mistyped setting does not decide for the administrator.
    """
    environ = os.environ if environ is None else environ
    raw = str(environ.get(ALLOW_SQL_ENV, "")).strip().lower()
    if raw == "" or raw in _FALSE_VALUES:
        return False
    if raw in _TRUE_VALUES:
        return True
    raise NodeConfigError(f"{ALLOW_SQL_ENV} must be true or false")


def withhold_small_count(row: Mapping[str, Any], minimum: int) -> dict[str, Any]:
    """Blank out a count from 1 to ``minimum - 1``; zero and larger counts stay.

    The row records that it was withheld and the minimum used, so a client
    can show "fewer than 5" instead of a missing value.
    """
    count = row.get("count")
    below = bool(row.get("below_minimum")) or (count is not None and 0 < count < minimum)
    return {
        **row,
        "count": None if below else count,
        "below_minimum": below,
        "minimum_count": minimum,
    }


def normalize_jdbc_uri(uri: str, database: str, default_port: int = 5432) -> str:
    """Complete a JDBC URI that names only the host.

    Node configurations tend to give ``jdbc:postgresql://host``; DatabaseConnector
    wants ``jdbc:postgresql://host:port/database``. Fill in whatever is missing
    and leave a complete URI alone.
    """
    prefix, sep, rest = uri.partition("://")
    if not sep:
        return uri
    hostpart, slash, path = rest.partition("/")
    if ":" not in hostpart:
        hostpart = f"{hostpart}:{default_port}"
    if not path:
        path = database
    return f"{prefix}://{hostpart}/{path}"


def _get_existing_cohort_row_count(
    *,
    common: Any,
    database_connector: Any,
    connection: Any,
    results_schema: str,
    cohort_id: int,
) -> int:
    query = (
        f"SELECT COUNT(*) AS row_count FROM {results_schema}.cohort "
        f"WHERE cohort_definition_id = {cohort_id}"
    )
    if hasattr(database_connector, "query_sql"):
        result = common.convert_from_r(database_connector.query_sql(connection, query))
    else:
        result = connection.execute(query)

    rows = _rows_from_count_result(result)
    if not rows:
        return 0

    normalized = {str(key).lower(): value for key, value in rows[0].items()}
    for key in ("row_count", "rowcount", "count"):
        if key in normalized:
            return _to_int(normalized[key], "existing cohort row count")

    first_value = next(iter(rows[0].values()), 0)
    return _to_int(first_value, "existing cohort row count")


def _delete_existing_cohort_rows(
    *,
    database_connector: Any,
    connection: Any,
    results_schema: str,
    cohort_id: int,
) -> None:
    statement = (
        f"DELETE FROM {results_schema}.cohort "
        f"WHERE cohort_definition_id = {cohort_id}"
    )
    if hasattr(database_connector, "execute_sql"):
        database_connector.execute_sql(connection, statement)
    else:
        connection.execute(statement)


def extract_subject_count(cohort_counts: Any, cohort_id: int) -> int:
    rows = _rows_from_count_result(cohort_counts)
    cohort_id_keys = ("cohortDefinitionId", "cohortId")
    count_keys = (
        "numberSubjects",
        "cohortSubjects",
        "subjectCount",
        "personCount",
        "count",
    )

    for row in rows:
        normalized = {str(key).lower(): value for key, value in row.items()}
        row_cohort_id = _first_value(normalized, cohort_id_keys)
        if row_cohort_id is None or _to_int(row_cohort_id, "cohort id") != cohort_id:
            continue

        subject_count = _first_value(normalized, count_keys)
        if subject_count is None:
            raise CohortInputError("No subject count column found")
        return _to_int(subject_count, "subject count")

    raise CohortInputError(f"No cohort count found for cohort_id {cohort_id}")


def aggregate_central_results(
    *,
    expected_org_ids: Iterable[int],
    raw_results: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Combine the rows ``cohort_count`` returned into one answer.

    ``total_count`` is exact only when every expected node answered and none
    withheld its count. When some did withhold, each of them holds between 1
    and its minimum - 1 subjects, so ``total_range`` gives the lowest and
    highest possible total instead; both are None if any node failed.
    """
    nodes: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    seen_org_ids: set[int] = set()
    cohort_ids: set[Any] = set()

    for result in raw_results:
        raw_organization_id = result.get("organization_id")
        organization_id = (
            int(raw_organization_id) if raw_organization_id is not None else None
        )
        if organization_id is not None:
            seen_org_ids.add(organization_id)
        status = result.get("status")
        if status == "success":
            cohort_ids.add(result.get("cohort_id"))
            count = result.get("count")
            node = {
                "organization_id": organization_id,
                "status": "success",
                "count": int(count) if count is not None else None,
            }
            for key in (
                "database", "cdm_schema", "results_schema", "below_minimum", "minimum_count",
            ):
                if key in result:
                    node[key] = result[key]
            nodes.append(node)
        else:
            errors.append(
                {
                    "organization_id": organization_id,
                    "status": "error",
                    "message": str(result.get("message", "Unknown node error")),
                }
            )

    for organization_id in expected_org_ids:
        if organization_id in seen_org_ids:
            continue
        errors.append(
            {
                "organization_id": organization_id,
                "status": "error",
                "message": f"No result returned by organization {organization_id}",
            }
        )

    total_range = None
    if not errors:
        low = high = 0
        for node in nodes:
            if node["count"] is not None:
                low += node["count"]
                high += node["count"]
            else:
                low += 1
                high += int(node.get("minimum_count") or DEFAULT_MINIMUM_COUNT) - 1
        total_range = [low, high]

    return {
        "cohort_id": next(iter(cohort_ids)) if len(cohort_ids) == 1 else None,
        "nodes": nodes,
        "errors": errors,
        "total_count": total_range[0] if total_range and total_range[0] == total_range[1] else None,
        "total_range": total_range,
    }


def _optional_int(environ: Mapping[str, str], key: str) -> int | None:
    value = environ.get(key)
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise NodeConfigError(f"{key} must be an integer") from exc


def _to_int(value: Any, context: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise CohortInputError(f"Invalid {context} value: {value!r}") from exc


def _rows_from_count_result(cohort_counts: Any) -> list[Mapping[str, Any]]:
    if hasattr(cohort_counts, "to_dict"):
        records = cohort_counts.to_dict(orient="records")
        return [record for record in records]
    if isinstance(cohort_counts, Mapping):
        return [cohort_counts]
    if isinstance(cohort_counts, Iterable) and not isinstance(cohort_counts, (str, bytes)):
        return [row for row in cohort_counts if isinstance(row, Mapping)]
    raise CohortInputError("cohort_counts must be dataframe-like or row mappings")


def _first_value(row: Mapping[str, Any], candidate_keys: Iterable[str]) -> Any | None:
    for key in candidate_keys:
        lowered_key = key.lower()
        if lowered_key in row:
            return row[lowered_key]
    return None
