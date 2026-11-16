"""
Link to an OMOP cohort: which of this hospital's samples belong to it.

A cohort defined on clinical data (e.g. with v6-cohort-builder-py, from an ATLAS
definition) is stored in the hospital's OMOP database as patients
(``results.cohort``: cohort_definition_id, subject_id = person_id). The OMOP
SPECIMEN table links each biological sample to its patient; its
``specimen_source_id`` holds the sample's label (the IDAT name, e.g.
``GSM9870086_209147150028_R02C01``). So the samples of a cohort are

    SELECT s.specimen_source_id
    FROM results.cohort c JOIN cdm.specimen s ON s.person_id = c.subject_id
    WHERE c.cohort_definition_id = <cohort id>

This runs inside the hospital, in the extraction step; only counts are logged and
no patient or sample identifier leaves the hospital.

vantage6 gives an extraction step only the database it runs on (here the IDAT
folder), so the node administrator adds the OMOP connection to that database's
settings in the node configuration::

    - name: idat
      type: folder
      ...
      env:
        omop_uri: jdbc:postgresql://<host>[:port]   # or postgresql://<host>[:port]
        omop_database: <database>
        omop_user: <read-only user>
        omop_password: <password>
        omop_cdm_schema: cdm
        omop_results_schema: results

The node passes them to this step only, as connection_details["OMOP_URI"], etc.
"""
from __future__ import annotations

from urllib.parse import urlparse

OMOP_KEYS = ("OMOP_URI", "OMOP_DATABASE", "OMOP_USER", "OMOP_PASSWORD")
DEFAULT_PORT = 5432


class OmopLinkError(ValueError):
    """The OMOP link is not configured or the cohort cannot be read."""


def omop_settings(connection_details: dict) -> dict:
    """Host, port, database, user, password and schemas from the node's settings."""
    missing = [key for key in OMOP_KEYS if not connection_details.get(key)]
    if missing:
        raise OmopLinkError(
            "This hospital's IDAT database has no OMOP connection, so samples cannot be "
            "selected by an OMOP cohort. The node administrator adds "
            + ", ".join(key.lower() for key in missing)
            + " to the 'env' of the IDAT database in the node configuration."
        )
    uri = connection_details["OMOP_URI"].removeprefix("jdbc:")
    parsed = urlparse(uri if "://" in uri else f"postgresql://{uri}")
    if parsed.scheme not in ("postgresql", "postgres"):
        raise OmopLinkError(f"Only PostgreSQL OMOP databases are supported (got '{parsed.scheme}')")
    return {
        "host": parsed.hostname,
        "port": parsed.port or DEFAULT_PORT,
        "dbname": connection_details["OMOP_DATABASE"],
        "user": connection_details["OMOP_USER"],
        "password": connection_details["OMOP_PASSWORD"],
        "cdm_schema": connection_details.get("OMOP_CDM_SCHEMA") or "cdm",
        "results_schema": connection_details.get("OMOP_RESULTS_SCHEMA") or "results",
    }


def cohort_sample_labels(connection_details: dict, cohort_id: int, connect=None) -> set[str]:
    """Labels (SPECIMEN.specimen_source_id) of the samples of the cohort's patients.

    ``connect`` replaces psycopg.connect in tests."""
    settings = omop_settings(connection_details)
    if connect is None:
        import psycopg
        connect = psycopg.connect
    from psycopg import sql

    query = sql.SQL(
        "SELECT DISTINCT s.specimen_source_id FROM {results}.cohort c "
        "JOIN {cdm}.specimen s ON s.person_id = c.subject_id "
        "WHERE c.cohort_definition_id = %s AND s.specimen_source_id IS NOT NULL"
    ).format(results=sql.Identifier(settings["results_schema"]),
             cdm=sql.Identifier(settings["cdm_schema"]))
    try:
        with connect(host=settings["host"], port=settings["port"], dbname=settings["dbname"],
                     user=settings["user"], password=settings["password"],
                     connect_timeout=15) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (int(cohort_id),))
                return {str(row[0]) for row in cursor.fetchall()}
    except OmopLinkError:
        raise
    except Exception as exc:  # connection or SQL error: say what failed, no secrets
        raise OmopLinkError(
            f"Could not read OMOP cohort {cohort_id} from database '{settings['dbname']}' "
            f"at {settings['host']}: {type(exc).__name__}: {exc}"
        ) from exc
