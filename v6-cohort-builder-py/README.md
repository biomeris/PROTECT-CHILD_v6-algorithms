
# v6-cohort-builder-py

Cohort builder for OMOP Common Data Model (CDM) databases through vantage6 5. It takes an ATLAS cohort definition (Circe JSON) or OHDSI SQL, builds the cohort in each node's OMOP database and returns how many patients each node has. Counts below a node's minimum never leave the node.

Developed by Universidad de Deusto (Rubén Sánchez-Corcuera, Unai Zulaika) for PROTECT-CHILD.

## Functions

| Function | Step | What it does |
|---|---|---|
| `generate_cohort` | data extraction | Turns the definition into SQL (CirceR), writes the cohort to the node's `results.cohort` table (CohortGenerator) and leaves a one-row dataframe with the outcome in the session |
| `cohort_count` | federated | Returns each node's row (`status`, `cohort_id`, `count`, `below_minimum`, `minimum_count` and, on failure, `message`) |
| `central_function` | central | Asks each node's `cohort_count`, optionally only for `organizations_to_include`, and returns every row, the errors and the total |

A client runs them in order in one session.

1. `generate_cohort` on each node's `omop` database, with `cohort_name` and exactly one of `atlas_json` or `sql`, plus optional `cohort_id` and `overwrite`.
2. `central_function` for the total, or `cohort_count` for each node's row.

The cohort is built in a data-extraction step because vantage6 5 gives a task the database credentials only when its action is `data_extraction`. Federated and central steps run without database access.

`central_function` returns `total_count` when the total is exact, and `total_range`, the lowest and highest possible total, when some node withheld its count.

Pass the same `cohort_id` to every node. When it is omitted, each node makes one up from the current time. If a node already holds rows for that cohort id, generation fails unless `overwrite` is true.

## Privacy

A count from 1 up to the node's minimum (5 by default) never leaves the node, not even in the algorithm log. The row carries `count: null` and `below_minimum: true` instead. Zero is shared.

Nodes accept `sql` only when their administrator allows it, because the requester's own SQL can make the count reveal any value in the database. `atlas_json` is always accepted.

Each node sets both in its configuration.

```yaml
algorithm_env:
  COHORT_BUILDER_MINIMUM_COUNT: 5
  COHORT_BUILDER_ALLOW_SQL: false
```

More detail in `docs/v6-cohort-builder-py/privacy.rst`.

## Node configuration

Each node needs an OMOP CDM database of type `omop`.

```yaml
node:
  databases:
    serviceBased:
      - name: omop
        uri: jdbc:postgresql://omop-postgres.omop.svc.cluster.local
        type: omop
        env:
          user: omop_admin
          password: ...
          dbms: postgresql
          cdm_database: omop_cdm
          cdm_schema: cdm
          results_schema: results
```

The node hands the `env` keys to the algorithm container as `DB_PARAM_*`, and the `@data_extraction` decorator passes them to `generate_cohort` upper-cased in `connection_details` (`user` becomes `USER`, and so on), together with `uri` and `type`.

vantage6 isolates algorithm pods with a deny-all network policy, DNS included, so the node also needs an egress policy that lets them reach the database.

## OHDSI dependencies

The image builds on vantage6's `algorithm-ohdsi-base:5.0`, pinned by digest in the `Dockerfile` to the same image as `v6-test-omop`. It carries R, Java and the OHDSI R packages with their [`vantage6/python-ohdsi`](https://github.com/vantage6/python-ohdsi) wrappers. `pyproject.toml` adds the `ohdsi-*` packages the base lacks and pins `rpy2` to the versions that load against the base's R 4.2.

## Tests

The unit tests stub the OHDSI calls, so they need no R, Java or database.

```bash
uv run --no-project --python 3.13 --with pytest --with pandas --with "vantage6-algorithm-tools>=5,<6" python -m pytest test/
```

## Build the Docker image

```bash
make image
# or with push
make image PUSH_REG=true TAG=1.0.0 VANTAGE6_VERSION=5.0.0
```
