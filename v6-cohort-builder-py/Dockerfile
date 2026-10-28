# vantage6's OHDSI algorithm base (maintained by F.C. Martin, IKNL): Python 3.13,
# vantage6-algorithm-tools 5, R, Java 17 and the OHDSI R packages this
# algorithm uses (SqlRender, DatabaseConnector, CirceR, CohortGenerator,
# FeatureExtraction) with their python-ohdsi wrappers. It is the base the
# other PROTECT-CHILD OMOP algorithms use, pinned to the same digest
# (biomeris/PROTECT-CHILD_v6-algorithms, v6-test-omop).
#
# No JDBC driver download is needed: python-ohdsi's database_connector ships
# the PostgreSQL jar and passes it as pathToDriver when none is given.
FROM ghcr.io/vantage6/infrastructure/algorithm-ohdsi-base:5.0@sha256:fe179b32294aa37638d25c06d99c392ec8c09fcb3967a6800220d165d4b6ec76

LABEL org.opencontainers.image.source="https://github.com/rubensancor/v6_cohort_builder"

# This is a placeholder that should be overloaded by invoking
# docker build with '--build-arg PKG_NAME=...'
ARG PKG_NAME="v6-cohort-builder-py"

# install federated algorithm. pyproject.toml pins rpy2 to the versions that
# load against the base image's R 4.2: the rpy2 the base ships fails on
# import with "undefined symbol: R_getVar".
COPY . /app
RUN pip install /app

# Fail the build if the OHDSI path cannot even be imported. python-ohdsi loads
# each R package on import, so this catches a missing package before a node
# does.
RUN python -c "import importlib; \
    importlib.import_module('v6-cohort-builder-py.cohort')._load_ohdsi_modules(); \
    from ohdsi import circe; print('python-ohdsi import OK')"

# Set environment variable to make name of the package available within the
# docker image.
ENV PKG_NAME=${PKG_NAME}

# Tell docker to execute `wrap_algorithm()` when the image is run. This function
# will ensure that the algorithm method is called properly.
#
# Then leave with os._exit(0), skipping the R and JVM teardown. On vantage6
# nodes the process now and then dies at exit with a SIGSEGV in libR.so, after
# the output is written; the node then marks the run crashed and drops that
# output. os._exit(0) is reached only when wrap_algorithm returns: on any error
# it calls sys.exit(1), which still exits non-zero.
CMD ["python", "-c", "import os, sys; from vantage6.algorithm.tools.wrap import wrap_algorithm; wrap_algorithm(); sys.stdout.flush(); sys.stderr.flush(); os._exit(0)"]
