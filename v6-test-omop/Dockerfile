# basic python3 image as base
FROM ghcr.io/vantage6/infrastructure/algorithm-ohdsi-base:5.0@sha256:fe179b32294aa37638d25c06d99c392ec8c09fcb3967a6800220d165d4b6ec76

# This is a placeholder that should be overloaded by invoking
# docker build with '--build-arg PKG_NAME=...'
ARG PKG_NAME="v6-test-omop"

# install federated algorithm
COPY . /app
RUN pip install /app

# Fail the build if the OHDSI path cannot even be imported. python-ohdsi loads
# each R package on import, so this catches a missing package before a node
# does.
RUN python -c "import importlib; \
    from ohdsi import database_connector; print('python-ohdsi import OK')"

# Set environment variable to make name of the package available within the
# docker image.
ENV PKG_NAME=${PKG_NAME}

# Tell docker to execute `wrap_algorithm()` when the image is run. This function
# will ensure that the algorithm method is called properly.
CMD python -c "from vantage6.algorithm.tools.wrap import wrap_algorithm; wrap_algorithm()"
