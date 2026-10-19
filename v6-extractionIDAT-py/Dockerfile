# basic python3 image as base
FROM ghcr.io/vantage6/infrastructure/algorithm-base:5.0

# This is a placeholder that should be overloaded by invoking
# docker build with '--build-arg PKG_NAME=...'
ARG PKG_NAME="extractionIDAT"

# install federated algorithm
COPY . /app
RUN pip install /app

# pylluminator downloads its array annotations on first use. Algorithms on a node
# have no internet access, so bundle the supported arrays into the image.
RUN python -c "from pylluminator.annotations import Annotations, ArrayType, GenomeVersion; \
[Annotations(array, GenomeVersion.HG38) for array in (ArrayType.HUMAN_450K, ArrayType.HUMAN_EPIC, ArrayType.HUMAN_EPIC_V2)]"

# Set environment variable to make name of the package available within the
# docker image.
ENV PKG_NAME=${PKG_NAME}

# Tell docker to execute `wrap_algorithm()` when the image is run. This function
# will ensure that the algorithm method is called properly.
CMD python -c "from vantage6.algorithm.tools.wrap import wrap_algorithm; wrap_algorithm()"
