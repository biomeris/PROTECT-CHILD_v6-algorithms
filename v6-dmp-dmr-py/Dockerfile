# basic python3 image as base
FROM ghcr.io/vantage6/infrastructure/algorithm-base:5.0

# install federated algorithm (the probe annotation is packaged with it, in
# differentialy_methylated_positions_regions/data/; the ~930 MB manifest CSV in
# test/ is excluded by .dockerignore)
COPY . /app
RUN pip install /app

# Name of the Python module that vantage6's wrapper imports: the module folder,
# not the project name. Declared here, after the slow steps, so changing it keeps
# the build cache.
ARG PKG_NAME="differentialy_methylated_positions_regions"
ENV PKG_NAME=${PKG_NAME}

# Tell docker to execute `wrap_algorithm()` when the image is run. This function
# will ensure that the algorithm method is called properly.
CMD python -c "from vantage6.algorithm.tools.wrap import wrap_algorithm; wrap_algorithm()"
