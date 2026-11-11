FROM ghcr.io/vantage6/infrastructure/algorithm-base:5.0

COPY . /app
RUN pip install /app

# Name of the Python module that vantage6's wrapper imports
ARG PKG_NAME="pca_methylation"
ENV PKG_NAME=${PKG_NAME}

CMD python -c "from vantage6.algorithm.tools.wrap import wrap_algorithm; wrap_algorithm()"
