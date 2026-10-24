FROM ghcr.io/vantage6/infrastructure/algorithm-base:5.0
COPY . /app
RUN pip install /app
ENV PKG_NAME="v6-pca-py"
CMD python -c "from vantage6.algorithm.tools.wrap import wrap_algorithm; wrap_algorithm()"
