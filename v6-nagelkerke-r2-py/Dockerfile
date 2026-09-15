# vantage6 v5 ("Uluru") algorithm base image
FROM ghcr.io/vantage6/infrastructure/algorithm-base:5.0

ARG PKG_NAME="v6-nagelkerke-r2-py"

COPY . /app
RUN pip install /app

ENV PKG_NAME=${PKG_NAME}
CMD ["python", "-c", "from vantage6.algorithm.tools.wrap import wrap_algorithm; wrap_algorithm()"]
