# vantage6 v5 ("Uluru") algorithm base image
# Nota: harbor2.vantage6.ai fue cerrado (incidente de seguridad). El registro
# oficial ahora es GitHub Container Registry (ghcr.io).
FROM ghcr.io/vantage6/infrastructure/algorithm-base:5.0

ARG PKG_NAME="v6-elastic-net-py"

# Instala el paquete del algoritmo
COPY . /app
RUN pip install /app

# vantage6 despacha la funcion via PKG_NAME + wrap_algorithm
ENV PKG_NAME=${PKG_NAME}
CMD ["python", "-c", "from vantage6.algorithm.tools.wrap import wrap_algorithm; wrap_algorithm()"]
