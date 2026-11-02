# basic python3 image as base (Debian 12)
FROM ghcr.io/vantage6/infrastructure/algorithm-base:5.0

# R and the Bioconductor packages LOLA needs, from Debian; LOLA itself (and
# simpleCache, which reads the LOLACore caches) from Bioconductor 3.16 / CRAN,
# the releases matching Debian's R 4.2
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      r-base-core r-base-dev build-essential \
      r-bioc-genomicranges r-bioc-iranges r-bioc-s4vectors r-bioc-biocgenerics \
      r-cran-data.table r-cran-reshape2 r-cran-biocmanager \
 && Rscript -e 'install.packages("simpleCache", repos="https://cloud.r-project.org")' \
 && Rscript -e 'BiocManager::install("LOLA", version="3.16", update=FALSE, ask=FALSE)' \
 && Rscript -e 'library(LOLA); library(simpleCache)' \
 && rm -rf /var/lib/apt/lists/*

# Reference database: LOLACore hg38 (cached region sets only), from databio.org.
# Built into the image because nodes have no internet access.
RUN mkdir -p /opt/lola \
 && curl -fsSL http://big.databio.org/regiondb/LOLACoreCaches_180412.tgz \
    | tar -xz -C /opt/lola --strip-components=4 nm/t1/resources/regions/LOLACore/hg38 \
 && ls /opt/lola/LOLACore/hg38
ENV LOLA_REGION_DB=/opt/lola/LOLACore/hg38

# install federated algorithm
COPY . /app
RUN pip install /app

# Name of the Python module that vantage6's wrapper imports: the module folder,
# not the project name. Declared here, after the slow steps, so changing it keeps
# the build cache.
ARG PKG_NAME="lola_sandbox"
ENV PKG_NAME=${PKG_NAME}

# Tell docker to execute `wrap_algorithm()` when the image is run. This function
# will ensure that the algorithm method is called properly.
CMD python -c "from vantage6.algorithm.tools.wrap import wrap_algorithm; wrap_algorithm()"
