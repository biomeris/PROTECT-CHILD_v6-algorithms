# syntax=docker/dockerfile:1
# basic python3 image as base
FROM ghcr.io/vantage6/infrastructure/algorithm-base:5.0

# This is a placeholder that should be overloaded by invoking
# docker build with '--build-arg PKG_NAME=...'
ARG PKG_NAME="epiclock-v5"

# install federated algorithm. PyTorch (needed by pyaging) is installed from the
# CPU-only index first: the default Linux wheel pulls several GB of CUDA libraries
# that the nodes do not use.
COPY . /app
RUN pip install torch --index-url https://download.pytorch.org/whl/cpu \
 && pip install /app

# Bundle the epigenetic clocks (~2.5 GB: every human DNA-methylation clock in
# pyaging except SystemsAge) into the Hugging Face cache inside the image, so the
# nodes never download anything and all nodes use the same clock files.
# An optional Hugging Face token can be passed as a build secret (not stored in the
# image): HF_TOKEN=hf_xxx make image   (or docker buildx build --secret id=hf_token,env=HF_TOKEN ...)
ENV HF_HOME=/opt/hf_cache
RUN --mount=type=secret,id=hf_token,required=false \
    if [ -f /run/secrets/hf_token ]; then export HF_TOKEN="$(cat /run/secrets/hf_token)"; fi; \
    cd /app && python bundle_clocks.py --out /opt/clock_bundle

# At runtime pyaging only reads the bundled clocks; the central function checks
# requested clocks against clocks.json.
ENV HF_HUB_OFFLINE=1 \
    EPICLOCK_CLOCK_BUNDLE=/opt/clock_bundle/clocks.json

# Set environment variable to make name of the package available within the
# docker image.
ENV PKG_NAME=${PKG_NAME}

# Tell docker to execute `wrap_algorithm()` when the image is run. This function
# will ensure that the algorithm method is called properly.
CMD python -c "from vantage6.algorithm.tools.wrap import wrap_algorithm; wrap_algorithm()"
