# syntax=docker/dockerfile:1
# basic python3 image as base
FROM ghcr.io/vantage6/infrastructure/algorithm-base:5.0

# Dependencies first, before the algorithm's code, so that changing the code does not
# reinstall them nor download the clocks again. PyTorch (needed by pyaging) is
# installed from the CPU-only index: the default Linux wheel pulls several GB of CUDA
# libraries that the nodes do not use. Keep these in line with pyproject.toml.
RUN pip install torch --index-url https://download.pytorch.org/whl/cpu \
 && pip install "vantage6-algorithm-tools>=5.0.0a43" pandas "pyaging>=0.5.7,<0.6"

# Bundle the epigenetic clocks (~2.5 GB: every human DNA-methylation clock in
# pyaging except SystemsAge) into the Hugging Face cache inside the image, so the
# nodes never download anything and all nodes use the same clock files. Only the
# bundling script and its module are copied here (no package __init__ needed).
# An optional Hugging Face token can be passed as a build secret (not stored in the
# image): HF_TOKEN=hf_xxx make image   (or docker buildx build --secret id=hf_token,env=HF_TOKEN ...)
ENV HF_HOME=/opt/hf_cache
COPY bundle_clocks.py /app/bundle_clocks.py
COPY epiclock_v5/clock_bundle.py /app/epiclock_v5/clock_bundle.py
RUN --mount=type=secret,id=hf_token,required=false \
    if [ -f /run/secrets/hf_token ]; then export HF_TOKEN="$(cat /run/secrets/hf_token)"; fi; \
    cd /app && python bundle_clocks.py --out /opt/clock_bundle

# At runtime pyaging only reads the bundled clocks; the central function checks
# requested clocks against clocks.json.
ENV HF_HUB_OFFLINE=1 \
    EPICLOCK_CLOCK_BUNDLE=/opt/clock_bundle/clocks.json

# install federated algorithm (dependencies are already installed above)
COPY . /app
RUN pip install --no-deps /app

# Name of the Python module that vantage6's wrapper imports: the module folder
# (epiclock_v5), not the pip package name (epiclock-v5). Declared here, after the
# slow steps, so changing it does not rebuild PyTorch and the clock bundle.
ARG PKG_NAME="epiclock_v5"
ENV PKG_NAME=${PKG_NAME}

# Tell docker to execute `wrap_algorithm()` when the image is run. This function
# will ensure that the algorithm method is called properly.
CMD python -c "from vantage6.algorithm.tools.wrap import wrap_algorithm; wrap_algorithm()"
