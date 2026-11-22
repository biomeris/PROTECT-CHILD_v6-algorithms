
# v6-efa-py

Federated exploratory factor analysis (minres, ML or principal extraction; varimax, promax or oblimin rotation) on the pooled correlation matrix, with KMO and Bartlett's test

This algorithm is designed to be run with the [vantage6](https://vantage6.ai)
infrastructure for distributed analysis and learning.

The base code for this algorithm has been created via the
[v6-algorithm-template](https://github.com/vantage6/v6-algorithm-template)
template generator.

### How it works

Each node returns the number of complete cases, the column sums and the
cross-product matrix `X^T X` of the selected columns. The central function
`efa` adds these up into the pooled correlation matrix, then computes KMO and
Bartlett's test, extracts the factors and rotates them. The result is
identical to an EFA on the pooled data. See `docs/` for the math, usage,
privacy guard and validation.

| File | Contents |
| --- | --- |
| `v6-efa-py/central.py` | `efa`: orchestration and factor analysis |
| `v6-efa-py/federated.py` | `compute_local_moments`: per-node moments |
| `v6-efa-py/efa_math.py` | Pure math (numpy/scipy only), unit tested directly |
| `v6-efa-py/globals.py` | Privacy threshold |
| `test/test_efa.py` | Math checked against `factor_analyzer` |
| `test/test_compute.py` | End-to-end runs on the vantage6 `MockNetwork` |
| `test/run_example.py` | Prints the results for the test data or your own CSVs |

### Running the tests

```bash
uv sync
uv run pytest test
uv run python test/run_example.py
```

`factor_analyzer` is a test-only dependency: version 0.5.1 crashes with
scikit-learn 1.8+, so the dev group pins `scikit-learn<1.8`. The algorithm
itself does not use it.

### Open points

The privacy threshold in `globals.py` (hospital > 10 records) is a
placeholder until the consortium agrees on a minimum-records policy.
Each node can override it through an environment variable of the same name.

### Dockerizing your algorithm

To finally run your algorithm on the vantage6 infrastructure, you need to
create a Docker image of your algorithm.

A Docker image can be created by executing the following command in the root of your
algorithm directory:

```bash
docker build -t [my_docker_image_name] .
```

That builds for your own machine (arm64 on an Apple-silicon Mac), which is fine for
local testing. Vantage6 nodes usually run on linux/amd64, so for an image that will
run on the consortium network build it as the team README asks:

```bash
docker buildx build --platform linux/amd64 -t [my_docker_image_name] .
```

where you should provide a sensible value for the Docker image name. The
`docker build` command will create a Docker image that contains your algorithm.
You can create an additional tag for it by running

```bash
docker tag [my_docker_image_name] [another_image_name]
```

This way, you can e.g. do
`docker tag local_average_algorithm ghcr.io/vantage6/algorithm/average` to
make the algorithm available on a remote Docker registry (in this case
`ghcr.io`).

Finally, you need to push the image to the Docker registry. This can be done
by running

```bash
docker push [my_docker_image_name]
```

Note that you need to be logged in to the Docker registry before you can push
the image. You can do this by running `docker login` and providing your
credentials. Check [this page](https://docs.docker.com/get-started/04_sharing_app/)
For more details on sharing images on Docker Hub. If you are using a different
Docker registry, check the documentation of that registry and be sure that you
have sufficient permissions.
