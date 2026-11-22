
# v6-manova-py

Federated one-way MANOVA (Wilks, Pillai, Hotelling-Lawley, Roy) computed exactly from per-group sums and cross-product matrices

This algorithm is designed to be run with the [vantage6](https://vantage6.ai)
infrastructure for distributed analysis and learning.

The base code for this algorithm has been created via the
[v6-algorithm-template](https://github.com/vantage6/v6-algorithm-template)
template generator.

### How it works

Each node returns, per group, the number of complete cases, the column sums
and the cross-product matrix `X^T X` of the outcome columns. The central
function `manova` adds these up, builds the within- and between-group SSCP
matrices and computes the four multivariate test statistics with their F
approximations. The result is identical to a MANOVA on the pooled data. See
`docs/` for the math, usage, privacy guards and validation.

| File | Contents |
| --- | --- |
| `v6-manova-py/central.py` | `manova`: orchestration and aggregation |
| `v6-manova-py/federated.py` | `compute_local_summary`: per-node summary |
| `v6-manova-py/manova_math.py` | Pure math shared by both, unit tested directly |
| `v6-manova-py/globals.py` | Privacy thresholds |
| `test/test_manova.py` | Math checked against `statsmodels` |
| `test/test_compute.py` | End-to-end runs on the vantage6 `MockNetwork` |

### Running the tests

```bash
uv sync
uv run pytest test
```

### Open points

The privacy thresholds in `globals.py` (hospital > 10 records, group ≥ 5) are
placeholders until the consortium agrees on a minimum-records policy.
Each node can override them through environment variables of the same name.

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
