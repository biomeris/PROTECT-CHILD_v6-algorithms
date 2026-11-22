
# v6-dunn-py

Federated Dunn's post-hoc test (pairwise mean-rank comparisons after Kruskal-Wallis) on binned global ranks, without any node sharing individual values

This algorithm is designed to be run with the [vantage6](https://vantage6.ai)
infrastructure for distributed analysis and learning.

The base code for this algorithm has been created via the
[v6-algorithm-template](https://github.com/vantage6/v6-algorithm-template)
template generator.

### How it works

Dunn's test only needs, per group, the count and the sum (and sum of squares)
of the ranks. The global ranks are approximated in three rounds, reusing the
protocol of the GT federated Kruskal-Wallis algorithm
(`v6-kruskal-wallis-py`): nodes send moments, then a group-blind histogram
that the central function turns into equal-count rank bins, then per-group
sums of their records' rank-bin mid-ranks. No round transmits an individual
value. The result also includes the Kruskal-Wallis test.

The ranks are binned, so the result is an approximation: at around 450
records it matches the exact test closely (H within 1%, same decisions); at
tens of records it is indicative only, and the result says so. See
`docs/v6-dunn-py/validation.rst`.

| File | Contents |
| --- | --- |
| `v6-dunn-py/central.py` | `dunn`: three-round orchestration and the tests |
| `v6-dunn-py/federated.py` | The three node rounds |
| `v6-dunn-py/dunn_math.py` | Pure math (binning, Kruskal-Wallis, Dunn's, p-adjust), unit tested directly |
| `v6-dunn-py/globals.py` | Privacy thresholds and binning settings |
| `test/test_dunn.py` | Math checked against `scikit-posthocs`, `scipy` and `statsmodels` |
| `test/test_compute.py` | End-to-end runs on the vantage6 `MockNetwork` |
| `test/run_example.py` | Prints the results for the test data or your own CSVs |

### Running the tests

```bash
uv sync
uv run pytest test
uv run python test/run_example.py
```

### Open points

The privacy thresholds in `globals.py` (hospital > 10 records, group ≥ 5,
histogram cell ≥ 5) are
placeholders until the consortium agrees on a minimum-records policy.
Each node can override them through environment variables of the same name.

The ranks are approximate (binned): close to the exact test from a few hundred
records, indicative only for very small cohorts. See `docs/v6-dunn-py/validation.rst`.

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
