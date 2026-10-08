
# epiclock-v5

Estimation of DNAm (epigenetic) age per cohort.

Each node predicts the epigenetic age of its samples with the requested
[pyaging](https://github.com/rsinghlab/pyaging) clocks and returns, per cohort and
clock, only the number of samples, the mean age and the standard deviation. The
central function pools each cohort across nodes. A node (e.g. a hospital) may hold
samples of several cohorts, and a cohort may span several nodes.

### Input data

Each node's data must be a long-format table with one row per (sample, probe):

| Column | Description |
| --- | --- |
| `probe_id` | CpG probe identifier |
| `sample_label` | Sample identifier |
| `beta` | Beta value |
| *cohort column* | Cohort of the sample (name set by `cohort_column`, default `cohort`) |

If the cohort column is missing, the node raises an error.

### Arguments

`central_function` and `federated_function` accept:

| Argument | Type | Default | Description |
| --- | --- | --- | --- |
| `lista_relojes` | list of strings | required | pyaging clocks to calculate, e.g. `["horvath2013", "hannum", "pcphenoage"]` |
| `cohort_column` | string | `"cohort"` | Name of the column that holds the cohort of each sample |
| `min_samples` | integer (>= 2) | `3` | Minimum number of samples a cohort needs on a node to be included |

### Privacy

- Nodes return only N, mean and standard deviation per (cohort, clock). No
  individual ages, minimum or maximum are returned.
- On each node, any cohort with fewer than `min_samples` samples is dropped before
  the clocks are run, and an info message names the dropped cohort and the reason.
  A (cohort, clock) with fewer than `min_samples` successfully predicted ages is
  dropped as well. If every cohort on a node is dropped, the node returns an empty
  result.
- The default `min_samples` is 3 because with 2 samples the mean and standard
  deviation reveal both individual ages.

### Output

`federated_function` returns one record per (cohort, clock) with `cohort`,
`clock`, `n_samples`, `mean_age` and `sd_age` (sample standard deviation).

`central_function` returns one record per (cohort, clock):

| Column | Description |
| --- | --- |
| `cohort` | Cohort label |
| `clock` | Clock name |
| `mean_age_global` | `sum(n_i * mean_i) / sum(n_i)` over the nodes |
| `sd_age_global` | Standard deviation of all the cohort's samples, combined exactly from the node N, mean and SD |
| `n_samples_total` | `sum(n_i)` over the nodes |
| `n_nodes` | Number of nodes that contributed to the cohort |

### Testing

```bash
python test/test_compute.py
```

The script runs from the algorithm root so pyaging finds the clock files in
`pyaging_data/`. The mock data in `test/test_data_{1,2,3}.csv` represents three
nodes: node 1 holds `cohort_A`, node 2 `cohort_A` and `cohort_B`, and node 3
`cohort_B` and `cohort_C`.

This algorithm is designed to be run with the [vantage6](https://vantage6.ai)
infrastructure for distributed analysis and learning.

The base code for this algorithm has been created via the
[v6-algorithm-template](https://github.com/vantage6/v6-algorithm-template)
template generator.

### Checklist

Note that the template generator does not create a completely ready-to-use
algorithm yet. There are still a number of things you have to do yourself.
Please ensure to execute the following steps. The steps are also indicated with
TODO statements in the generated code - so you can also simply search the
code for TODO instead of following the checklist below.

- [ ] Fill out the fields in the `pyproject.toml` file, such as a URL to your code
      repository. Alternatively, remove these fields.
- [ ] Implement your algorithm functions.
  - [ ] You are free to add more arguments to the functions. Be sure to add them
    *after* the `client` and dataframe arguments.
  - [ ] When adding new arguments, if you run the `test/test.py` script, be sure
    to include values for these arguments in the `client.task.create()` calls
    that are available there.
- [ ] If you are using Python packages that are not in the standard library, add
  them to the `pyproject.toml` file. Note that `pandas` is already included by default.
- [ ] Fill in the documentation template. This will help others to understand your
  algorithm, be able to use it safely, and to contribute to it.
- [ ] If you want to submit your algorithm to a vantage6 algorithm store, be sure
  to fill in everything in ``algorithm_store.json`` (and be sure to update
  it if you change function names, arguments, etc.). It is recommended to run
  ``v6 algorithm generate-store-json`` to automatically generate the file - this
  should work especially well if you have added proper docstrings to your functions.
  Note that you do need the `vantage6` CLI to be able to use this command, which can be
  installed by e.g. running `pip install vantage6` (or `uv pip install vantage6`).
- [ ] Finally, remove this checklist section to keep the README clean.

### Dockerizing your algorithm

To finally run your algorithm on the vantage6 infrastructure, you need to
create a Docker image of your algorithm.

#### Manually via ``make``

You can build (and optionally push) locally:

```bash
make image
# or with push:
make image PUSH_REG=true TAG=1.0.0 VANTAGE6_VERSION=5.0.0
```

#### Manually via ``docker``

Alternatively, a Docker image can be created by executing the following command in the
root of your algorithm directory:

```bash
docker build -t [my_docker_image_name] .
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