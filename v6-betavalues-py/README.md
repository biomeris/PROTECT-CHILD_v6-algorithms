
# globalIDAT

Global beta and m values, computed per cohort.

For every CpG probe, each node computes the mean Beta and M value of its samples
per cohort. The central function combines the node results within each cohort
into a sample-weighted global mean. A node (e.g. a hospital) may hold samples of
several cohorts, and a cohort may span several nodes.

### Input data

Each node's data must be a long-format table with one row per (sample, probe) and
the columns:

| Column | Description |
| --- | --- |
| `probe_id` | CpG probe identifier |
| `sample_label` | Sample identifier |
| `beta` | Beta value |
| `m_value` | M value |
| *cohort column* | Cohort of the sample (name set by `cohort_column`, default `cohort`) |

If the cohort column is missing, the node raises an error.

### What the user gets

The full table has one row per cohort and probe: about 3.5 million rows (some
460 MB) for four cohorts on EPIC v2, more than vantage6 can store as a result or the
UI can show. The result is therefore limited to at most **10,000 probes**:

- **Targeted query** (`probes`): the probes the user asks for, e.g. the CpGs of a
  gene. A CpG name such as `cg00000029` also selects its EPIC v2 replicates
  (`cg00000029_TC21`, `cg00000029_BC11`), so 450K/EPIC probe lists work on EPIC v2.
  Requested probes that no node has are skipped (the log says how many).
- **Overview** (no `probes`): the `top_n` probes (default 1,000) with the largest
  difference in mean Beta between cohorts, Δβ = largest minus smallest cohort mean.

Requests above 10,000 probes are rejected with a message instead of being tried.

### Arguments (`central_function`)

| Argument | Type | Default | Description |
| --- | --- | --- | --- |
| `cohort_column` | string | `"cohort"` | Name of the column that holds the cohort of each sample. |
| `min_samples` | integer | `2` | Minimum number of samples a cohort needs on a node to be included. |
| `probes` | list of strings | none | Probes to report (at most 10,000). When given, `top_n` is not used. |
| `top_n` | integer | `1000` | Without `probes`: number of top probes by Δβ to report (1-10,000). |

### How the top probes are found

Ranking all probes globally would need every node's full table. Instead:

1. Each node proposes its own `top_n` probes by Δβ between its cohorts
   (`federated_top_probes`; only probe IDs and the node's Δβ leave the node).
2. The central function keeps the best `2 x top_n` candidates and asks every node
   for the per-cohort summaries of those probes (`federated_function`).
3. It computes the global, sample-weighted cohort means and Δβ and returns the
   `top_n` probes with the largest global Δβ.

A probe that is not among the top candidates of any node can be missed, so the
overview is exact for the probes it reports but may not contain every probe of the
true global top. Use a targeted query to check specific probes.

On EPIC v2 data (17 samples on a node) a node sends about 70 KB of candidates and
1 MB of summaries, and needs about 20 s.

### Privacy threshold

On each node, any cohort with fewer than `min_samples` samples is dropped before
anything is returned, and an info message names the dropped cohort and the reason.
Probes that fewer than `min_samples` samples of a cohort have a value for are
dropped as well. If every cohort on a node is dropped, the node returns an empty
result. A node never returns the summaries of all its probes: `federated_function`
requires a probe list of at most 20,000 probes.

### Output

`central_function` returns one record per (cohort, probe), sorted by `delta_beta`
(largest first):

| Column | Description |
| --- | --- |
| `cohort` | Cohort label |
| `probe_id` | CpG probe identifier |
| `beta_mean_global` | `sum(beta_mean_i * n_samples_i) / sum(n_samples_i)` over the nodes |
| `m_mean_global` | `sum(m_mean_i * n_samples_i) / sum(n_samples_i)` over the nodes |
| `n_samples_total` | `sum(n_samples_i)` over the nodes |
| `n_samples_cohort` | Samples of the cohort over all nodes (cohorts that pass `min_samples`) |
| `coverage` | `n_samples_total / n_samples_cohort`: share of the cohort's samples behind the means. QC removes a probe in samples where it fails detection, so a probe can have coverage below 1; such probes are kept, and users can filter on coverage |
| `delta_beta` | Δβ of the probe: largest minus smallest `beta_mean_global` over its cohorts (empty if the probe has only one cohort) |

`federated_function` returns `summary` (one record per (cohort, probe) with
`beta_mean`, `m_mean` and `n_samples` of the node) and `cohort_sizes` (samples per
cohort on the node); `federated_top_probes` returns `probe_id` and
`delta_beta` of the node's candidates.

### Testing

```bash
python test/test_compute.py
```

The mock data in `test/hospital_A` and `test/hospital_B` represents two nodes:
`hospital_A` holds `cohort_A` and `cohort_B`, and `hospital_B` holds `cohort_B` and
`cohort_C`.

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