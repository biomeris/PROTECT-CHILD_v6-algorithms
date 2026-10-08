
# Differentialy methylated positions & regions

Federated differentially methylated positions (DMPs) and regions (DMRs) between
two cohorts.

Each hospital compares the two cohorts **within its own patients** (a per-probe
linear model with pylluminator) and shares only per-probe summary statistics. The
central function combines the hospitals with a meta-analysis. Because cohorts are
compared within each hospital, differences between hospitals (batch, scanner,
protocol) do not end up in the cohort effect. A hospital that does not hold at
least `min_samples` patients of both cohorts is skipped and reported.

### Input data

Each node's data is the output of `v6-preprocessIDAT-py`: one row per
(sample, probe) with `probe_id`, `sample_label`, `beta`, `m_value` and a cohort
column (default `cohort`). Probe positions and genes come from the EPIC v2
manifest at `MANIFEST_PATH` (default `test/EPIC-8v2-0_A2.csv`).

### Arguments (`central_methylation_analysis`)

| Argument | Default | Description |
| --- | --- | --- |
| `cohort_a` | required | Reference cohort label |
| `cohort_b` | required | Compared cohort label; estimates are `cohort_b - cohort_a` |
| `cohort_column` | `"cohort"` | Column that holds the cohort of each sample |
| `use_m_values` | `true` | Model M-values (default) or beta values |
| `min_samples` | `10` | Minimum patients of each cohort a hospital needs to take part (>= 2). See Privacy |
| `dmr_max_gap` | `1000` | Maximum bp between neighbouring probes of a DMR |
| `dmr_fdr` | `0.05` | Probe `adj_p_value` threshold for DMR probes |
| `dmr_min_probes` | `2` | Minimum probes per DMR |

### What each hospital shares

Per probe: `estimate` (cohort_b - cohort_a), `std_err`, raw `p_value`, the number
of patients per cohort and the mean beta per cohort, plus the probe's position and
genes. Probes with fewer than `min_samples` values in a cohort are dropped. No
patient-level values are shared.

### Privacy

Each hospital shares, for every probe, the mean and standard error per cohort.
With few patients per cohort these reveal a lot about individuals, so the default
`min_samples` is 10. Do not lower it for real data; the test script uses 3 only
because the mock hospitals are small.

### Output

- `comparison`: the compared cohorts, direction (`cohort_b - cohort_a`) and
  `estimate_scale`.
- `n_nodes_used`, `skipped_nodes`: hospitals that took part / were skipped (with reason).
- `warnings`: e.g. when only one hospital contributed. The results then equal that
  hospital's own analysis, there is no replication across hospitals and
  `heterogeneity_p_value` is empty.
- `global_dmps`: per probe, a fixed-effect inverse-variance meta-analysis:
  `estimate`, `std_err`, `estimate_scale`, `z`, `p_value`, `adj_p_value`
  (Benjamini-Hochberg over all probes), `heterogeneity_p_value` (Cochran's Q; empty
  when only one hospital contributed to the probe), `mean_beta_a`, `mean_beta_b`,
  `delta_beta`, `n_nodes`, `n_samples_a`, `n_samples_b`, `chromosome`, `position`,
  `genes`. Sorted by `p_value`.
- `global_dmrs`: runs of neighbouring mapped probes (at most `dmr_max_gap` bp
  apart) that all have `adj_p_value < dmr_fdr` and the same direction, with at
  least `dmr_min_probes` probes. Ranked by `max_probe_adj_p_value` (the least
  significant probe in the region), then `n_probes` and absolute `mean_estimate`.
  Also reports `mean_delta_beta`. `stouffer_p_value` combines the probe z-scores
  as if the probes were independent; neighbouring CpGs are correlated, so it is far
  too small (e.g. 1e-190 for 4 probes). It is descriptive only and is not adjusted:
  use `max_probe_adj_p_value`, `n_probes` and the effect size to judge regions.
- `heatmap_data`: the top 50 DMPs with the mean beta per cohort, pooled over all
  hospitals.

### Scale of the estimates

With `use_m_values=true` (default), `estimate` and `std_err` are differences of
**M-values** (log2(beta / (1 - beta))), not of beta values. An M-value difference of
1.4 can correspond to a beta difference of about 0.2, and with extreme betas M-value
differences get very large. Use `delta_beta` / `mean_delta_beta` to read effects on
the beta scale.

### Testing

```bash
python test/test_compute.py
```

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
  - [ ] When adding new arguments, update the argument values in the test scripts
    under ``test/`` (for example ``test/test_compute.py``).
  - [ ] Run local tests with MockNetwork: ``uv sync --group dev`` then
    ``uv run python test/test_compute.py`` (or the extraction/preprocessing script).
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