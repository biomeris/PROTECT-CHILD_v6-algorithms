
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
| *age column* (optional) | Chronological age of the sample (name set by `age_column`, default `age`, as added by `v6-extractionIDAT-py` with `sample_columns=["Age"]`) |

If the cohort column is missing, the node raises an error. Without an age column
the chronological age comparison is skipped.

Only the CpGs of the requested clocks are used: each node keeps the probes that one
of the clocks needs (read from the clock models) before converting and pivoting
the data, which leaves the ages unchanged and saves most of the memory and time on
EPIC v2 tables of ~870k probes.

### Arguments

`central_function` and `federated_function` accept:

| Argument | Type | Default | Description |
| --- | --- | --- | --- |
| `clock_set` | string | none | `central_function` only. A clock set (the web interface's dropdown): `Default (horvath2013, hannum, pcphenoage, pedbe)`, `Blood chronological-age clocks`, `All chronological-age clocks`, `Biological, phenotypic and mortality clocks`, `Paediatric and gestational clocks`, `Mitotic clocks` or `Only the clocks listed below`. Except the default, the sets are computed from the image's clock bundle and leave out clocks needing extra inputs; `lista_relojes` are added to the set. Not given: `lista_relojes`, or the defaults |
| `lista_relojes` | list of strings | `horvath2013`, `hannum`, `pcphenoage`, `pedbe` | pyaging clocks to calculate (empty = the defaults). The web interface offers the 142 clocks of the image as a list; [docs/clocks.md](docs/clocks.md) describes what each predicts, for which tissue and population, and which need extra inputs |
| `cohort_column` | string | `"cohort"` | Name of the column that holds the cohort of each sample |
| `min_samples` | integer (>= 2) | `3` | Minimum number of samples a cohort needs on a node to be included |
| `age_column` | string | `"age"` | Column with the chronological age; used to report the age acceleration (predicted minus chronological age). `null` skips it |
| `organizations_to_include` | list of organization ids | all | `central_function` only: hospitals to use; empty = all |
| `reference_cohort` | string | first alphabetically | `central_function` only: the cohort the others are compared with |
| `bin_width` | float (years) | `2` | Width of the bins of the age distributions and of the chronological-age bins of the calibration (blank or 0 = 2) |
| `min_bin_count` | integer (>= 3) | `5` | A bin with fewer samples at a hospital is not reported |

If HQ refuses to create the per-hospital subtasks (e.g. the dataframe does not exist at one of the hospitals), the task stops at once with HQ's message instead of waiting.

### EPIC v2 data

The clocks were trained on 450K / EPIC v1 data and use plain CpG identifiers
(`cg00000029`). EPIC v2 adds a suffix to every probe (`cg00000029_TC21`) and
measures some CpGs with several replicate probes. Each node converts the
identifiers automatically and averages replicate probes before running the clocks
(`epiclock_v5/epicv2.py`); 450K / EPIC v1 data is used as it is.

EPIC v2 also **renamed** some CpGs: the manifest columns `Methyl450_Loci` /
`EPICv1_Loci` link 1,112 EPIC v2 probes to a different 450K / EPIC v1 identifier
(e.g. `cg11707779_TC21` is `cg14361627` on 450K). These CpGs are mapped to their
450K / EPIC v1 identifier. Only the 1,085 unambiguous links are used (one target
identifier, not itself an EPIC v2 name, not claimed by another CpG).

The mapping, built from the Illumina manifest `EPIC-8v2-0_A2.csv`, **ships with the
algorithm** (`epiclock_v5/data/epicv2_legacy_loci.json`, 30 KB), so nodes need no
manifest. Optional environment variables on a node:

- `MANIFEST_PATH`: build the mapping from this EPIC v2 manifest instead (e.g. a
  newer release; the same variable `v6-dmp-dmr-py` uses).
- `EPICLOCK_LEGACY_MAP=none`: switch the mapping off (only suffixes are stripped).

The output reports in `n_nodes_legacy_loci_mapping` how many nodes used a mapping.
To regenerate the packaged file from a new manifest:

```bash
python adapt_epicv2.py --manifest EPIC-8v2-0_A2.csv --export-legacy-map epiclock_v5/data/epicv2_legacy_loci.json
```

Some clock CpGs are not on EPIC v2 at all. On the full EPIC v2 array:

| Clock | CpGs | On EPIC v2 (suffix only) | With rename mapping | Still missing |
| --- | --- | --- | --- | --- |
| `horvath2013` | 353 | 340 (96.3%) | 340 (96.3%) | 13 |
| `hannum` | 71 | 64 (90.1%) | 65 (91.5%) | 6 |
| `pcphenoage` | 78,464 | 72,663 (92.6%) | 72,772 (92.7%) | 5,692 |

pyaging fills missing CpGs with reference values or, for clocks without them
(such as `hannum`), with 0, which can bias the predicted age. Each node therefore
measures the coverage of each clock (the share of its CpGs present) on its own data,
after QC, which removes more probes. No clock is ever skipped because of low
coverage: every result carries its coverage, in %, and there is no threshold or
pass/fail flag. EPIC v2 always lacks some clock CpGs, so the user judges whether a
clock's coverage is enough for the question at hand (`hannum`, for example, is
around 90% on EPIC v2). A clock that fails to compute on a node gives no results for
that node and is counted as failed in `clock_coverage`.

To check or convert a file before running the algorithm:

```bash
python adapt_epicv2.py input.csv [output.csv] [--clocks horvath2013 hannum pcphenoage] [--manifest EPIC-8v2-0_A2.csv]
# without --manifest, the packaged rename mapping is used
```

The input may be long format (`probe_id`, `sample_label`, `beta`, ...) or wide
format (one column per probe). The script prints the coverage of each clock and
writes the converted data if an output path is given.

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
- The chronological age comparison follows the same threshold: if fewer than
  `min_samples` samples of a (cohort, clock) have an age, its age fields are empty
  (the row is kept; `n_with_age` says how many samples had an age).
- Distributions are binned counts, not quartiles or minimum / maximum (the median
  of 3 samples is a patient's age). A bin with fewer than `min_bin_count` samples
  (default 5) at a hospital is not reported; the hospital only says how many of
  its samples are in bins not shown, so an empty bin cannot be told from one with
  a few patients, nor where those samples are. The same rule applies to the
  calibration bins.
- The cohort comparisons need no further data from the hospitals: they are
  computed exactly from the N, means and SDs above.

### Output

`federated_function` returns `{"results": [...], "clock_coverage": {clock: coverage},
"legacy_loci_mapping": true/false}`,
with one result record per (cohort, clock): `cohort`, `clock`, `n_samples`,
`mean_age`, `sd_age` (sample standard deviation), `coverage` (share of the clock's
CpGs present on this node, 0-1),
`n_with_age`, `mean_chronological_age`, `mean_age_acceleration`,
`sd_age_acceleration` (acceleration = predicted minus chronological age),
`sd_chronological_age`, `sd_predicted_age_with_age` (SD of the predicted age of the
samples with an age) and `mean_abs_error` (mean of |predicted - chronological|).
These are aggregates over at least `min_samples` samples, like the other fields.
In `clock_coverage`, a clock that failed to compute has the value `null`.

`central_function` returns `{"results": [...], "clock_coverage": [...],
"clock_accuracy": [...], "n_nodes_legacy_loci_mapping": n}`.
`clock_coverage` has one record per clock with `coverage_pct_min`, `coverage_pct_max`
(lowest and highest coverage over the nodes, in %), `n_nodes_reported` and
`n_nodes_failed` (nodes where the
clock failed to compute). `results` has one record per (cohort, clock):

| Column | Description |
| --- | --- |
| `cohort` | Cohort label |
| `clock` | Clock name |
| `mean_age_global` | `sum(n_i * mean_i) / sum(n_i)` over the nodes |
| `sd_age_global` | Standard deviation of all the cohort's samples, combined exactly from the node N, mean and SD |
| `n_samples_total` | `sum(n_i)` over the nodes |
| `n_nodes` | Number of nodes that contributed to the cohort |
| `coverage_pct_min` | Lowest clock coverage (% of its CpGs present) over the nodes that contributed to this cohort and clock |
| `coverage_pct_max` | Highest clock coverage (%) over those nodes |
| `n_with_age_total` | Samples with a predicted and a chronological age, over the nodes that reported the comparison |
| `mean_chronological_age_global` | Mean chronological age of those samples |
| `mean_age_acceleration_global` | Mean of predicted minus chronological age, pooled exactly like the ages |
| `sd_age_acceleration_global` | Standard deviation of the age acceleration |
| `mean_abs_error_global` | Mean absolute error of the predicted age (years), weighted by `n_with_age` |
| `correlation_global` | Pearson correlation between predicted and chronological age over those samples, exact from the node means and SDs: `cov = (var_pred + var_real - var_accel) / 2` |

`clock_accuracy` has one record per clock, over all cohorts and nodes with an age
comparison, sorted by mean absolute error (best first): `clock`,
`n_with_age_total`, `n_cohorts`, `mean_abs_error_global`, `correlation_global`,
`mean_age_acceleration_global` and `coverage_pct_min` (lowest coverage, %, over the
nodes behind the comparison). A clock is accurate when its error
is small and its correlation high; within a single cohort the age range is narrow,
so the correlation per cohort is less informative than over all cohorts. Results
from nodes running an earlier version pool as before, with these fields empty.

`clock_coverage` also has the clock's `tissue`, `population`, `predicts` (what it
estimates, e.g. chronological or biological age), `unit`, `year` and `citation`,
from the clock bundle.

**Cohort comparisons** (1.6.0), each cohort against `reference_cohort`:

- `cohort_comparison`: difference in mean age acceleration (predicted minus
  chronological age), 95% CI and Welch p-value, from the pooled N, mean and SD
  (equal to a Welch test on all samples; tested against scipy).
- `age_adjusted_comparison`: linear regression predicted age ~ chronological age +
  cohort over all samples with an age, one common age slope (`age_slope`); the
  cohort coefficient is the difference in predicted age at the same chronological
  age. Fitted exactly from per-cohort sums rebuilt from the hospitals' N, means and
  SDs (the covariance from `var_pred + var_real - var_accel = 2 cov`; tested
  against OLS on the samples).
- `p_adjusted`: Benjamini-Hochberg over the clocks, per cohort. The clocks are
  correlated (shared CpGs, all estimate age), so several significant clocks are
  not independent confirmations.
- `summary` notes these caveats, and that a difference between cohorts may be a
  difference between hospitals when each compared cohort comes from a single,
  different hospital.

**Distribution and calibration** (1.6.0):

- `distribution`: per cohort, clock and measure (`predicted_age`,
  `age_acceleration`): samples per bin [`bin_start`, `bin_end`), summed over the
  hospitals; `n_total` and `n_not_shown` (samples in bins a hospital could not
  report).
- `calibration`: per clock and chronological-age bin, over all cohorts: `n`, mean
  chronological age, mean and SD of the predicted age, and their mean difference.

### Testing

```bash
python test/test_compute.py
```

The mock data in `test/test_data_{1,2,3}.csv` represents three nodes: node 1 holds
`cohort_A`, node 2 `cohort_A` and `cohort_B`, and node 3 `cohort_B` and `cohort_C`.

### Clocks and pyaging version

Requires `pyaging>=0.5.7,<0.6` (installed with `pip install -e .`). Clock names
are pyaging's, e.g. `horvath2013`, `hannum`, `pcphenoage`, `dnamphenoage` (Levine
2018 DNAm PhenoAge; note that `phenoage` is Levine's *clinical* PhenoAge from blood
biomarkers, not a methylation clock), `zhangen`, `weidner`, `lin`, `bocklandt`,
`corticalclock` (Shireby 2020) and `pedbe` (paediatric buccal clock). See
`pyaging.utils.show_all_clocks()`.

If `lista_relojes` is not given (or empty), the default clocks are calculated:
`horvath2013`, `hannum`, `pcphenoage` and `pedbe` (`DEFAULT_CLOCKS` in
`epiclock_v5/federated.py`; keep the `default_value` in `algorithm_store.json` in
sync, the test checks it).

### Clock bundle in the Docker image

pyaging 0.5 downloads each clock from the Hugging Face Hub the first time it is
used. vantage6 nodes normally run algorithms without internet access, so the clocks
are downloaded **when the Docker image is built** and the image runs with
`HF_HUB_OFFLINE=1`; the nodes never contact Hugging Face, and every node uses
exactly the same clock files (fixed by the image version).

- The bundle holds every **human DNA-methylation clock** in pyaging's catalogue
  except the SystemsAge family: 142 clocks, about 2.5 GB. SystemsAge (12 clocks,
  ~24 GB, trained on older adults) is left out; it can be added later as a
  separate image if needed. Clocks for other data types (proteomics,
  transcriptomics, ...) or species cannot use this algorithm's input.
- `bundle_clocks.py` does the download and writes `/opt/clock_bundle/clocks.json`
  with, per clock, the Hugging Face revision, size, number of features, the non-CpG
  inputs it needs and pyaging's catalogue metadata (tissue, population, platform).
  `python bundle_clocks.py --list --out x` prints the bundled clocks.
- The central function rejects clocks that are not in the bundle before any task
  is sent, with a message listing the available clocks. The output's
  `clock_coverage` reports per clock its `extra_features`, `tissue` and
  `population`.
- Some clocks need inputs besides CpGs (e.g. GrimAge needs age and sex). The
  algorithm does not pass these yet; pyaging fills them with reference values or 0
  and the central function warns. They can be passed once the data model provides
  them.
- Build with `make image`. An optional Hugging Face token (higher download rate
  limits) is passed as a build secret and is not stored in the image:
  `HF_TOKEN=hf_xxx make image`.

Outside Docker (local development) there is no bundle: pyaging downloads clocks on
first use into `~/.cache/huggingface` and any clock name is accepted.

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