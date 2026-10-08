
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
| `lista_relojes` | list of strings | `horvath2013`, `hannum`, `pcphenoage`, `pedbe` | pyaging clocks to calculate; the user can choose any pyaging clock names |
| `cohort_column` | string | `"cohort"` | Name of the column that holds the cohort of each sample |
| `min_samples` | integer (>= 2) | `3` | Minimum number of samples a cohort needs on a node to be included |
| `coverage_threshold` | float (0-1) | `0.9` | Minimum coverage for a result to be flagged `coverage_ok`; only sets the flag, never removes results |

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
coverage: every result carries its coverage and a `coverage_ok` flag (`true` when
the coverage is at least `coverage_threshold`), so results can be judged or
filtered afterwards. With the default 0.9, `hannum` is at the limit on EPIC v2 and
will often have `coverage_ok = false`. A clock that fails to compute on a node gives no results for
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

### Output

`federated_function` returns `{"results": [...], "clock_coverage": {clock: coverage},
"legacy_loci_mapping": true/false}`,
with one result record per (cohort, clock): `cohort`, `clock`, `n_samples`,
`mean_age`, `sd_age` (sample standard deviation), `coverage` (share of the clock's
CpGs present on this node, 0-1) and `coverage_ok` (`coverage >= coverage_threshold`).
In `clock_coverage`, a clock that failed to compute has the value `null`.

`central_function` returns `{"results": [...], "clock_coverage": [...],
"n_nodes_legacy_loci_mapping": n}`.
`clock_coverage` has one record per clock with `coverage_min`, `coverage_max`,
`n_nodes_reported`, `n_nodes_coverage_ok` (nodes meeting the threshold) and `n_nodes_failed` (nodes where the
clock failed to compute). `results` has one record per (cohort, clock):

| Column | Description |
| --- | --- |
| `cohort` | Cohort label |
| `clock` | Clock name |
| `mean_age_global` | `sum(n_i * mean_i) / sum(n_i)` over the nodes |
| `sd_age_global` | Standard deviation of all the cohort's samples, combined exactly from the node N, mean and SD |
| `n_samples_total` | `sum(n_i)` over the nodes |
| `n_nodes` | Number of nodes that contributed to the cohort |
| `coverage_min` | Lowest clock coverage over the nodes that contributed to this cohort and clock |
| `coverage_max` | Highest clock coverage over those nodes |
| `coverage_ok` | `true` only if every contributing node meets `coverage_threshold` |

The pooled mean and SD do not depend on `coverage_threshold`.

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