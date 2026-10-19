
# extractionIDAT

Extraction of IDAT

Reads the IDAT files of a node into Beta and M values, with the cohort of each
sample from the sample sheet in the IDAT folder.

This is a **data extraction** step: the result is a dataframe stored at each node
(nothing is returned to the user). Analyses such as `v6-betavalues-py`,
`v6-dmp-dmr-py`, `v6-elastic-net-py` and the epigenetic clocks run on it. The IDAT
files are only available to the data extraction step of vantage6, so the quality
control and normalisation happen here; `v6-preprocessIDAT-py` is no longer needed.

### Node database

A database of type `folder` that holds the IDAT pairs (`<label>_Grn.idat`,
`<label>_Red.idat`) and the sample sheet. 450K, EPIC and EPIC v2 arrays are
supported; their pylluminator annotations are bundled in the Docker image, so the
node needs no internet access.

### Sample sheet

The IDAT folder must contain a sample sheet named `samplesheet.csv` or
`sample_sheet.csv` (or the name given in `sample_sheet_name`). Both plain CSV and
Illumina sheets with a `[Data]` section are supported. Column names are
case-insensitive. Required columns:

| Column | Description |
| --- | --- |
| `Sample_ID` | Sample identifier. Matched to the IDAT file names (`<Sample_ID>_Grn.idat`); if that fails, `Sentrix_ID` + `Sentrix_Position` are used |
| cohort column | Cohort of the sample (name set by `cohort_column`, default `cohort`). Use the same cohort codes at every hospital |

Extraction stops with an error if the IDAT folder, the sample sheet or the cohort
column is missing, if any IDAT sample is not in the sheet or has an empty cohort,
or if pylluminator returns no values for a sample. It never falls back to other data.

### Processing

pOOBAH detection p-value masking, masking of quality, control, non-CpG, SNP and
sex-chromosome probes, non-linear dye-bias correction, NOOB background correction,
then Beta and M values of the probes that pass the masks (pylluminator).

### Arguments (`data_extraction_function`)

| Argument | Default | Description |
| --- | --- | --- |
| `cohort_column` | `"cohort"` | Sample sheet column that holds the cohort |
| `sample_sheet_name` | `samplesheet.csv` or `sample_sheet.csv` | Sample sheet file name |
| `sample_columns` | none | Other sample sheet columns to add to every row, e.g. `["Age"]` (named in snake_case: `age`). Numeric columns become numbers; empty or `NA` values are kept as missing |
| `pvalue_threshold` | `0.05` | pOOBAH detection p-value threshold |
| `min_beads` | `1` | Minimum number of beads per probe |

### Output

One row per (probe, sample): `probe_id`, `sample_label`, `cohort`, `beta`,
`m_value` (float32), followed by the `sample_columns`.

Size: an EPIC v2 sample has about 870k probes after QC. 17 samples gave 14.7 million
rows, a 226 MB dataframe at the node, about 100 s and 4 GB of memory to extract.

### Tests

    python test/test_extraction.py

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