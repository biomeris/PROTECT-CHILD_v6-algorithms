# v6-lola-sandbox-py

LOLA region set enrichment of the DMRs found by
[v6-dmp-dmr-py](../v6-dmp-dmr-py), against LOLACore hg38. A copy of
[v6-lola-py](../v6-lola-py) adapted to run in the vantage6 v5 sandbox.

## What changes with respect to v6-lola-py

| | v6-lola-py | v6-lola-sandbox-py |
|---|---|---|
| User set | BED files on the node | `dmrs`: the `global_dmrs` of a DMP/DMR result, as an argument |
| Universe | BED file on the node | `universe`: the probes DMP/DMR tested (its `universe` field), as an argument |
| Reference | LOLA database folder on the node | LOLACore hg38, built into the image (`/opt/lola/LOLACore/hg38`) |
| Computation | counts on every node, summed centrally | once, on one node (`central_function` only) |
| R | must be on the node | R 4.2 + LOLA 1.28 (Bioconductor 3.16) in the image |

The three inputs are not patient-level data: the DMRs and the universe are an
aggregate result, and the reference is public. Every node would count exactly
the same overlaps, so summing the nodes' tables would multiply the counts by
the number of nodes; the overlap counts and the Fisher tests are computed once.

Before counting, the DMRs are redefined as the universe CpGs they cover
(LOLA's `redefineUserSets`), so that the user set and the universe count the
same units, tested CpGs, and the user set is part of the universe. For each
reference region set the 2x2 table is

| | in the region set | not in it |
|---|---|---|
| DMR CpGs | support | c |
| other tested CpGs | b | d |

## Function

`central_function(dmp_task_id=None, dmrs=None, universe=None, collections=None, direction="enrichment", min_overlap=1, top_n=1000)`

- `dmp_task_id`: id of a finished DMP/DMR task (2.1.0 or later). Its DMRs and
  universe are read from that task's result, so nothing has to be copied.
  Give either this or `dmrs` and `universe`; with neither, the most recent
  finished DMP/DMR task of this session is used. In the web interface the form
  asks first: result of a previous task, or upload own files.

- `dmrs`: list of records with `chromosome`, `start`, `end` (the `global_dmrs`
  of a DMP/DMR result).
- `universe`: the `universe` of the same DMP/DMR result (Parquet in base64,
  columns `probe_id`, `chromosome`, `position`), or a list of records.
- `collections`: LOLACore collections, default all of `ucsc_features`,
  `encode_tfbs`, `sheffield_dnase`, `codex`, `cistrome_cistrome`,
  `cistrome_epigenome` (the hg38 caches of LOLACoreCaches_180412;
  `encode_segmentation` has no cache there).
- `direction`: `enrichment` (one-sided Fisher, upper tail) or `depletion`.
- `top_n`: region sets returned, smallest p-value first (1 to 10,000).

Result: `input` (where the DMRs and universe came from), `summary` (DMRs, universe CpGs, DMR CpGs, region sets tested and
significant at q < 0.05), `warnings`, and `results`, one record per region set:
its LOLACore annotation (`collection`, `description`, `cellType`, `tissue`,
`antibody`, `treatment`, `dataSource`, `filename`), `support`, `b`, `c`, `d`,
`oddsRatio`, `pValue`, `qValue` (Benjamini-Hochberg over all region sets
tested), `pValueLog` and LOLA's ranks (`rnkSup`, `rnkPV`, `rnkOR`, `maxRnk`,
`meanRnk`).

## Build and test

```bash
docker build -t lola-sandbox:dev .
docker run --rm -v "$PWD/test:/app/test" -v ~/v6-sandbox-data/results:/results \
    lola-sandbox:dev python /app/test/test_compute.py /results/dmp_task27_with_universe.json
```

The test checks every count and p-value on a tiny region database against
values computed by hand, and runs a real DMP/DMR result (DMRs + universe)
against LOLACore hg38. R and LOLA are only in the image, so it runs there.

The build downloads LOLACoreCaches_180412.tgz (0.6 GB) from
big.databio.org and keeps the hg38 caches (0.25 GB).

In the sandbox:

```bash
~/v6-sandbox-data/scripts/build_push.sh . lola-sandbox:1.0.0
python ~/v6-sandbox-data/scripts/run_idat_pipeline.py lola --dmp ~/v6-sandbox-data/results/dmp_task27_with_universe.json
```

## References

Sheffield NC, Bock C (2016). LOLA: enrichment analysis for genomic region sets
and regulatory elements in R and Bioconductor. Bioinformatics 32(4):587-589.
