
# v6-kruskal-wallis-py

Federated Kruskal-Wallis test for non-parametric comparison of two or more independent groups across multiple organizations without sharing raw data.

This algorithm is designed to be run with the [vantage6](https://vantage6.ai)
infrastructure for distributed analysis and learning.

The base code for this algorithm has been created via the
[v6-algorithm-template](https://github.com/vantage6/v6-algorithm-template)
template generator.

## How raw data is kept at the node

A rank-based test needs a *global* ordering of observations, which is normally
obtained by sending every value to the central server. This algorithm never
does that. It runs three rounds, and each round transmits only aggregates.

**Round 1 — `partial_moments`.** Each node returns, per column, three numbers:
the record count, the sum, and the sum of squares. The central server pools
these into a global mean and standard deviation and uses them to lay out a
shared reporting grid spanning `mean ± 4 sd`.

**Round 2 — `partial_histogram`.** Each node places its records into that grid
and returns *record counts per bin*, pooled across groups (group-blind). The
central server sums the counts across nodes and merges adjacent bins into
roughly equal-count **rank bins**, then computes each rank bin's global
mid-rank.

**Round 3 — `partial_rank_scores`.** Each node assigns its records the mid-rank
of the bin they fall into and returns, per group, three numbers: the sum of
those scores, their sum of squares, and the record count. The H statistic
follows from those alone.

So the complete payload leaving a node, every round, is a handful of counts,
sums and sums of squares — never an individual value, and never the raw sorted
column that earlier versions of this algorithm transmitted.

### Disclosure control

| Setting | Default | Effect |
| --- | --- | --- |
| `KRUSKAL_WALLIS_MINIMUM_NUMBER_OF_RECORDS` | 10 | A node holding this many records or fewer refuses to contribute at all. |
| `KRUSKAL_WALLIS_MINIMUM_GROUP_SIZE` | 5 | A (column, group) cell smaller than this is dropped from every round. |
| `KRUSKAL_WALLIS_MINIMUM_CELL_COUNT` | 5 | No individual histogram cell below this count is ever transmitted. |

### Accuracy of the approximation

H is computed in its general-scores form, `H = (N-1) × SSB / SST`, which
reproduces `scipy.stats.kruskal` exactly (including its tie correction) when
fed true ranks — `test/test_math.py` checks this to floating-point precision.
Binning introduces an approximation on top of that, and how large it is
depends heavily on sample size:

| Total eligible records | Typical H accuracy | Significance-flip rate at α=0.05 |
| --- | --- | --- |
| ~400 (e.g. 200/node × 2 nodes) | within 1–5% | ~1% |
| ~36 (e.g. 18/node × 2 nodes) | 10th–90th percentile 0.32×–2.73× the exact H | ~16% |

The reason is the same minimum-cell-count privacy floor described above: every
rank bin must hold enough records per node to be non-disclosive, so at small
total sample sizes there are necessarily very few, very wide rank bins, and
the statistic becomes correspondingly coarse. This is an inherent trade-off of
computing the test without centralizing the data, not a defect — `central()`
logs a warning whenever a column resolves to fewer than 10 rank bins, so this
condition is visible rather than silent. If your total sample size per test is
in the tens rather than the hundreds, treat the result as indicative of
direction and rough magnitude, not as a precise p-value.

## Testing

```bash
# statistics only — needs numpy and scipy, not vantage6
python test/test_math.py

# end-to-end against the vantage6 mock client
python test/test.py
```

### Dockerizing your algorithm

To finally run your algorithm on the vantage6 infrastructure, you need to
create a Docker image of your algorithm.

A Docker image can be created by executing the following command in the root of your
algorithm directory:

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
`docker tag local_kruskal_wallis_algorithm harbor2.vantage6.ai/algorithms/kruskal-wallis` to
make the algorithm available on a remote Docker registry (in this case
`harbor2.vantage6.ai`).

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
