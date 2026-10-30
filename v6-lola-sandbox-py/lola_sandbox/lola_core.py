"""
LOLA overlap counting in R (via rpy2), on regions given as tables.

The R routine is adapted from LOLA's runLOLA (as in v6-lola-py): for every user set
and every region set of the reference database it counts the 2x2 table used by
the Fisher test:

    support  user regions overlapping the region set
    b        universe regions overlapping the region set, minus support
    c        user regions not overlapping the region set
    d        universe regions in neither

The user set must be part of the universe; ``run_lola`` therefore first redefines
the DMRs as the universe CpGs they cover (LOLA's redefineUserSets), so that user
set and universe count the same units: tested CpGs.
"""
from __future__ import annotations

from functools import lru_cache

import pandas as pd

# R code: counts per (user set, db set); returns the score table and the annotation
_COUNT_OVERLAPS_R = """
function(userSets, userUniverse, regionDB, minOverlap=1) {
    suppressMessages(library(GenomicRanges))
    suppressMessages(library(LOLA))
    suppressMessages(library(data.table))

    annotationDT = regionDB$regionAnno
    testSetsGRL = GRangesList(regionDB$regionGRL)
    if (any(is.null(names(testSetsGRL)))) {
        names(testSetsGRL) = seq_along(testSetsGRL)
    }
    userSets = GRangesList(userSets)
    if (is.null(names(userSets))) {
        names(userSets) = seq_along(userSets)
    }
    userSetsLength = unlist(lapply(userSets, length))

    countOverlapsRev = function(query, subject, ...) countOverlaps(subject, query, ...)
    # (region sets without a chromosome of the universe only warn that they have none in common)
    olmat = suppressWarnings(do.call(cbind, lapply(userSets, countOverlapsRev, testSetsGRL, minoverlap=minOverlap)))
    testSetsOverlapUniverse = suppressWarnings(countOverlaps(testSetsGRL, userUniverse, minoverlap=minOverlap))
    universeLength = length(userUniverse)

    scoreTable = data.table(reshape2::melt(t(olmat), variable.factor=FALSE))
    setnames(scoreTable, c("Var1", "Var2", "value"), c("userSet", "dbSet", "support"))
    scoreTable$userSet = as.character(scoreTable$userSet)
    scoreTable[, c("b", "c") := list(
        b = testSetsOverlapUniverse[match(dbSet, names(testSetsOverlapUniverse))] - support,
        c = userSetsLength[userSet] - support)]
    scoreTable[, d := universeLength - support - b - c]
    list(scoreTable = as.data.frame(scoreTable), annotationDT = as.data.frame(annotationDT))
}
"""


def _r():
    """rpy2 modules, imported lazily so the package can be imported without R."""
    import rpy2.robjects as ro
    from rpy2.robjects import pandas2ri
    from rpy2.robjects.conversion import localconverter
    from rpy2.robjects.packages import importr
    return ro, pandas2ri, localconverter, importr


@lru_cache(maxsize=4)
def load_region_db(db_path: str, collections: tuple[str, ...] | None = None):
    """Load a LOLA region database (cached per process; LOLACore caches load in seconds)."""
    ro, _, _, importr = _r()
    lola = importr("LOLA")
    if collections:
        return lola.loadRegionDB(db_path, collections=ro.StrVector(list(collections)))
    return lola.loadRegionDB(db_path)


def _granges(chromosome, start, end):
    ro, _, _, importr = _r()
    gr = importr("GenomicRanges")
    iranges = importr("IRanges")
    return gr.GRanges(
        seqnames=ro.StrVector([str(c) for c in chromosome]),
        ranges=iranges.IRanges(start=ro.IntVector([int(s) for s in start]),
                               end=ro.IntVector([int(e) for e in end])),
    )


def run_lola(
    user_regions: pd.DataFrame,
    universe: pd.DataFrame,
    db_path: str,
    collections: tuple[str, ...] | None = None,
    min_overlap: int = 1,
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """Count LOLA overlaps of the user regions (e.g. DMRs) against a region database.

    user_regions: columns chromosome, start, end (one row per region, e.g. per DMR)
    universe: columns chromosome, position (one row per tested CpG)
    Returns (score table, database annotation, number of universe CpGs in the user set).
    """
    ro, pandas2ri, localconverter, importr = _r()
    lola = importr("LOLA")

    universe_gr = _granges(universe["chromosome"], universe["position"], universe["position"])
    dmr_gr = _granges(user_regions["chromosome"], user_regions["start"], user_regions["end"])
    # The user set as the universe CpGs the DMRs cover: user set and universe both
    # count tested CpGs, and the user set is part of the universe
    user_sets = lola.redefineUserSets(importr("GenomicRanges").GRangesList(dmr_gr), universe_gr)
    n_user_cpgs = int(ro.r["length"](user_sets[0])[0])

    region_db = load_region_db(db_path, collections)
    result = ro.r(_COUNT_OVERLAPS_R)(user_sets, universe_gr, region_db, min_overlap)
    with localconverter(ro.default_converter + pandas2ri.converter):
        score_table = ro.conversion.get_conversion().rpy2py(result.rx2("scoreTable"))
        annotation = ro.conversion.get_conversion().rpy2py(result.rx2("annotationDT"))
    annotation = pd.DataFrame(annotation)
    # R's missing strings arrive as the text "NA_character_"
    annotation = annotation.where(annotation.astype(str) != "NA_character_")
    return pd.DataFrame(score_table), annotation, n_user_cpgs
