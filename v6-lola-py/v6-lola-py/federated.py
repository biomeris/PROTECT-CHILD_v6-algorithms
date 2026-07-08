"""
This file contains all federated algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the federated task
or directly to the user (if they requested federated results).
"""

from typing import Any
from pathlib import Path
import pandas as pd
import numpy as np

from rpy2.robjects.packages import importr
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri
import rpy2.rinterface as ri

from vantage6.algorithm.tools.util import info, error
from vantage6.algorithm.decorator.action import federated


@federated
def federated_function(
    user_set_path: list[str],
    user_universe_path: str,
    region_db_path: str,
    min_overlap: int = 1,
    redefine_user_set: bool = False,
) -> Any:
    """
    This function computes overlaps between user-defined genomic region sets and a
    reference region database (LOLA), producing overlap counts for each user
    set–database set pair.

    Parameters
    ----------
    user_set_path :
        Path to the file containing the genomic regions of interest (user set) to be
        tested for enrichment or depletion.
    user_universe_path :
        Path to the file containing the genomic universe, i.e., the complete set of
        regions from which the user set was derived.
    region_db_path :
        Path to the LOLA region database directory containing the reference genomic
        region sets against which overlaps are computed.
    min_overlap : int, default=1
        Minimum number of overlapping base pairs required for two regions to
        be considered overlapping.
    redefine_user_set : bool, default=False
        Whether to redefine the user region sets with respect to the supplied
        genomic universe before computing overlaps.

    Returns
    -------
    dict
        Dictionary containing two pandas DataFrames:

        - ``scoreTable``: overlap statistics between each user region set and each
        region set in the LOLA database. Includes the overlap support counts and
        contingency table values (support, b, c, d) required for downstream
        enrichment analysis.

        - ``annotationDT``: annotation table for the LOLA region database,
        containing metadata describing each reference region set.
    """

    # Check that .bed files exist
    info("Checking that input files exist...")
    for p in user_set_path:
        if not Path(p).exists():
            error(
                f"Invalid path: {p}. "
                "Ensure the file or directory exists and the path is correct."
            )

    for p in [user_universe_path, region_db_path]:
        if not Path(p).exists():
            error(
                f"Invalid path: {p}. "
                "Ensure the file or directory exists and the path is correct."
            )

    # Import R libraries
    lola_r = importr("LOLA")
    genomic_ranges_r = importr("GenomicRanges")

    # Convert py string to R string
    r_user_set_path = ro.StrVector(user_set_path)
    r_db_path = ro.StrVector([region_db_path])
    r_universe_path = ro.StrVector([user_universe_path])

    # Read .bed files
    info("Reading .bed files...")
    if len(user_set_path) == 1:
        user_sets_r = lola_r.readBed(r_user_set_path[0])
    elif len(user_set_path) > 1:
        user_sets_r = [lola_r.readBed(ro.StrVector([p])) for p in user_set_path]
        user_sets_r = genomic_ranges_r.GRangesList(user_sets_r)
    else:
        error(
            "No input BED files found. Please provide one or more BED files to continue."
        )

    user_universe_r = lola_r.readBed(r_universe_path)

    info("Loading region DB...")
    region_db_r = lola_r.loadRegionDB(r_db_path)

    # Calculate unit set overlaps
    info("Calculating unit set overlaps...")
    res = _run_lola_overlap_count(
        user_sets_r,
        user_universe_r,
        region_db_r,
        min_overlap,
        redefine_user_set,
    )

    return {
        "scoreTable": _clean_results(res["scoreTable"]),
        "annotationDT": _clean_results(res["annotationDT"]),
    }


def _run_lola_overlap_count(
    user_sets,
    user_universe,
    region_db,
    min_overlap=1,
    redefine_user_set=False,
):
    """
    Compute overlap statistics between user genomic region sets and the LOLA
    region database using an embedded R implementation.

    This function wraps an R routine adapted from LOLA that computes overlap
    counts between the user region sets and each region set contained in the
    reference database. The resulting R objects are converted into pandas
    DataFrames.

    Parameters
    ----------
    user_sets :
        User-defined genomic region set(s) as an R ``GRanges`` or
        ``GRangesList`` object.
    user_universe :
        Genomic universe as an R ``GRanges`` object.
    region_db :
        LOLA region database loaded with ``loadRegionDB``.
    min_overlap : int, default=1
        Minimum number of overlapping base pairs required for two regions to
        be considered overlapping.
    cores : int, default=1
        Number of CPU cores to use for parallel overlap computation. Values
        greater than 1 enable multicore execution when supported.
    redefine_user_set : bool, default=False
        Whether to redefine the user region sets with respect to the supplied
        genomic universe before computing overlaps.

    Returns
    -------
    dict
        Dictionary containing:

        - ``scoreTable``: pandas DataFrame with overlap statistics between the
          user region sets and the reference database.
        - ``annotationDT``: pandas DataFrame containing the annotation metadata
          for the reference region database.
    """

    # Define R function to calculate local overlaps
    get_overlap_counts_r = ro.r("""
        function(userSets, userUniverse, regionDB, minOverlap=1, redefineUserSets=FALSE) {
                            suppressMessages(library(GenomicRanges))
                            suppressMessages(library(LOLA))
                            suppressMessages(library(data.table))

                            # -------------------------------------------
                            # Supporting function not exported from LOLA
                            # -------------------------------------------
                            setLapplyAlias = function(cores=0) {
                                if (cores < 1) {
                                    return(getOption("mc.cores"))
                                }
                                if(cores > 1) { #use multicore?
                                    if (requireNamespace("parallel", quietly = TRUE)) {
                                        options(mc.cores=cores)
                                    } else {
                                        warning("You don't have package parallel installed. Setting cores to 1.")
                                        options(mc.cores=1) #reset cores option.
                                    }
                                } else {
                                    options(mc.cores=1) #reset cores option.
                                }
                            }

                            lapplyAlias = function(..., mc.preschedule=TRUE) {
                                if (is.null(getOption("mc.cores"))) { setLapplyAlias(1) }
                                if(getOption("mc.cores") > 1) {
                                    return(parallel::mclapply(..., mc.preschedule=mc.preschedule))
                                } else {
                                    return(lapply(...))
                                }
                            }

                            countOverlapsRev = function(query, subject, ...) {
                                return(countOverlaps(subject, query, ...))
                            }

                            cleanws = function(string) {
                                return(gsub('[[:space:]]'," ", string))
                            }   

                            # -------------------------------------------
                            # Main
                            # -------------------------------------------
                            cores = 1

                            annotationDT = regionDB$regionAnno
                            testSetsGRL = regionDB$regionGRL

                            userSets = GRangesList(userSets)
                            testSetsGRL = GRangesList(testSetsGRL)

                            setLapplyAlias(cores)

                            if (any(is.null(names(testSetsGRL)))) {
                                names(testSetsGRL) = seq_along(testSetsGRL)
                            }

                            if (redefineUserSets) { #redefine user sets in terms of universe?
                                userSets =	redefineUserSets(userSets, userUniverse, cores=cores)
                                userSets = GRangesList(userSets)
                            }

                            userSetsLength = unlist(lapplyAlias((userSets), length))

                            if (! any( isDisjoint( userSets) ) ) {
                                message("You have non-disjoint userSets.")
                            }

                            ### Construct significance tests ###
                            message("Calculating unit set overlaps...")

                            # Returns for each userSet, a vector of length length(testSetsGRL), with total
                            # number of regions in that set overlapping anything in each testSetsGRL; this
                            # is then lapplied across each userSet.

                            geneSetDatabaseOverlap =
                                lapplyAlias( (userSets), countOverlapsRev, testSetsGRL, minoverlap=minOverlap)

                            # This will become "support" -- the number of regions in the
                            # userSet (which I implicitly assume is ALSO the number of regions
                            # in the universe) that overlap anything in each database set.
                            # Turn results into an overlap matrix. It is
                            # dbSets (rows) by userSets (columns), counting overlap.
                            olmat = do.call(cbind, geneSetDatabaseOverlap)
                                

                            message("Calculating universe set overlaps...")
                            # Now for each test set, how many items *in the universe* does
                            # it overlap? This will go into the calculation for c

                            #faster. Returns number of items in userUniverse.
                            testSetsOverlapUniverse = countOverlaps(testSetsGRL, userUniverse,
                                minoverlap=minOverlap)
                            # Total size of the universe
                            universeLength = length(userUniverse)

                            # To build the fisher matrix, support is 'a'

                            scoreTable = data.table(reshape2::melt(t(olmat), variable.factor=FALSE))

                            setnames(scoreTable, c("Var1", "Var2", "value"), c("userSet", "dbSet", "support"))

                            if ("factor" %in% class(scoreTable[, userSet])) {
                                scoreTable$userSet = as.character(scoreTable$userSet)
                            }

                            message("Building local tables to calculate Fisher scores...")
                            # b = the # of items *in the universe* that overlap each dbSet,
                            # less the support; This is the number of items in the universe
                            # that are in the dbSet ONLY (not in userSet)
                            # c = the size of userSet, less the support; This is the opposite:
                            # Items in the userSet ONLY (not in the dbSet)

                            scoreTable[,c("b", "c"):=list(b=testSetsOverlapUniverse[match(dbSet,
                            names(testSetsOverlapUniverse))]-support, c=userSetsLength-support)]

                            # This is the regions in the universe, but not in dbSet nor userSet.
                            scoreTable[,d:=universeLength-support-b-c]  
                            if( scoreTable[,any(b<0)] ) { # Inappropriate universe.
                                warning(cleanws("Negative b entry in table. This means either: 1) Your user sets
                                contain items outside your universe; or 2) your universe has a region that
                                overlaps multiple user set regions, interfering with the universe set overlap
                                calculation."))

                                return(scoreTable)
                            }
                            if( scoreTable[,any(c<0)] ) {
                                warning("Negative c entry in table. Bug with userSetsLength; this should not happen.")
                                return(scoreTable)
                            }
                            return(list(
                                scoreTable = scoreTable,
                                annotationDT = annotationDT
                            ))

        }

    """)

    # Run R function
    info("Calling get_overlap_counts_r...")
    res = get_overlap_counts_r(
        user_sets,
        user_universe,
        region_db,
        min_overlap,
        redefine_user_set,
    )
    info("Returned from get_overlap_counts_r")

    # Return results
    return {
        "scoreTable": pandas2ri.rpy2py_dataframe(res.rx2("scoreTable")),
        "annotationDT": pandas2ri.rpy2py_dataframe(res.rx2("annotationDT")),
    }


def _clean_results(df):
    df = df.copy()

    def clean_value(x):
        # NA R
        if x is ri.NA_Character:
            return None
        if x is ri.NA_Integer:
            return None
        if x is ri.NA_Real:
            return None
        if x is ri.NA_Logical:
            return None

        # NA pandas / numpy
        try:
            if pd.isna(x):
                return None
        except TypeError:
            pass

        # numpy scalar -> Python scalar
        if isinstance(x, np.generic):
            return x.item()

        return x

    return [
        {col: clean_value(val) for col, val in row.items()}
        for row in df.to_dict(orient="records")
    ]
