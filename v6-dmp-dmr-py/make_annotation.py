"""
Build the packaged probe annotation from Illumina's EPIC v2 manifest.

    python make_annotation.py test/EPIC-8v2-0_A2.csv

Writes differentialy_methylated_positions_regions/data/epicv2_annotation.parquet
with the columns the algorithm needs (IlmnID, Name, chromosome, position, genes),
a few MB instead of the ~930 MB manifest.
"""
import sys

from differentialy_methylated_positions_regions.annotation import PACKAGED_ANNOTATION, read_manifest_csv

if __name__ == "__main__":
    table = read_manifest_csv(sys.argv[1])
    PACKAGED_ANNOTATION.parent.mkdir(parents=True, exist_ok=True)
    table.to_parquet(PACKAGED_ANNOTATION, index=False, compression="zstd")
    print(f"{len(table):,} probes -> {PACKAGED_ANNOTATION} "
          f"({PACKAGED_ANNOTATION.stat().st_size / 1e6:.1f} MB)")
