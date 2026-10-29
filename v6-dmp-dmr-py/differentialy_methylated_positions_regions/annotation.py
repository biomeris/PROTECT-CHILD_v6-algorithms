"""
Genomic annotation of EPIC v2 probes (chromosome, position, genes).

The annotation comes from Illumina's EPIC v2 manifest. The full manifest is a
~930 MB CSV, so the algorithm ships only the columns it needs as a small Parquet
file (``data/epicv2_annotation.parquet``, built with ``make_annotation.py``).
``$MANIFEST_PATH`` can point to another manifest CSV or Parquet file instead.

Probes are matched on the manifest's ``IlmnID`` (the EPIC v2 identifier with its
suffix, e.g. ``cg25324105_BC11``, as produced by the IDAT extraction) and, for
identifiers without a suffix (450K / EPIC v1 style, e.g. ``cg25324105``), on
``Name``. Replicate EPIC v2 probes of one CpG share its location.
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import pandas as pd

PACKAGED_ANNOTATION = Path(__file__).parent / "data" / "epicv2_annotation.parquet"
ANNOTATION_COLUMNS = ["ilmn_id", "name", "chromosome", "position", "genes"]
MANIFEST_COLUMNS = {
    "IlmnID": "ilmn_id", "Name": "name", "CHR": "chromosome",
    "MAPINFO": "position", "UCSC_RefGene_Name": "genes",
}


def read_manifest_csv(path: str | Path) -> pd.DataFrame:
    """Read the needed columns of an Illumina manifest CSV ([Assay] section)."""
    df = pd.read_csv(path, skiprows=7, usecols=list(MANIFEST_COLUMNS), dtype=str, low_memory=False)
    df = df.rename(columns=MANIFEST_COLUMNS).dropna(subset=["ilmn_id"])
    df["chromosome"] = df["chromosome"].fillna("").str.replace("chr", "", regex=False)
    df["chromosome"] = df["chromosome"].where(df["chromosome"].isin(["", "0"]), "chr" + df["chromosome"])
    df["position"] = pd.to_numeric(df["position"], errors="coerce").fillna(0).astype("int64")
    # The manifest repeats a gene once per transcript ("DDX55;DDX55;DDX55"): keep each once
    df["genes"] = df["genes"].fillna("Intergenic").map(
        lambda genes: ";".join(dict.fromkeys(g for g in genes.split(";") if g)) or "Intergenic"
    )
    return df[ANNOTATION_COLUMNS].drop_duplicates(subset="ilmn_id").reset_index(drop=True)


@lru_cache(maxsize=2)
def load_annotation(path: str | None = None) -> pd.DataFrame:
    """The annotation table: $MANIFEST_PATH if set, else the packaged Parquet file."""
    path = path or os.environ.get("MANIFEST_PATH") or str(PACKAGED_ANNOTATION)
    if str(path).lower().endswith(".csv"):
        return read_manifest_csv(path)
    return pd.read_parquet(path, columns=ANNOTATION_COLUMNS)


def annotate(probe_ids, path: str | None = None) -> pd.DataFrame:
    """chromosome, position and genes for each probe ID (index), NaN if unknown."""
    table = load_annotation(path)
    ids = pd.Index(pd.Series(probe_ids, dtype=str).unique(), name="probe_id")
    by_id = table.set_index("ilmn_id")[["chromosome", "position", "genes"]]
    by_name = table.drop_duplicates(subset="name").set_index("name")[["chromosome", "position", "genes"]]
    result = by_id.reindex(ids)
    missing = result["chromosome"].isna()
    if missing.any():
        result.loc[missing] = by_name.reindex(ids[missing]).to_numpy()
    result.index.name = "probe_id"
    return result
