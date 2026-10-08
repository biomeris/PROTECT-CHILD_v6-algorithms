"""
Adapt Illumina EPIC v2 data to the CpG identifiers used by the epigenetic clocks.

The clocks (Horvath 2013, Hannum, PhenoAge, ...) were trained on 450K / EPIC v1
data and use plain CpG identifiers such as ``cg00000029``. EPIC v2 changed the
probe identifiers:

- every probe gets a suffix, e.g. ``cg00000029_TC21`` (``_[TB][CO]<nn>``), and
- some CpGs are measured by several replicate probes
  (``cg00000029_TC21`` and ``cg00000029_BC21``).

``adapt_long`` / ``adapt_wide`` strip the suffix and average the replicate probes
of each CpG, as ``pyaging.preprocess.epicv2_probe_aggregation`` does. Data that
already uses plain identifiers (450K, EPIC v1) is returned unchanged.

EPIC v2 also renamed some CpGs: the manifest's ``Methyl450_Loci`` / ``EPICv1_Loci``
columns link ~1,100 EPIC v2 probes to a different 450K / EPIC v1 identifier (e.g.
``cg11707779_TC21`` is ``cg14361627`` on 450K). ``load_legacy_map`` builds that
mapping from the manifest and the adapt functions use it, which recovers e.g. 1
Hannum and 109 PhenoAge CpGs. Only unambiguous links are used: the probe links to
exactly one other identifier, no other EPIC v2 CpG links to it, and it is not
itself an EPIC v2 CpG name.

The mapping built from the Illumina manifest EPIC-8v2-0_A2 (1,085 CpGs) ships with
the package (``data/epicv2_legacy_loci.json``), so nodes need no manifest.
``default_legacy_map`` picks the mapping: $MANIFEST_PATH if set (e.g. a newer
manifest), else the packaged file; ``EPICLOCK_LEGACY_MAP=none`` switches it off.
Regenerate the packaged file with
``python adapt_epicv2.py --manifest EPIC-8v2-0_A2.csv --export-legacy-map epiclock_v5/data/epicv2_legacy_loci.json``.

Some clock CpGs are not on EPIC v2 at all (e.g. 13 of the 353 Horvath 2013 CpGs).
pyaging fills missing CpGs with reference values or, for clocks without them
(e.g. Hannum), with 0, which can bias the predicted age. ``clock_coverage`` reports
which share of each clock's CpGs is present, so that results with low coverage can
be flagged and judged.

Run as a script to check or convert a file before using it:

    python adapt_epicv2.py input.csv [output.csv] [--clocks horvath2013 hannum] [--manifest EPIC-8v2-0_A2.csv]

The input may be long format (``probe_id``, ``sample_label``, ``beta`` [...]) or wide
format (one column per probe). The script prints the coverage of each clock and,
if an output path is given, writes the converted data in the same format.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
from functools import lru_cache
from pathlib import Path

import pandas as pd

EPICV2_SUFFIX = re.compile(r"_[TB][CO]\d+$")
LEGACY_COLUMNS = ("Methyl450_Loci", "EPICv1_Loci")
PACKAGED_LEGACY_MAP = Path(__file__).parent / "data" / "epicv2_legacy_loci.json"


@lru_cache(maxsize=2)
def load_legacy_map(manifest_path: str) -> dict[str, str]:
    """EPIC v2 CpG name -> 450K / EPIC v1 identifier, for CpGs that were renamed.

    Reads the Illumina EPIC v2 manifest (CSV with a [Heading] section before the
    ``IlmnID`` header line). Only unambiguous links are kept: the EPIC v2 CpG links
    to exactly one identifier different from its own name, that identifier is not
    an EPIC v2 CpG name itself, and no other EPIC v2 CpG links to it.
    """
    with open(manifest_path) as handle:
        header_line = next(i for i, line in enumerate(handle) if line.startswith("IlmnID,"))
    available = pd.read_csv(manifest_path, skiprows=header_line, nrows=0).columns
    legacy_columns = [c for c in LEGACY_COLUMNS if c in available]
    manifest = pd.read_csv(
        manifest_path, skiprows=header_line, usecols=["Name", *legacy_columns], dtype=str
    ).dropna(subset=["Name"])

    names = set(manifest["Name"])
    links = pd.concat(
        manifest[["Name", c]].dropna().set_axis(["Name", "locus"], axis=1) for c in legacy_columns
    ) if legacy_columns else pd.DataFrame(columns=["Name", "locus"])
    links = links.assign(locus=links["locus"].str.split(";")).explode("locus")
    links["locus"] = links["locus"].str.strip()
    links = links[(links["locus"] != "") & (links["locus"] != links["Name"])].drop_duplicates()

    links = links[~links["locus"].isin(names)]
    links = links[links.groupby("Name")["locus"].transform("nunique") == 1]
    links = links[links.groupby("locus")["Name"].transform("nunique") == 1]
    return dict(zip(links["Name"], links["locus"]))


def export_legacy_map(manifest_path: str, out_path: str) -> dict:
    """Write the mapping built from a manifest to a JSON file (the packaged format)."""
    mapping = load_legacy_map(manifest_path)
    content = {
        "source": os.path.basename(manifest_path),
        "created": time.strftime("%Y-%m-%d", time.gmtime()),
        "n_mappings": len(mapping),
        "mapping": dict(sorted(mapping.items())),
    }
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_path).write_text(json.dumps(content, indent=0) + "\n")
    return content


@lru_cache(maxsize=1)
def load_packaged_legacy_map() -> dict[str, str] | None:
    """The mapping shipped with the package, or None if the file is missing."""
    if not PACKAGED_LEGACY_MAP.is_file():
        return None
    return json.loads(PACKAGED_LEGACY_MAP.read_text())["mapping"]


def default_legacy_map() -> tuple[dict[str, str] | None, str]:
    """The legacy mapping to use and where it comes from.

    $EPICLOCK_LEGACY_MAP=none switches the mapping off; otherwise the manifest at
    $MANIFEST_PATH is used if set, else the mapping packaged with the algorithm.
    """
    if os.environ.get("EPICLOCK_LEGACY_MAP", "").lower() == "none":
        return None, "disabled ($EPICLOCK_LEGACY_MAP=none)"
    path = os.environ.get("MANIFEST_PATH")
    if path and os.path.isfile(path):
        return load_legacy_map(path), f"manifest {os.path.basename(path)}"
    packaged = load_packaged_legacy_map()
    if packaged is not None:
        return packaged, "packaged mapping"
    return None, "not available"


def to_clock_ids(probe_ids, legacy_map: dict[str, str] | None = None) -> pd.Series:
    """Strip the EPIC v2 suffix ('cg00000029_TC21' -> 'cg00000029') and, if a legacy
    mapping is given, rename renamed CpGs to their 450K / EPIC v1 identifier."""
    ids = pd.Series(probe_ids, dtype=str).str.replace(EPICV2_SUFFIX, "", regex=True)
    if legacy_map:
        ids = ids.map(lambda cpg: legacy_map.get(cpg, cpg))
    return ids


def adapt_long(
    df: pd.DataFrame,
    sample_column: str = "sample_label",
    value_columns: tuple[str, ...] = ("beta", "m_value"),
    legacy_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Long format (one row per sample and probe) -> clock CpG identifiers.

    Replicate probes of the same CpG are averaged per sample (each value column
    separately). All other columns must have one value per sample (e.g. cohort).
    """
    if df.duplicated([sample_column, "probe_id"]).any():
        raise ValueError("The data has duplicated (sample, probe_id) rows")
    value_columns = [c for c in value_columns if c in df.columns]
    df = df.assign(probe_id=to_clock_ids(df["probe_id"], legacy_map).to_numpy())
    if not df.duplicated([sample_column, "probe_id"]).any():
        return df

    other = [c for c in df.columns if c not in value_columns + [sample_column, "probe_id"]]
    averaged = df.groupby([sample_column, "probe_id"], sort=False)[value_columns].mean()
    if other:
        per_sample = df[[sample_column] + other].drop_duplicates()
        if per_sample[sample_column].duplicated().any():
            raise ValueError(f"Columns {other} must have a single value per sample")
        averaged = averaged.join(per_sample.set_index(sample_column), on=sample_column)
    return averaged.reset_index()[list(df.columns)]


def adapt_wide(df: pd.DataFrame, legacy_map: dict[str, str] | None = None) -> pd.DataFrame:
    """Wide format (one column per probe) -> clock CpG identifiers.

    Columns whose name is not a probe identifier are kept as they are.
    """
    renamed = df.set_axis(to_clock_ids(df.columns, legacy_map).to_numpy(), axis=1)
    duplicated = renamed.columns[renamed.columns.duplicated()].unique()
    if len(duplicated) == 0:
        return renamed
    means = {cpg: renamed.loc[:, renamed.columns == cpg].mean(axis=1) for cpg in duplicated}
    result = renamed.loc[:, ~renamed.columns.duplicated()].copy()
    for cpg, values in means.items():
        result[cpg] = values
    return result


def clock_features(clock: str, pyaging_dir: str = "pyaging_data") -> list[str]:
    """CpG identifiers used by a pyaging clock (downloads the clock if needed)."""
    from pyaging.logger import LoggerManager
    from pyaging.predict._pred_utils import load_clock

    model = load_clock(clock, "cpu", pyaging_dir, LoggerManager.gen_logger("clock_features"), indent_level=1)
    return list(model.features)


def clock_coverage(cpg_ids, clocks: list[str], pyaging_dir: str = "pyaging_data") -> pd.DataFrame:
    """Share of each clock's CpGs present in ``cpg_ids`` (clock identifiers)."""
    available = set(cpg_ids)
    rows = []
    for clock in clocks:
        features = clock_features(clock, pyaging_dir)
        missing = [f for f in features if f not in available]
        rows.append({
            "clock": clock,
            "n_features": len(features),
            "n_present": len(features) - len(missing),
            "coverage": 1 - len(missing) / len(features),
            "missing_examples": ";".join(missing[:5]),
        })
    return pd.DataFrame(rows)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Adapt EPIC v2 probe identifiers to the clock CpG identifiers.")
    parser.add_argument("input", nargs="?", help="CSV in long (probe_id, sample_label, beta...) or wide format")
    parser.add_argument("output", nargs="?", help="where to write the converted CSV (optional)")
    parser.add_argument("--clocks", nargs="+", default=["horvath2013", "hannum", "pcphenoage"])
    parser.add_argument("--sample-column", default="sample_label")
    parser.add_argument("--pyaging-dir", default="pyaging_data")
    parser.add_argument(
        "--manifest", default=os.environ.get("MANIFEST_PATH"),
        help="EPIC v2 manifest CSV; maps renamed CpGs to their 450K/EPIC v1 identifier "
             "(default: $MANIFEST_PATH, else the mapping packaged with the algorithm)",
    )
    parser.add_argument(
        "--export-legacy-map", metavar="JSON",
        help="write the mapping built from --manifest to this JSON file and exit",
    )
    args = parser.parse_args(argv)

    if args.export_legacy_map:
        if not args.manifest:
            parser.error("--export-legacy-map needs --manifest")
        content = export_legacy_map(args.manifest, args.export_legacy_map)
        print(f"Written {content['n_mappings']} mappings from {content['source']} to {args.export_legacy_map}")
        return
    if not args.input:
        parser.error("input is required")

    if args.manifest:
        legacy_map, source = load_legacy_map(args.manifest), f"manifest {os.path.basename(args.manifest)}"
    else:
        legacy_map, source = default_legacy_map()
    if legacy_map is None:
        print(f"Renamed EPIC v2 CpGs: {source}; only the EPIC v2 suffixes are stripped")
    else:
        print(f"Renamed EPIC v2 CpGs: {len(legacy_map)} mapped to their 450K/EPIC v1 identifier ({source})")

    df = pd.read_csv(args.input)
    if "probe_id" in df.columns:
        adapted = adapt_long(df, sample_column=args.sample_column, legacy_map=legacy_map)
        cpg_ids = adapted["probe_id"].unique()
        n_before, n_after = df["probe_id"].nunique(), len(cpg_ids)
    else:
        adapted = adapt_wide(df, legacy_map=legacy_map)
        cpg_ids = adapted.columns
        n_before, n_after = df.shape[1], adapted.shape[1]
    print(f"{n_before} probes -> {n_after} CpG identifiers")

    coverage = clock_coverage(cpg_ids, args.clocks, args.pyaging_dir)
    print(coverage.to_string(index=False, float_format=lambda v: f"{v:.3f}"))

    if args.output:
        adapted.to_csv(args.output, index=False)
        print(f"Written {args.output}")


if __name__ == "__main__":
    main()
