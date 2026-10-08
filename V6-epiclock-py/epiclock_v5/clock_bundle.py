"""
Bundle pyaging clocks into the Docker image so nodes never download anything.

pyaging (>= 0.5) downloads each clock from the Hugging Face Hub the first time it is
used. vantage6 nodes usually run algorithms without internet access, so the image
build downloads the clocks into the Hugging Face cache inside the image
(``$HF_HOME``) and the image then runs with ``HF_HUB_OFFLINE=1``. Every node uses
exactly the same clock files, fixed by the image version.

The bundle holds every human DNA-methylation clock in pyaging's catalogue except
the SystemsAge family (12 clocks of ~2 GB each, trained on older adults); that is
142 clocks, ~2.5 GB. ``clocks.json`` records, per clock, the Hugging Face revision
that was downloaded, the file size, the number of features, the non-CpG inputs it
needs (e.g. age and sex for GrimAge) and the catalogue metadata (tissue,
population, platform), so the central function can check requested clocks and
report what each clock needs.

Build (run inside the Dockerfile; see README):

    python bundle_clocks.py --out /opt/clock_bundle [--clocks horvath2013 hannum ...]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
from pathlib import Path

BUNDLE_ENV = "EPICLOCK_CLOCK_BUNDLE"
EXCLUDED_PREFIXES = ("systemsage",)
CPG_PATTERN = re.compile(r"^(cg|ch\.|ch\d|rs|nv)", re.IGNORECASE)
METADATA_FIELDS = ("year", "tissue", "population", "platform", "predicts", "unit", "research_only", "citation")


def _catalogue() -> dict:
    """pyaging's clock catalogue (downloaded from the Hub)."""
    import pyaging as pya
    from pyaging.logger import LoggerManager

    return pya.utils.load_clock_metadata("pyaging_data", LoggerManager.gen_logger("clock_bundle"))


def bundle_clock_names(catalogue: dict) -> list[str]:
    """Human DNA-methylation clocks, without the SystemsAge family."""
    return sorted(
        name for name, meta in catalogue.items()
        if meta.get("species") == "Homo sapiens"
        and meta.get("data_type") == "DNA methylation"
        and not name.startswith(EXCLUDED_PREFIXES)
    )


def _download_and_describe(clock: str) -> dict:
    """Download one clock into the HF cache, load it and describe it."""
    from pyaging.utils._hf import download_clock_weights
    import torch

    path = download_clock_weights(clock)
    model = torch.load(path, weights_only=False, map_location="cpu")
    features = list(model.features)
    extra = [f for f in features if not CPG_PATTERN.match(str(f))]
    revision = Path(path).parent.name  # .../snapshots/<commit>/<clock>.pt
    return {
        "revision": revision,
        "size_bytes": os.path.getsize(path),
        "n_features": len(features),
        "extra_features": extra,
        "reference_values": getattr(model, "reference_values", None) is not None,
    }


def build_bundle(out_dir: str, clocks: list[str] | None = None) -> dict:
    """Download the clocks and write ``<out_dir>/clocks.json``."""
    catalogue = _catalogue()
    names = clocks or bundle_clock_names(catalogue)
    bundle, failed = {}, {}
    start = time.time()
    for i, clock in enumerate(names, start=1):
        try:
            entry = _download_and_describe(clock)
        except Exception as exc:  # report and continue: one broken clock must not stop the build
            failed[clock] = f"{type(exc).__name__}: {exc}"
            print(f"[{i}/{len(names)}] {clock}: FAILED ({failed[clock][:120]})", flush=True)
            continue
        meta = catalogue.get(clock, {})
        entry.update({k: meta.get(k) for k in METADATA_FIELDS if k in meta})
        bundle[clock] = entry
        print(f"[{i}/{len(names)}] {clock}: {entry['size_bytes'] / 1e6:.1f} MB, "
              f"{entry['n_features']} features"
              + (f", needs {entry['extra_features']}" if entry["extra_features"] else ""), flush=True)

    manifest = {
        "pyaging_version": __import__("pyaging").__version__,
        "built": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "n_clocks": len(bundle),
        "total_bytes": sum(c["size_bytes"] for c in bundle.values()),
        "clocks": bundle,
        "failed": failed,
    }
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    (Path(out_dir) / "clocks.json").write_text(json.dumps(manifest, indent=1, default=str))
    print(f"Bundled {len(bundle)} clocks ({manifest['total_bytes'] / 1e9:.2f} GB) in "
          f"{time.time() - start:.0f}s; {len(failed)} failed")
    return manifest


def load_bundle() -> dict | None:
    """The bundle manifest at $EPICLOCK_CLOCK_BUNDLE, or None (e.g. local development)."""
    path = os.environ.get(BUNDLE_ENV)
    if path and os.path.isfile(path):
        return json.loads(Path(path).read_text())
    return None


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Download pyaging clocks into the HF cache and write clocks.json")
    parser.add_argument("--out", required=True, help="directory for clocks.json")
    parser.add_argument("--clocks", nargs="+", help="only these clocks (default: all human DNAm clocks except SystemsAge)")
    parser.add_argument("--list", action="store_true", help="only print the clocks that would be bundled")
    args = parser.parse_args(argv)
    if args.list:
        names = bundle_clock_names(_catalogue())
        print(f"{len(names)} clocks:\n" + " ".join(names))
        return
    manifest = build_bundle(args.out, args.clocks)
    if manifest["failed"]:
        raise SystemExit(f"{len(manifest['failed'])} clock(s) failed to bundle: {sorted(manifest['failed'])}")


if __name__ == "__main__":
    main()
