"""
Download the pyaging clocks into the Hugging Face cache and write clocks.json.
Used by the Dockerfile; see epiclock_v5/clock_bundle.py.

    python bundle_clocks.py --out /opt/clock_bundle [--clocks horvath2013 hannum ...] [--list]
"""
from epiclock_v5.clock_bundle import main

if __name__ == "__main__":
    main()
