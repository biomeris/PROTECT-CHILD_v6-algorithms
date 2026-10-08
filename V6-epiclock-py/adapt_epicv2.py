"""
Convert EPIC v2 probe identifiers to the clock CpG identifiers and report the
coverage of each clock. See epiclock_v5/epicv2.py for details.

    python adapt_epicv2.py input.csv [output.csv] [--clocks horvath2013 hannum pcphenoage]
"""
from epiclock_v5.epicv2 import main

if __name__ == "__main__":
    main()
