""" Global variables for the manova package. """

# A node refuses to share anything if it has this many complete cases or fewer
# (after small groups have been suppressed). Overridable at the node through the
# MANOVA_MINIMUM_NUMBER_OF_RECORDS environment variable.
MANOVA_MINIMUM_NUMBER_OF_RECORDS = 10

# A group with fewer local complete cases than this is left out of the node's
# summary: with very few records, the per-group sum and X^T X come close to
# revealing the individual records. Overridable at the node through the
# MANOVA_MINIMUM_GROUP_SIZE environment variable.
MANOVA_MINIMUM_GROUP_SIZE = 5
