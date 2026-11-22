""" Global variables for the dunn package.

The node-side guards are overridable at each node through environment variables
of the same name; the binning settings through environment variables at the
node that runs the central function. Defaults match v6-kruskal-wallis-py.
"""

# A node holding this many eligible records or fewer refuses to contribute.
DUNN_MINIMUM_NUMBER_OF_RECORDS = 10

# A group with fewer records than this at a node is left out of every round.
DUNN_MINIMUM_GROUP_SIZE = 5

# No non-empty histogram cell below this count leaves a node; such cells are
# merged into a neighbour. Every rank bin also holds at least this many records
# per contributing node.
DUNN_MINIMUM_CELL_COUNT = 5

# Resolution of the equal-width grid the nodes report their histogram on.
DUNN_HISTOGRAM_BINS = 200

# Upper bound on the number of equal-count rank bins.
DUNN_MAXIMUM_RANK_BINS = 50

# Half-width of the reporting grid, in pooled standard deviations around the
# pooled mean. Values outside the grid are clipped into the outermost bins.
DUNN_RANGE_SD = 4.0

# Below this many rank bins the result carries an accuracy warning.
DUNN_FEW_RANK_BINS_WARNING = 10
