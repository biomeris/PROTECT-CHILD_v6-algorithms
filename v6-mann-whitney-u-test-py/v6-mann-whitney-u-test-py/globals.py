# Minimum number of eligible records a node must hold before it will
# contribute at all.
MANN_WHITNEY_MINIMUM_NUMBER_OF_RECORDS = 10

# Minimum number of records a single (column, group) cell must hold at a node
# before that cell is reported. Smaller cells are suppressed entirely.
MANN_WHITNEY_MINIMUM_GROUP_SIZE = 5

# Minimum count for any individual histogram cell that leaves a node. Non-empty
# cells below this are merged into a neighbouring cell (record count preserved).
MANN_WHITNEY_MINIMUM_CELL_COUNT = 5

# Resolution of the equal-width grid the nodes report their histogram on.
MANN_WHITNEY_HISTOGRAM_BINS = 200

# Upper bound on the number of equal-count bins the central server derives from
# the pooled histogram to assign global ranks. The real limit is usually the
# disclosure floor: every rank bin must hold at least
# MANN_WHITNEY_MINIMUM_CELL_COUNT records per contributing node.
MANN_WHITNEY_MAXIMUM_RANK_BINS = 50

# Half-width of the reporting grid, in pooled standard deviations around the
# pooled mean. Values outside the grid are clipped into the outermost bins.
MANN_WHITNEY_RANGE_SD = 4.0
