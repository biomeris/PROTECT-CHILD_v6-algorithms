""" Global variables for the efa package. """

# A node refuses to share anything if it has this many complete cases or fewer.
# Overridable at the node through the EFA_MINIMUM_NUMBER_OF_RECORDS environment
# variable.
EFA_MINIMUM_NUMBER_OF_RECORDS = 10
