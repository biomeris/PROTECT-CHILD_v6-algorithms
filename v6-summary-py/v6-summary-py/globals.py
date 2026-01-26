""" Global variables for the summary package. """

from enum import Enum

# names of environment variables
## minimum number of rows in the dataframe
ENVVAR_MINIMUM_ROWS = "SUMMARY_MINIMUM_ROWS"
## whitelist of columns allowed to be requested
ENVVAR_ALLOWED_COLUMNS = "SUMMARY_ALLOWED_COLUMNS"
## blacklist of columns not allowed to be requested
ENVVAR_DISALLOWED_COLUMNS = "SUMMARY_DISALLOWED_COLUMNS"
## privacy threshold for count of a unique value in a categorical column
ENVVAR_PRIVACY_THRESHOLD = "SUMMARY_PRIVACY_THRESHOLD"


class EnvVarsAllowed(Enum):
    """Environment varible names to allow computation of different variables"""

    ALLOW_MIN = "SUMMARY_ALLOW_MIN"
    ALLOW_MAX = "SUMMARY_ALLOW_MAX"
    ALLOW_COUNT = "SUMMARY_ALLOW_COUNT"
    ALLOW_SUM = "SUMMARY_ALLOW_SUM"
    ALLOW_MISSING = "SUMMARY_ALLOW_MISSING"
    ALLOW_VARIANCE = "SUMMARY_ALLOW_VARIANCE"
    ALLOW_COUNTS_UNIQUE_VALUES = "SUMMARY_ALLOW_COUNTS_UNIQUE_VALUES"
    ALLOW_NUM_COMPLETE_ROWS = "SUMMARY_ALLOW_NUM_COMPLETE_ROWS"


# default values for environment variables
DEFAULT_MINIMUM_ROWS = 5
DEFAULT_PRIVACY_THRESHOLD = 5
