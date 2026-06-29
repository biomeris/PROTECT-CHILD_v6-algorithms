"""
This file contains all federated algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the federated task
or directly to the user (if they requested federated results).
"""
import pandas as pd
from typing import Any

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.action import federated
from vantage6.algorithm.decorator.data import dataframe


@federated
@dataframe(1)
def federated_function(
    df1: pd.DataFrame, arg1
) -> Any:
    """ Decentral part of the algorithm """
    # TODO this is a simple example to show you how to return something simple.
    # Replace it by your own code
    info("Computing mean age by gender")
    result = df1[["Gender", "Age"]].groupby("Gender").mean()

    # Return results to the vantage6 server.
    # TODO make sure no privacy sensitive data is shared
    return result.to_dict()

# TODO Feel free to add more federated functions here.
