"""
This file contains all data preprocessing algorithm functions.

Data preprocessing functions can be used to preprocess the data extracted from the
databases. For instance, you can bin data into categories, or remove outliers.
"""
from typing import Any
import pandas as pd

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.action import preprocessing

@preprocessing
def data_preprocessing_function(
    df1: pd.DataFrame, arg1
) -> Any:
    """ This function preprocesses the data by ..."""
    # TODO this is a simple example to show you how to write a data preprocessing function.
    # Replace it by your own code. Example adds a new BMI column based on height and
    # weight.
    df1["BMI"] = df1["Weight"] / (df1["Height"] ** 2)
    return df1

# TODO Feel free to add more data preprocessing functions here.