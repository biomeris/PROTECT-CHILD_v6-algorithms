import pandas as pd
import pandas.api.types as ptypes

from vantage6.algorithm.tools.util import info, get_env_var
from vantage6.algorithm.tools.decorators import data
from vantage6.algorithm.tools.exceptions import InputError
from .globals import T_TEST_MINIMUM_NUMBER_OF_RECORDS


@data(1)
def partial(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    group_col: str | None = None,   
) -> dict:
    """
    Compute the mean and the sample variance of a column for a single data station to
    share with the aggregator part of the algorithm. If 'group_col' is passed, then it
    returns the same statistics for each group.

    Parameters
    ----------
    df : pd.DataFrame
        The data for the data station
    columns : list[str] | None
        The columns to compute the mean and sample variance for. The columns must be
        numeric. If not provided, all numeric columns are included.

    Returns
    -------
    dict
        The mean, the number of observations and the sample variance for the data
        station.
    """

    info("Checking number of records in the DataFrame.")
    MINIMUM_NUMBER_OF_RECORDS = get_env_var(
        "T_TEST_MINIMUM_NUMBER_OF_RECORDS",
        T_TEST_MINIMUM_NUMBER_OF_RECORDS,
        as_type="int",
    )
    if len(df) <= MINIMUM_NUMBER_OF_RECORDS:
        raise InputError(
            "Number of records in 'df' must be greater than "
            f"{MINIMUM_NUMBER_OF_RECORDS}."
        )

    if not columns:
        columns = df.select_dtypes(include=["number"]).columns.tolist()
    else:
        # Check that column names exist in the dataframe
        non_existing_columns = [col for col in columns if col not in df.columns]
        if non_existing_columns:
            raise InputError(
                f"Columns {non_existing_columns} do not exist in the dataframe"
            )
        # Check that columns are numerical
        non_numeric_columns = [
            col for col in columns if not ptypes.is_numeric_dtype(df[col])
        ]
        if non_numeric_columns:
            raise InputError(f"Columns {non_numeric_columns} are not numeric")

    def summarize(frame: pd.DataFrame) -> dict:
        partial_results = {}
        # Mean and total count (N)
        for col in columns:
            info(f"Computing mean and sample variance for {col}")
            count = frame[col].count()
            # Check if count is not equal to 0 or 1 to avoid division by 0
            if count == 0 or count == 1:
                info(f"Skipping {col} due to insufficient data.")
                continue
            column_sum = frame[col].sum()
            average = column_sum / count
            # Sample variance
            info(f"Computing sample variance for {col}")
            ssd = ((df[col].astype(float) - average) ** 2).sum()
            variance = ssd / (count - 1)
    
            partial_results[col] = {
                "average": float(average),
                "count": float(count),
                "variance": float(variance),
            }

        return partial_results

    if group_col:
        if group_col not in df.columns:
            raise InputError(f"Group column '{group_col}' not in dataframe")

        grouped = {}
        for g, sub in df.groupby(group_col):
            grouped[str(g)] = summarize(sub)
        return grouped
    else:
        return summarize(df)
    