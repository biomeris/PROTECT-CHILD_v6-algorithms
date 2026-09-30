"""
DATA EXTRACTION functions (vantage6 v5 "Uluru").

In v5, data extraction is a separate session step: it reads from the node's
database and builds a dataframe that is reused during compute.

For plain CSV (feature columns + a binary outcome column) vantage6's built-in
`read_csv` extractor is enough, re-exported in __init__.py — no custom
function is needed here. Unused columns are simply ignored during compute.

If production needs source-specific logic (column filtering, SQL, SPARQL...),
add a function here, e.g.:

    from vantage6.algorithm.decorator.action import data_extraction

    @data_extraction
    def read_filtered(connection_details: dict, drop_columns=None):
        df = pd.read_csv(connection_details["uri"])
        if drop_columns:
            df = df.drop(columns=drop_columns)
        return df
"""
# we can add custom functions here if we need to