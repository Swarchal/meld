"""
Functions for dealing with dataframe column names
"""

import pandas as pd

def inflate_cols(dataframe, sep=" "):
    """
    Given a DataFrame with collapsed multi-index columns this will
    return a pandas DataFrame index. that can be used like so:
        df.columns = inflate_columns(df)

    Parameters:
    ------------
    dataframe: pandas.DataFrame
    sep: string (default=" ")

    Returns:
    --------
    pandas.MultiIndex
    """
    header_tuples = list(zip(*[col.split(sep) for col in dataframe.columns]))
    print(pd.DataFrame(header_tuples).T)
    return pd.MultiIndex.from_frame(pd.DataFrame(header_tuples).T)


def collapse_cols(dataframe, sep="_"):
    """
    Given a dataframe, will collapse multi-indexed columns names

    Parameters:
    -----------
    dataframe: pandas.DataFrame
    sep: string (default="_")

    Returns:
    --------
    list of collapsed column names
    """
    return [sep.join(col).strip() for col in dataframe.columns.values]
