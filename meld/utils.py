"""utility functions"""

import numpy as np
import pandas as pd


# TODO merge by index
def aggregate(data, on, method="median", **kwargs):
    """
    Aggregate dataset

    Parameters
    -----------
    data : pandas DataFrame
        DataFrame
    on : string or list of strings
        column(s) with which to group by and aggregate the dataset.
    method : string (default="median")
        method to average each group. options = "median","mean" or "sum"
    **kwargs : additional args to utils.get_metadata / utils.get_featuredata

    Returns
    -------
    agg_df : pandas DataFrame
        aggregated dataframe, with a row per value of 'on'
    """
    _check_inputs(data, on, method)
    _check_featuredata(data, on, **kwargs)
    # keep track of original column order
    df_cols = data.columns.tolist()
    grouped = data.groupby(on, as_index=False)
    if method == "mean":
        agg = grouped.aggregate(np.mean)
    if method == "median":
        agg = grouped.aggregate(np.median)
    if method == "sum":
        agg = grouped.aggregate(np.sum)
    df_metadata = data[get_metadata(data, **kwargs)].copy()
    # add indexing column to metadata if not already present
    df_metadata[on] = data[on]
    # drop metadata to the same level as aggregated data
    df_metadata.drop_duplicates(subset=on, inplace=True)
    # merge aggregated and feature data
    # FIXME merge by index rather than suffix bodge
    merged_df = pd.merge(agg, df_metadata, on=on, how="outer",
                         suffixes=("remove_me", ""))
    # re-arrange so that the columns are in the original order
    # also doubles to check that no columns are missing or changed names
    merged_df = merged_df[df_cols]
    assert len(merged_df.columns) == len(data.columns)
    return merged_df


def _check_inputs(data, on, method):
    """ internal function for aggregate() to check validity of inputs """
    valid_methods = ["median", "mean"]
    if not isinstance(data, pd.DataFrame):
        raise ValueError("not a a pandas DataFrame")
    if method not in valid_methods:
        msg = "{} is not a valid method, options: median or mean".format(method)
        raise ValueError(msg)
    df_columns = data.columns.tolist()
    if isinstance(on, str):
        if on not in df_columns:
            raise ValueError("{} not a column in df".format(on))
    elif isinstance(on, list):
        for col in on:
            if col not in df_columns:
                raise ValueError("{} not a column in df".format(col))


def _check_featuredata(data, on, **kwargs):
    """
    Check feature data is numerical
    """
    feature_cols = get_featuredata(data, **kwargs)
    cols_to_check = [col for col in feature_cols if col not in [on]]
    df_to_check = data[cols_to_check]
    is_number = np.vectorize(lambda x: np.issubdtype(x, np.number))
    if all(is_number(df_to_check.dtypes)) is False:
        # find which metadata columns are non-numeric
        bad_cols = []
        for col in df_to_check.columns:
            if not np.issubdtype(df_to_check[col], np.number):
                bad_cols.append(col)
        msg = "non-numeric column found in feature data : {}".format(bad_cols)
        raise ValueError(msg)


def get_featuredata(data, metadata_string="Metadata", prefix=False):
    """
    identifies columns in a dataframe that are not labelled with the
    metadata prefix. Its assumed everything not labelled metadata is
    featuredata

    Parameters
    ----------
    data : pandas DataFrame
        DataFrame
    metadata_string : string (default="Metadata")
        string that denotes a column is a metadata column
    prefix: boolean (default=False)
        if True, then only columns that are prefixed with metadata_string are
        selected as metadata. If False, then any columns that contain the
        metadata_string are selected as metadata columns

    Returns
    -------
    f_cols : list
        List of feature column labels
    """
    if prefix:
        f_cols = [i for i in data.columns if not i.startswith(metadata_string)]
    elif prefix is False:
        f_cols = [i for i in data.columns if metadata_string not in i]
    return f_cols


def get_metadata(data, metadata_string="Metadata", prefix=False):
    """
    identifies column in a dataframe that are labelled with the metadata_prefix

    Parameters
    ----------
    data : pandas DataFrame
        DataFrame

    metadata_string : string (default="Metadata")
        string that denotes a column is a metadata column

    prefix: boolean (default=False)
        if True, then only columns that are prefixed with metadata_string are
        selected as metadata. If False, then any columns that contain the
        metadata_string are selected as metadata columns

    Returns
    -------
    m_cols : list
        list of metadata column labels
    """
    if prefix:
        m_cols = [i for i in data.columns if i.startswith(metadata_string)]
    elif prefix is False:
        m_cols = [i for i in data.columns if metadata_string in i]
    return m_cols

