"""
Class to merge output files to tables within an sqlite database
"""

import os
import sys
import warnings
import pandas as pd
import sqlalchemy
from meld import colfuncs
from meld import utils


class Merger(object):

    """
    Collect and merge distributed cellprofiler results into an sqlite database.

    Methods
    -------
    create_db :
        creates an sqlite database in the results directory
        directory - string, top level directory containing cellprofiler output
    to_db :
        loads the csv files in the directory and writes them as tables to the
        sqlite database created by create_db
    to_db_agg :
        like to_db, but aggregates the data on a specified column
    """

    def __init__(self, directory):
        """
        Get full filepaths of all files in a directory, including
        sub-directories.

        Parameters:
        ------------
        directory: string
            Path to results directory containing sub-directories of results

        Returns:
        ---------
        Nothing
        """
        if not os.path.isdir(directory):
            raise NotADirectoryError("{} is not a directory".format(directory))
        file_paths = []
        for root, _, files in os.walk(directory):
            for filename in files:
                filepath = os.path.join(root, filename)
                file_paths.append(filepath)
            self.file_paths = file_paths
        if len(self.file_paths) == 0:
            raise RuntimeError("{} does not contain any files".format(directory))
        self.db_handle = None
        self.engine = None

    def create_db(self, location, db_name="results"):
        """
        Creates an sqlite database named `db_name` at `location`.

        Parameters:
        -----------
        location : string
            filepath to directory in which the database will be created.
        db_name : string (default="results")
            What to call the database at location

        Returns:
        --------
        Nothing

        Note:
        ------
        If a database already exists with the same location and name then
        this will act on the existing database rather than overwriting.
        If the database contains existing tables of the same name, then these
        will be appended to if possible. If they have different column names,
        then an sqlalchemy error will be returned.
        """
        if not db_name.lower().endswith((".sqlite", ".sqlite3")):
            db_name = "{}.sqlite".format(db_name)
        db_path = os.path.join(location, db_name)
        if os.path.isfile(db_path):
            msg = "{}' already exists, database will be extended".format(db_path)
            warnings.warn(msg)
        self.db_handle = "sqlite:///{}".format(db_path)
        self.engine = sqlalchemy.create_engine(self.db_handle)

    def to_db(self, select="DATA", header=0, **kwargs):
        """
        Append files to a database table.

        Parameters
        -----------
        select : string
            the name of the .csv file, this will also be the database table
        header : int or list
            the number of header rows, i.e. rows of column names.
        **kwargs : additional arguments to pandas.read_csv

        Returns:
        --------
        Nothing, writes to databases or raises an Error

        Note:
        ------
        This will attempt to append to a database table if one exists with the
        same name and the same column headers. The an existing database table
        under the same name exists, but has different column headers then an
        sqlalchemy error will be raised.
        """
        self.check_database()
        file_name = self.get_file_name(select)
        table_name = self.get_table_name(select)
        # filter files
        file_paths = [f for f in self.file_paths if f.endswith(file_name)]
        # check there are files matching file_name argument
        if len(file_paths) == 0:
            raise ValueError("No files found matching '{}'".format(file_name))
        for indv_file in file_paths:
            if header == 0 or header == [0]:
                # dont need to collapse headers
                tmp_file = pd.read_csv(indv_file, header=0, chunksize=10000,
                                       iterator=True, **kwargs)
                all_file = pd.concat(tmp_file)
                all_file.to_sql(table_name, con=self.engine, index=False,
                                if_exists="append")
            else:
                # have to collapse columns
                tmp_file = pd.read_csv(indv_file, header=header, chunksize=10000,
                                       iterator=True, **kwargs)
                all_file = pd.concat(tmp_file)
                # collapse column names if multi-indexed
                if isinstance(all_file.columns, pd.core.index.MultiIndex):
                    all_file.columns = colfuncs.collapse_cols(all_file)
                else:
                    raise HeaderError(
                        "Multiple headers selected, yet dataframe is not "
                        + "multi-indexed, try with 'header=0'"
                    )
                # write to database
                all_file.to_sql(table_name, con=self.engine, index=False,
                                if_exists="append")

    def to_db_agg(self, select="DATA", header=0, by="Image_ImageNumber",
                  method="median", prefix=False, **kwargs):
        """
        Append files to database table after aggregating replicates.

        Parameters
        -----------
        select : string
            the name of the .csv file, this will also be the prefix of the
            database table name.
        header : int or list
            the number of header rows.
        by : string
            the column by which to group the data by.
            NOTE: if collapsing multiindexed columns this will have to be the
                  name of collapsed column. i.e ImageNumber => Image_ImageNumber
        method : string (default="median")
            method by which to average groups, median or mean
        prefix : Boolean
            whether the metadata label required for discerning featuredata
            and metadata needs to be a prefix, or can just be contained within
            the column name
        **kwargs : additional arguments to pandas.read_csv and aggregate

        Returns:
        --------
        Nothing, writes to database or raises an Error
        
        Note:
        ------
        This will attempt to append to a database table if one exists with the
        same name and the same column headers. The an existing database table
        under the same name exists, but has different column headers then an
        sqlalchemy error will be raised.

        The database tables will be named the same as `select`, but appended
        with '_agg', e.g if `select='DATA'', then the table will be named
        `DATA_agg`.
        """
        self.check_database()
        file_name = self.get_file_name(select)
        table_name = "{}_agg".format(self.get_table_name(select))
        # filter files
        file_paths = [f for f in self.file_paths if f.endswith(file_name)]
        # check there are files matching file_name argument
        if len(file_paths) == 0:
            raise ValueError("No files found matching '{}'".format(file_name))
        for indv_file in file_paths:
            if header == 0 or header == [0]:
                tmp_file = pd.read_csv(indv_file, header=0, **kwargs)
                tmp_agg = utils.aggregate(tmp_file, on=by, method=method,
                                          prefix=prefix)
                tmp_agg.to_sql(table_name, con=self.engine, index=False,
                               if_exists="append")
            else:
                tmp_file = pd.read_csv(indv_file, header=header, **kwargs)
                # collapse multi-indexed columns
                # NOTE will aggregate on the collapsed column name
                if isinstance(tmp_file.columns, pd.core.index.MultiIndex):
                    tmp_file.columns = colfuncs.collapse_cols(tmp_file)
                else:
                    # user has passed multiple header rows, but pandas doesn't
                    # think the dataframe has multi-indexed columns so return
                    # an error
                    raise HeaderError(
                        "Multiple headers selected, yet dataframe is not "
                        + "multi-indexed, try with 'header=0'"
                    )
                tmp_agg = utils.aggregate(tmp_file, on=by, method=method,
                                          **kwargs)
                tmp_agg.to_sql(table_name, con=self.engine, index=False,
                               if_exists="append")

    def to_csv_agg(self, save_location, select="DATA", header=0, by="Image_ImageNumber",
                    method="median", prefix=False, **kwargs):
        """
        Bodge to store data in a csv file. Useful if your data contains more
        than 999 columns, which is the maximum number of columns you can
        insert into an sqlite database at once.

        Paramters:
        ----------
        save_location: string
            path to where and what to call the resulting csv file
        select : string
            the name of the .csv file, this will also be the prefix of the
            database table name.
        header : int or list
            the number of header rows.
        by : string
            the column by which to group the data by.
            NOTE: if collapsing multiindexed columns this will have to be the
                  name of collapsed column. i.e ImageNumber => Image_ImageNumber
        method : string (default="median")
            method by which to average groups, median or mean
        prefix : Boolean
            whether the metadata label required for discerning featuredata
            and metadata needs to be a prefix, or can just be contained within
            the column name
        **kwargs : additional arguments to pandas.read_csv and aggregate

        Returns:
        --------
        nothing, saves file to disk at 'save_location'
        """
        file_name = self.get_file_name(select)
        tmp_files = []
        file_paths = [f for f in self.file_paths if f.endswith(file_name)]
        # check there are files matching select argument
        if len(file_paths) == 0:
            raise ValueError("No files found matching '{}'".format(file_name))
        for indv_file in file_paths:
            if header == 0 or header == [0]:
                tmp_file = pd.read_csv(indv_file, header=0, **kwargs)
                tmp_agg = utils.aggregate(tmp_file, on=by, method=method,
                                          prefix=prefix)
            else:
                tmp_file = pd.read_csv(indv_file, header=header, **kwargs)
                # collapse multi-indexed columns
                # NOTE will aggregate on the collapsed column name
                if isinstance(tmp_file.columns, pd.core.index.MultiIndex):
                    tmp_file.columns = colfuncs.collapse_cols(tmp_file)
                else:
                    # user has passed multiple header rows, but pandas doesn't
                    # think the dataframe has multi-indexed columns so return
                    # an error
                    raise HeaderError(
                        "Multiple headers selected, yet dataframe is not "
                        + "multi-indexed, try with 'header=0'"
                    )
                tmp_agg = utils.aggregate(tmp_file, on=by, method=method,
                                          **kwargs)
            tmp_files.append(tmp_agg)
        concat_df = pd.concat(tmp_files, copy=False)
        concat_df.to_csv(save_location, index=False)

    @staticmethod
    def get_table_name(select_name):
        """
        When given `select` in the `to_db*()` methods, this will create
        a name suitable for the database table. So if `select` ends with
        .csv this will be omitted.

        Parameters:
        -----------
        select_name: string

        Returns:
        --------
        string
        """
        if select_name.endswith(".csv"):
            return select_name.replace(".csv", "")
        else:
            return select_name

    @staticmethod
    def get_file_name(select_name):
        """
        When given `select` in the `to_db*()` methods, this will create
        ensure it has a file extension if not already there.

        Parameters:
        -----------
        select_name: string

        Returns:
        --------
        string
        """
        if select_name.endswith(".csv"):
            return select_name
        else:
            return "{}.csv".format(select_name)

    def check_database(self):
        if self.engine is None or self.db_handle is None:
            msg = "no database found, need to call create_db() first"
            raise RuntimeError(msg)


class HeaderError(Exception):
    """Custom error class"""
    pass


if sys.version_info.major < 3:
    # running on python2, create NotADirectoryError class
    class NotADirectoryError(Exception):
        """Doesn't exist in python2, create our own"""
        pass
