"""
tests for meld.colfuncs
"""

import os
import pandas as pd
import meld.colfuncs

CURRENT_PATH = os.path.dirname(__file__)
TEST_PATH = os.path.join(CURRENT_PATH, "test_data", "test_run0/DATA.csv")

def test_collapse_cols():
    """meld.colfuncs.collapse_cols(dataframe, sep)"""
    example_df = pd.read_csv(TEST_PATH, header=[0,1])
    collapsed_colnames = meld.colfuncs.collapse_cols(example_df, sep="_")
    example_df.columns = collapsed_colnames
    expected_colnames = ["Image_ImageNumber",
                         "Image_Intensity_channel_1",
                         "Cell_Area",
                         "Cell_Eccentricity",
                         "Nucleus_Area",
                         "Nucleus_Eccentricity",
                         "Metadata_Well"]
    assert collapsed_colnames == expected_colnames


def test_inflate_cols():
    """meld.colfuncs.inflate_cols(dataframe)"""
    # create example dataframe
    example_df = pd.DataFrame(
        {"Image_ImageNumber": [1, 2, 3],
         "Cell_Area"        : [20, 20, 20],
         "Nuclei_Perimeter" : [15, 15, 15]}
    )
    example_df.columns = meld.colfuncs.inflate_cols(example_df, sep="_")
    # check the columns are MultiIndexed
    assert isinstance(example_df.columns, pd.core.index.MultiIndex)
