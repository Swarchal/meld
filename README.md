# meld

**Collate results from distributed cellprofiler jobs**

--------------------------------------------------------------------------------

Following a cellprofiler analysis with ExportToSpreadsheet, we produce many .csv files that we want to merge together.

## Creating a database of results

If there is a lot of data, then the sensible option is to create a database of the results. We can use meld to scan through multiple sub-directories and build an sqlite database from the .csv files, with the option of aggregating the data as we go.

If we have a directory of results containing multiple sub-folders, e.g:

```
results/
├── output_1
│   ├── DATA.csv
│   └── IMAGE.csv
├── output_2
│   ├── DATA.csv
│   └── IMAGE.csv
└── output_3
    ├── DATA.csv
    └── IMAGE.csv
```

```python
import meld
merger = meld.Merger("/results")
```

We then want to tell `merge_to_db` where to store the database.

```python
merger.create_db("/path/to/db/location")
```

Now we want the database to create separate tables for  `DATA` and `IMAGE`. We
can specify the name of the .csv file we want to store in each table.

```python
merger.to_db("DATA")
merger.to_db("IMAGE")
```

This will automatically scan through the sub-directories, and read in the DATA
and IMAGE files respectively, appending each to the appropriate table.


### Multi-indexed columns

CellProfiler can combine the results of different objects into a single csv
file, when this is done it produces a .csv file with multi-indexed columns.

`merge_to_db` can automatically flatten these column headers before storing in
the database if we specify the number of headers beforehand. So if `DATA` has
two indices:

```python
merger.to_db("DATA", header=[0,1])
```

### Aggregating cell-level data

As the default output from CellProfiler is cell-level data, whereby we have a
row per object, it's normally convenient to aggregate this to image or well
averages. We can do this automatically when appending the raw-data to the
database with the method `to_db_agg()`, which produces a separate table in the
database named `<object>_agg`, where object is the name of the .csv file
containing the raw data.

usage:

```python
merger.to_db_agg(select="DATA", header=[0,1], by="ImageNumber")
```

This will group the data by ImageNumber and create a row with a median value
for each image.

We can change the aggregation function by passing the `method` argument.

```python
merger.to_db_agg(select="DATA", header=[0,1], by="ImageNumber", method="mean")
```

This will create a table called `DATA_agg` in the database, with a row per
image.


## Potential problems

If you're collapsing multi-indexed columns and aggregating data, you have to specify the column you wish to aggregate by with the collapsed name.  
For example:

|    Image        |        Image            | ... |
|:---------------:|:-----------------------:|-----|
| **ImageNumber** | **Intensity_channel_1** | ... |
| 1               | 0.758                   | ... |
| ...             | ...                     | ... |

You would aggregate by `Image_ImageNumber`, as the `Image` and `ImageNumber` column headers will be collapsed before aggregation.

```python
merger.to_db_agg(select="DATA", header=[0,1], by="Image_ImageNumber")
```
