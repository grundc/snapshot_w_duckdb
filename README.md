# snapshot_w_duckdb

The reasoning behind this code is to avoid unnecessary Snowflake warehouse costs.
For analysis we sometimes use only a small subset of data from a much bigger dataset.
We apply filters / logic and change and play around with data. All this triggers
warehouse. If the dataset of interest is small and could be handled by a local computer,
why not creating a snapshot of the data and work offline with it?

# setup

- setup a virtual environment based on the "requirements.txt"
- create a "tables.json" file --> checkout "tables_example.json"


# note
This version uses the approach to generate parquet files on Snowflake and
downloads the files. This was the best solution to ensure proper data type
matching. The downside is, that it won't work if a column on Snowflake has the datatype
"timestamp_ltz" or "timestamp_tz", as these are not supported.
