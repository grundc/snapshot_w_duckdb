# snapshot_w_duckdb

The reasoning behind this code is to avoid unnecessary Snwoflake warehouse costs.
For analysis we sometimes use only a small subset of data from a much bigger dataset.
We apply filters / logic and change and paly around with data. All this triggers
warehouse. If the dataset of interest is small and could be handled by a local computer,
why not creating a snapshot of the data and work offline with it?

# setup

- setup a virtual environment based on the "requirements.txt"
- create a "tables.json" file --> checkout "tables_template.json"
