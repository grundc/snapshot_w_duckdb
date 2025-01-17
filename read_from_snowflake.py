import snowflake.connector
import os
import duckdb
import json

# Setup Snowflake connection parameters
sf_connection = "CTID_DEV_DP_INSTRUMENT_GETLOGS" # should be in connections.toml


with open('tables.json', 'r') as f:
    tables_list = json.load(f)

# Convert list of lists back to list of tuples
tables = [tuple(item) for item in tables_list["tables"]]



# Establish connection to Snowflake and DuckDB
conn = snowflake.connector.connect(
   connection_name=sf_connection
)
duck_conn = duckdb.connect('duckdb.db')


try:
    for table, filter in tables:
        # SQL query to retrieve data from the current table
        query = f"SELECT * FROM {table} WHERE {filter}"

        # Create a cursor object
        cur = conn.cursor()

        # Execute the query
        cur.execute(query)

       
        # Remove table if it already exists
        duck_conn.execute(f"DROP TABLE IF EXISTS {table}")

        file_counter = 0
        for batch_df in cur.fetch_pandas_batches():
            file_counter += 1
            file_name = f'{table}.{file_counter}.parquet'
            batch_df.to_parquet(file_name, index=False)
            if file_counter == 1:
                duck_conn.execute(f"""
                    CREATE TABLE {table} AS 
                    SELECT * FROM read_parquet('{file_name}')
                    """)
            else:
                duck_conn.execute(f"""
                    INSERT INTO {table}
                    SELECT * FROM read_parquet('{file_name}')
                    """)
            print(f"Imported {file_name} into DuckDB table {table}")
            os.remove(file_name)

       

        # Close the cursor
        cur.close()


finally:
    # Close the connection
    conn.close()
    duck_conn.close()



print("Data import to DuckDB completed.")
