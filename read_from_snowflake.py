from  snowflake.snowpark import Session
import os
import duckdb
import json
import argparse
import shutil

# *********************************************************************************************************************
# Setup argument parser
parser = argparse.ArgumentParser(description='Read data from Snowflake and save to DuckDB.')
parser.add_argument('--sf_connection', type=str, required=True, help='Snowflake connection name in connections.toml')
parser.add_argument('--duckdb', type=str, required=True, help='Name of the duckdb file to save the data')

args = parser.parse_args()

# *********************************************************************************************************************

# Setup Snowflake connection parameters
sf_connection = args.sf_connection # should be in connections.toml


with open('tables.json', 'r') as f:
    tables_list = json.load(f)

# Convert list of lists back to list of tuples
tables = [tuple(item) for item in tables_list["tables"]]


# Establish connection to Snowflake and DuckDB
session = Session.builder.config("connection_name", sf_connection).create()

duck_conn = duckdb.connect(args.duckdb)


try:
    for table, filter in tables:
        # SQL query to retrieve data from the current table
        query = f"SELECT * FROM {table} WHERE {filter}"

        # Create a cursor object
        df = session.sql(query)
        # file_name = f"{table}.parquet"
        remote_file_path = f"{session.get_session_stage()}/{table}"
        copy_result = df.write.parquet(remote_file_path, overwrite=True, header = True)
        print(f"Exported {copy_result[0].rows_unloaded} rows from {table} to Snowflake stage")

        # Get file from STAGE
        session.file.get(remote_file_path, f"./stage/{table}")

       
        # Remove table if it already exists
        duck_conn.execute(f"DROP TABLE IF EXISTS {table}")

  
        duck_conn.execute(f"""
            CREATE TABLE {table} AS 
            SELECT * FROM read_parquet('stage/{table}/*.parquet')
            """)
  
        print(f"Imported {table} into DuckDB!! ")
       


finally:
    # Close the connection
    session.close()
    duck_conn.close()
    shutil.rmtree('./stage')



print("Data import to DuckDB completed.")
