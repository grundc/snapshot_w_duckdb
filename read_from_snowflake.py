import snowflake.connector
import csv
import os
import duckdb

# List of tables to extract.  Table + filter condition
tables = [('raw_hosts','1=1'), ('raw_listings','1=1'), ('raw_reviews','1=1')]  # Add all your table names here

# Remove existing CSV files
for table in tables:
    csv_file = f'{table}.csv'
    if os.path.exists(csv_file):
        os.remove(csv_file)
        print(f"Removed existing file: {csv_file}")

# Establish connection to Snowflake
conn = snowflake.connector.connect(
   connection_name="my_snowflake_connection",
   warehouse="compute_wh",
   database="airbnb",
   schema="raw"
)

# Set batch size (number of rows to fetch per iteration)
batch_size = 10000

try:
    for table, filter in tables:
        # SQL query to retrieve data from the current table
        query = f"SELECT * FROM {table} WHERE {filter}"

        # Create a cursor object
        cur = conn.cursor()

        # Execute the query
        cur.execute(query)

        # Fetch the column names
        column_names = [desc[0] for desc in cur.description]

        # Open a CSV file to write the data
        with open(f'{table}.csv', mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)

            # Write the column headers to the CSV file
            writer.writerow(column_names)

            # Fetch data in batches and write to the CSV file
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break  # Exit loop when no more rows are fetched
                writer.writerows(rows)

        print(f"Data saved to {table}.csv in chunks of {batch_size} rows")

        # Close the cursor
        cur.close()

finally:
    # Close the connection
    conn.close()

# Connect to DuckDB
duck_conn = duckdb.connect('airbnb.db')

# Import CSV files into DuckDB
for table in tables:
    csv_file = f'{table}.csv'
    duck_conn.execute(f"DROP TABLE IF EXISTS {table}")
    duck_conn.execute(f"""
        CREATE TABLE {table} AS 
        SELECT * FROM read_csv_auto('{csv_file}')
    """)
    print(f"Imported {csv_file} into DuckDB table {table}")

# Close DuckDB connection
duck_conn.close()

print("Data import to DuckDB completed.")
