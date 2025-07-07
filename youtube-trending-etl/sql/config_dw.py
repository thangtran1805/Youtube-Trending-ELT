import duckdb
import os

# DuckDB file path:
database_path  = '/home/thangtranquoc/youtube-project/youtube-trending-etl/datawarehouse.duckdb'

# Remove database file if already exists
if os.path.exists(database_path):
    os.remove(database_path)

# Connect to DuckDB, open or create database
conn = duckdb.connect(database=database_path)

# Read the query of the sql file
with open('/home/thangtranquoc/youtube-project/youtube-trending-etl/sql/datawarehouse.sql','r') as file:
    query = file.read()

# Execute the query
conn.execute(query)

# Close the connection

print(f'Database has been created and saved to {database_path}')