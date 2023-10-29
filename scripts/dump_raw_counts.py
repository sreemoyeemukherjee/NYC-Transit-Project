# Author: Sreemoyee Mukherjee (sreemoym)
# Importing duckdb to connect to local database
import duckdb

# Connecting to local database
connection = duckdb.connect('..\main.db')
# Fetching all tables currently existing in our database
tables = connection.execute("SELECT table_name FROM information_schema.tables").fetchall()
table_names = [row[0] for row in tables]  # creating list of table names from list of tuples
print("table_name , row_count")  # this is our header row
# Looping through each table and displaying number of rows in each table
for table_name in table_names:
    row_count_query = f"SELECT COUNT(*) FROM {table_name}"
    row_count = connection.execute(row_count_query).fetchall()
    print(table_name, ",", row_count[0][0])
# Closing the database connection
connection.close()
