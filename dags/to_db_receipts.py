import os
from datetime import date
import psycopg2
import pandas as pd
import psycopg2.extras as extras
import config

def get_file_path(attribute_name):
    today = date.today()
    filename = "loyverse__{0}_{1}.csv".format(attribute_name,today)
    return os.path.join(config.CSV_FILE_DIR_TEST, filename)


def main_receipts(conn, table):
    filename = get_file_path('receipts_only')
    if not os.path.isfile(filename):
        print(f"Error: file {filename} not found")
        return 1
    
    df = pd.read_csv(filename)
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT (receipt_number) DO UPDATE SET %s" % (table, cols, ", ".join([f"{col}=EXCLUDED.{col}" for col in df.columns]))
    with conn.cursor() as cursor:
        try:
            extras.execute_values(cursor, query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            print(tuples)
            conn.rollback()
            return 1
    print("the dataframe is inserted")
    return 0

def main_sales(conn, table):
    filename = get_file_path('all_sales')
    if not os.path.isfile(filename):
        print(f"Error: file {filename} not found")
        return 1
    
    # Read CSV file into a pandas DataFrame
    df = pd.read_csv(filename)

    # Get the value of the last row in the sales_id column of the table
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT sales_id FROM {table} ORDER BY sales_id DESC LIMIT 1")
        last_sales_id = cursor.fetchone()[0]

    # Add a new column to the DataFrame with the sales_id values
    df.insert(0, 'sales_id', range(last_sales_id + 1, last_sales_id + 1 + len(df)))

    # Convert the DataFrame to a list of tuples
    tuples = [tuple(x) for x in df.to_numpy()]

    # Create the SQL query to execute
    cols = ','.join(list(df.columns))
    query = f"INSERT INTO {table} ({cols}) VALUES %s"

    # Execute the SQL query
    with conn.cursor() as cursor:
        try:
            extras.execute_values(cursor, query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            print(tuples)
            conn.rollback()
            return 1
    
    print("the dataframe is inserted")
    return 0

if __name__ == '__main__':
    conn = None
    try:
        conn = psycopg2.connect(
             database="ipc", user="postgres", password="Chisom33", host=config.PSQL_HOST_LH, port="5432"
        )
        if main_receipts(conn, 'loyverse_sales_receipts') == 0:
            print('receipts inserted_successfully')

        if main_sales(conn, 'loyverse_sales') == 0:
            print('sales inserted successfully')

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
    finally:
        if conn is not None:
            conn.close()

