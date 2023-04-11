import os
from datetime import date
import psycopg2
import pandas as pd
import psycopg2.extras as extras
import config

def get_file_path():
    today = date.today()
    attribute_name = 'transactions'
    filename = "lenco__{0}_{1}.csv".format(attribute_name,today)
    return os.path.join(config.CSV_FILE_DIR_LENCO, filename)


def main(conn, table):
    filename = get_file_path()
    if not os.path.isfile(filename):
        print(f"Error: file {filename} not found")
        return 1
    
    df = pd.read_csv(filename)
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
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
            database="ipc", user="postgres", password="Chisom33", host="host.docker.internal", port="5432"
        )
        if main(conn, 'lenco_transactions') == 0:
            print('inserted_successfully')
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
    finally:
        if conn is not None:
            conn.close()