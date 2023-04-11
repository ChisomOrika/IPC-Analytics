import sqlalchemy
from ingestion_customers import main_customers
import config

customers_df = main_customers()

def main_customers(engine,table,df):
    try:
        temp_table_name = 'temp_table'
        df.to_sql(temp_table_name, engine, if_exists='replace', index=False)

        with engine.begin() as conn:
            # Update the corresponding columns in the database
            conn.execute(f'''
                INSERT INTO {table} (id,name,first_visit,last_visit,total_visits,total_spent,total_points,created_at,updated_at)
                SELECT id,name,CAST(first_visit AS timestamp without time zone),CAST(last_visit AS timestamp without time zone),total_visits,total_spent,total_points,CAST(created_at AS timestamp without time zone),CAST(updated_at AS timestamp without time zone) FROM {temp_table_name}
                ON CONFLICT (id) DO UPDATE
                SET last_visit = EXCLUDED.last_visit,
                    total_visits = EXCLUDED.total_visits,
                    total_spent = EXCLUDED.total_spent,
                    updated_at = EXCLUDED.updated_at
            ''')
    
    except(Exception, sqlalchemy.exc.DatabaseError) as error:
        print("Error: %s" % error)
        engine.dispose()
        return 1
    
    print("the dataframe is inserted")
    return 0

if __name__ == '__main__':
    engine = None
    try:
        engine = sqlalchemy.create_engine(config.sqlalchemy_connection_string_docker)
        if main_customers(engine, 'loyverse_customers',customers_df) == 0:
            print('receipts inserted_successfully')

    except (Exception, sqlalchemy.exc.DatabaseError) as error:
        print("Error: %s" % error)
    finally:
        if engine is not None:
            engine.dispose()