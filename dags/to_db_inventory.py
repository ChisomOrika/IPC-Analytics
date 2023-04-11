import sqlalchemy
from ingestion_inventory import main_inventory
import config

inventory_df = main_inventory()


def main_customers(engine,table,df):
    try:
        temp_table_name = 'temp_table'
        df.to_sql(temp_table_name, engine, if_exists='replace', index=False)

        with engine.begin() as conn:
            # Update the corresponding columns in the database
            conn.execute(f'''
                INSERT INTO {table} 
                SELECT * FROM {temp_table_name}
                ON CONFLICT (id) DO UPDATE
                SET cost = EXCLUDED.cost,
                    purchase_cost = EXCLUDED.purchase_cost,
                    low_stock = EXCLUDED.low_stock,
                    optimal_stock = EXCLUDED.optimal_stock,
                    option1_value = EXCLUDED.option1_value,
                    option2_value = EXCLUDED.option2_value,
                    option3_value = EXCLUDED.option3_value,
                    option2_name = EXCLUDED.option2_name,
                    option3_name = EXCLUDED.option3_name,
                    category_id = EXCLUDED.category_id,
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
        engine = sqlalchemy.create_engine(config.sqlalchemy_connection_string_lh)
        if main_customers(engine, 'loyverse_inventory',inventory_df) == 0:
            print('inventory items inserted_successfully')

    except (Exception, sqlalchemy.exc.DatabaseError) as error:
        print("Error: %s" % error)
    finally:
        if engine is not None:
            engine.dispose()