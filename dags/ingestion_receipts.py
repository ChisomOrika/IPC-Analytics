import requests 
import json
from datetime import date
import pandas as pd
import config
import os


# Define API endpoint and parameters
url_name = config.loyverse_url
path = 'receipts'
parameters = config.loyverse_parameter
nested_column = 'line_items'
columns_to_drop = ['note','order','source','receipt_date','points_deducted','points_earned','points_balance','tip','surcharge','line_discounts','line_modifiers','line_taxes','total_discount','item_name','variant_name','payments','pos_device_id','dining_option','total_discounts','total_taxes','total_tax','id']


def make_request():
    try:
        response = requests.get(url_name+path, headers=parameters)    
        response.raise_for_status()  # raise an exception if the response is not successful
        json_response = json.loads(response.text).get(path)
        df = pd.DataFrame.from_dict(json_response)
        return df
    except Exception as e:
        print(f"An error occurred while making a request to {url_name+path}: {e}")
        return None

def unnest_nested_column(df, column_name):
    unchanged_column_names = list(df.columns.difference([column_name], sort=False))
    df = df.explode(column_name)
    df_unnest_items = df.apply(lambda x: pd.concat([x[unchanged_column_names], pd.Series(x[column_name])]), axis=1).reset_index(drop = True)
    return df_unnest_items



def get_today_data(df):
    # convert 'created_at' column to datetime type
    df['created_at'] = pd.to_datetime(df['created_at'])
    # set the start date to 24th March 2023
    start_date = pd.to_datetime('2023-04-03')
    # get today's date
    today = date.today()
    # filter rows where 'created_at' column corresponds to dates between start_date and today
    today_df = df[(df['created_at'].dt.date >= start_date.date()) & (df['created_at'].dt.date <= today)]
    # return the filtered DataFrame
    return today_df

def transform_data_sales_receipts(df):
    fill_value = '2020-01-01T00:00:00.000Z'
    df_sales_receipt = df.drop(columns_to_drop,axis=1)
    df_sales_receipt = df_sales_receipt.iloc[:,:9].drop_duplicates().reset_index(drop = True)
    df_sales_receipt.fillna(value={'cancelled_at': fill_value}, inplace=True)
    return df_sales_receipt

def transform_data_sales(df):
    df = df.drop(columns_to_drop, axis=1).reset_index(drop=True)
    df = df.drop(['total_money'], axis=1).reset_index(drop=True)
    # Reorder the columns as desired
    df = df.iloc[:, [0, 3, 9,10, 11, 12, 13, 14, 15, 16, 17]]
    # Add a new column for the sales ID
    return df

def get_file_path(attribute_name):
    today = date.today()
    filename = "loyverse__{0}_{1}.csv".format(attribute_name,today)
    return os.path.join(config.CSV_FILE_DIR_TEST, filename)

def save_new_data_to_csv(transform_data,attribute_name):
    filename = get_file_path(attribute_name)
    if not transform_data.empty:
        transform_data.to_csv(filename, encoding='utf-8', index=False)

def main(): 
    data_json = make_request()
    df = unnest_nested_column(data_json,'line_items')
    today_receipt_data = get_today_data(df)
    receipts_data = transform_data_sales_receipts(today_receipt_data)
    save_new_data_to_csv(receipts_data,'receipts_only')
    today_sales_data = get_today_data(df)
    sales_data = transform_data_sales(today_sales_data)
    save_new_data_to_csv(sales_data,'all_sales')
    if len(receipts_data) == 0:
        print('No data for today yet')
    else:
        print("Data exists, saving to csv")
    
if __name__ == '__main__':
    main()
    print("process is done")
else:
    print('not executed')