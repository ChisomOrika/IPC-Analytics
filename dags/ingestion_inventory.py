import pandas as pd
import requests
import json
import config


# Define API endpoint and parameters
url_name = config.loyverse_url
path = 'items'
page_limit = '?limit=250'
parameters = config.loyverse_parameter
columns_to_drop = ['handle','reference_id','is_composite','use_production','components','primary_supplier_id','tax_ids','deleted_at','modifier_ids','image_url','barcode','reference_variant_id','item_id','price','pricing_type']
col_positions = [11, 12] 


def make_request(key_1):
    # Send a GET request to the URL and extract the JSON response
    response = requests.get(url_name + path + page_limit, headers=parameters)
    json_response = json.loads(response.text).get(key_1) 
    return json_response


def json_to_df(json_response):
    df = pd.DataFrame.from_dict(json_response)
    return df

def explode_df(df, column_name):
    unchanged_column_names = list(df.columns.difference([column_name], sort=False))
    df = df.explode(column_name)
    df_unnest_items = df.apply(lambda x: pd.concat([x[unchanged_column_names], pd.Series(x[column_name])]), axis=1).reset_index(drop = True)
    df_unnest_items = df_unnest_items.loc[:,~df_unnest_items.columns.duplicated()]
    return df_unnest_items

def transform_data(df):
    df = df.drop(columns=columns_to_drop, errors='ignore')
    df.fillna({'purchase_cost': 0.0, 'default_price': 0.0,'low_stock': 0,'optimal_stock': 0}, inplace=True)
    df = pd.concat([df.iloc[:, :col_positions[0]], df.iloc[:, col_positions[-1]+1:], df.iloc[:, col_positions]], axis=1)
    df[['low_stock','sku']] = df[['low_stock','sku']].astype(int)
    df[['created_at','updated_at']] = df[['created_at','updated_at']].astype('datetime64[ns]')
    sku_col = df.pop('sku')
    df.insert(0, 'sku', sku_col)
    df.sort_values('sku', ascending=True, inplace=True)
    df.rename(columns={'id': 'item_id','sku': 'id'}, inplace=True)
    return df.reset_index(drop=True)

def main_inventory():
    data_json = make_request('items')
    df = json_to_df(data_json)
    df_variants = explode_df(df,'variants')
    df_stores = explode_df(df_variants,'stores')
    df_cleaned = transform_data(df_stores)
    return df_cleaned