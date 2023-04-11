import requests 
import json
import pandas as pd
from datetime import date
import os
import config


# Define API endpoint and parameters
url = config.lenco_url
path = 'transactions'
headers = config.lenco_headers
columns_to_drop = ['bank.code']

def make_request(key_1, key_2):
    # Send a GET request to the URL and extract the JSON response
    response = requests.get(url + path, headers=headers)
    json_response = json.loads(response.text).get(key_1) 
    # Extract the value of key_2 from the JSON response if present, otherwise return the value of key_1
    if key_2 in json_response:
        json_response = json_response.get(key_2)
    else:
        pass
    # Return the final JSON response
    return json_response


def json_to_df(json_response,column_name):
    fill_value = '2020-01-01T00:00:00.000Z'
    df = pd.DataFrame.from_dict(json_response)
    new_df = pd.json_normalize(df[column_name])
    df = pd.concat([df.drop(column_name, axis=1), new_df], axis=1)
    df = df.drop(columns_to_drop,axis=1)
    df = df.rename(columns=lambda x: x.lower())
    df.rename(columns = {'bank.name':'bankname','type':'transactiontype'}, inplace = True)
    df.fillna(value={'failedat': fill_value, 'initiatedat': fill_value,'completedat': fill_value}, inplace=True)
    return df

def get_today_data(df):
    if df is None:
        raise ValueError("Input DataFrame is None.")
    # convert 'timestamp' column to datetime type
    df['completedat'] = pd.to_datetime(df['completedat'])
    # get today's date
    today = date.today()
    # filter rows where 'timestamp' column corresponds to today's date
    today_df = df[df['completedat'].dt.date == today]
    # return the filtered DataFrame
    return today_df

def get_file_path(attribute_name):
    today = date.today()
    filename = "lenco__{0}_{1}.csv".format(attribute_name,today)
    return os.path.join(config.CSV_FILE_DIR_LENCO, filename)

def save_new_data_to_csv(today_df,attribute_name):
    filename = get_file_path(attribute_name)
    if not today_df.empty:
        today_df.to_csv(filename, encoding='utf-8', index=False)

def main(): 
    data_json = make_request('data','transactions')
    df = json_to_df(data_json,'details')
    today_data = get_today_data(df)
    save_new_data_to_csv(today_data,'transactions')
    if len(today_data) == 0:
        print('No data for today yet')
    else:
        print("Data exists, saving to csv")

if __name__ == '__main__':
    main()
    print("process is done")
else:
    print('not executed')