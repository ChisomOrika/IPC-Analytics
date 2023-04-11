import requests 
import json
import pandas as pd
import config


# Define API endpoint and parameters
url_name = config.loyverse_url
parameters = config.loyverse_parameter


def make_request(key_1):
    try:
        # Send a GET request to the URL and extract the JSON response
        response = requests.get(url_name + key_1, headers=parameters)
        json_response = json.loads(response.text).get(key_1) 
        # Return the final JSON response
        return json_response
        
    except Exception as e:
        print(f"An error occurred while making a request to {url_name+key_1}: {e}")
        return None

    
def json_to_df(json_response):
    df = pd.DataFrame.from_dict(json_response)
    df_customers = df.drop(['email', 'customer_code','country_code','postal_code','region','city','address','phone_number','note','permanent_deletion_at','deleted_at'], axis=1)
    return df_customers


def main_customers():
    json_customers = make_request('customers')
    df_customers = json_to_df(json_customers)
    return df_customers