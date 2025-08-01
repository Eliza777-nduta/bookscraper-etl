import requests
import pandas as pd

# Extract

def extract_data_from_api():
    url = 'https://open.er-api.com/v6/latest/USD'
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print("API request failed with status:", response.status_code)
        return None
# Transform
def transform_data(data):
    if data and 'rates' in data:
        df = pd.DataFrame(data['rates'].items(), columns=['Currency', 'Rate'])
        return df
    else:
        return pd.DataFrame()  # empty
#load
def load_data(df):
    df.to_csv("exchange_rates.csv", index=False)
    print("Data saved to exchange_rates.csv")
# Main function to run the ETL process
def etl_api():
    data = extract_data_from_api()
    df = transform_data(data)
    load_data(df)

etl_api()
