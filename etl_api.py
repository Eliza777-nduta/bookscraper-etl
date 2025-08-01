from prefect import flow, task
import requests
import pandas as pd

@task
def extract_api():
    url = "https://api.exchangerate-api.com/v4/latest/USD"
    response = requests.get(url)
    data = response.json()
    print("✅ Extracted API data")
    return data

@task
def transform_rates(data):
    rate = data["rates"]["KES"]
    print(f"🔧 USD to KES rate: {rate}")
    return pd.DataFrame([{"currency": "KES", "rate": rate}])

@task
def load_to_csv(df, output_path="usd_to_kes.csv"):
    df.to_csv(output_path, index=False)
    print(f"📦 Loaded exchange rate to {output_path}")

@flow
def etl_api_flow():
    data = extract_api()
    df = transform_rates(data)
    load_to_csv(df)

if __name__ == "__main__":
    etl_api_flow()
