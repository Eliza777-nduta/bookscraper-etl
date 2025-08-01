from prefect import flow, task
import pandas as pd

@task
def extract():
    df = pd.read_csv("data.csv")
    print(" Extracted data:")
    print(df)
    return df

@task
def transform(df):
    # Let's convert all names to uppercase as our "transformation"
    df['name'] = df['name'].str.upper()
    print(" Transformed data:")
    print(df)
    return df

@task
def load(df):
    df.to_csv("transformed_data.csv", index=False)
    print(" Data saved to 'transformed_data.csv'")

@flow(name="Simple ETL Flow")
def etl():
    data = extract()
    transformed = transform(data)
    load(transformed)

if __name__ == "__main__":
    etl()
