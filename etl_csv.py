from prefect import flow, task
import pandas as pd
import sqlite3
@task
@task
def extract_csv(path):
    try:
        df = pd.read_csv(path)
        print("✅ Extracted CSV data")
        print(df.head())
        return df
    except Exception as e:
        print(f"❌ Error extracting data: {e}")
        raise

@task
def transform_sales(df):
    df['Total Net Sales'] = df['Total Net Sales'].astype(float)
    filtered = df[df['Total Net Sales'] > 10000]
    print("🔧 Transformed data (Total Net Sales > 10000):")
    print(filtered)
    return filtered


@task
def load_to_db(df, db_name='sales_data.db'):
    conn = sqlite3.connect(db_name)
    df.to_sql('sales_data', conn, if_exists='replace', index=False)
    conn.close()
    print(f"📦 Loaded data to sales_data table in {db_name}")

@flow
def etl_csv_to_sqlite():
    df = extract_csv("businessretailsales.csv")
    clean_df = transform_sales(df)
    load_to_db(clean_df)


if __name__ == "__main__":
    etl_csv_to_sqlite()
