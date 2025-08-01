from prefect import task, flow

@task
def extract():
    data = [10, 20, 30, 40]
    print("✅ Extracted:", data)
    return data

@task
def transform(data):
    transformed = [x * 10 for x in data]
    print("🔧 Transformed:", transformed)
    return transformed

@task
def load(data):
    print("📦 Loaded data to destination:", data)

@flow
def etl_flow():
    raw_data = extract()
    clean_data = transform(raw_data)
    load(clean_data)

# Run the flow
if __name__ == "__main__":
    etl_flow()
# This code defines a simple ETL (Extract, Transform, Load) flow using Prefect.