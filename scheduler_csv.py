import time
from etl_csv import etl_csv_to_sqlite  # Make sure etl_csv.py is in the same folder

while True:
    print("🚀 Running CSV ETL flow...")
    etl_csv_to_sqlite()
    print("⏳ Waiting 1 hour...")
    time.sleep(3600)  # 3600 seconds = 1 hour
