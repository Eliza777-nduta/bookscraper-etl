import requests
from bs4 import BeautifulSoup
import pandas as pd

# 1. Extract
def extract_books():
    url = "http://books.toscrape.com"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    books = soup.select('.product_pod')
    data = []

    for book in books:
        title = book.h3.a['title']
        price_raw = book.find('p', class_='price_color').text
        price_clean = price_raw.encode('ascii', 'ignore').decode().replace('£', '').strip()
        data.append({'title': title, 'price': float(price_clean)})
    return data

# 2. Transform
def transform_books(book_data):
    df = pd.DataFrame(book_data)
    df['price'] = df['price'].round(2)  # Clean: round prices
    return df

# 3. Load
def load_to_csv(df, filename='books.csv'):
    df.to_csv(filename, index=False)
    print(f"✅ Data saved to {filename}")

# Run ETL
def run_etl():
    raw_data = extract_books()
    clean_data = transform_books(raw_data)
    load_to_csv(clean_data)

if __name__ == "__main__":
    run_etl()
