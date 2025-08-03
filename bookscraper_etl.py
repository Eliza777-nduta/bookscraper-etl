import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from prefect import flow, task

@task
def extract_books():
    base_url = "http://books.toscrape.com/catalogue/page-{}.html"
    all_books = []

    for page_num in range(1, 51):
        url = base_url.format(page_num)
        response = requests.get(url)

        if response.status_code != 200:
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        books = soup.select('.product_pod')

        if not books:
            break

        for book in books:
            title = book.h3.a['title']
            price_raw = book.find('p', class_='price_color').text
            price_clean = price_raw.encode('ascii', 'ignore').decode().replace('£', '').strip()
            all_books.append({'title': title, 'price': float(price_clean)})

    return all_books


@task
def transform_books(book_data):
    df = pd.DataFrame(book_data)
    df['price'] = df['price'].round(2)
    return df


@task
def load_to_sqlite(data, db_name='books.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            price REAL
        )
    ''')

    for _, row in data.iterrows():
        title = row['title']
        price = row['price']

        if not title or not isinstance(price, (int, float)):
            continue

        cursor.execute(
            'INSERT INTO books (title, price) VALUES (?, ?)',
            (title, price)
        )

    conn.commit()
    conn.close()




@flow
def etl_pipeline():
    raw = extract_books()
    clean = transform_books(raw)
    load_to_sqlite(clean)


if __name__ == "__main__":
    etl_pipeline()
    print("ETL pipeline completed successfully.")
