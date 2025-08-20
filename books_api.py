from fastapi import FastAPI
import sqlite3
import pandas as pd

app = FastAPI(title="Books Scraper API")

DB_PATH = "books.db"  # your books ETL pipeline saves data here

@app.get("/")
def home():
    return {"message": "Welcome to the Books API"}

@app.get("/books")
def get_books():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM books", conn)
    conn.close()
    return df.to_dict(orient="records")

@app.get("/books/{book_id}")
def get_book(book_id: int):
    conn = sqlite3.connect(DB_PATH)
    query = f"SELECT * FROM books WHERE id={book_id}"
    df = pd.read_sql(query, conn)
    conn.close()
    if df.empty:
        return {"error": "Book not found"}
    return df.to_dict(orient="records")[0]
