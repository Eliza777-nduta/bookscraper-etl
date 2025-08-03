import streamlit as st
import pandas as pd
import sqlite3

# Connect to your SQLite DB
conn = sqlite3.connect("books.db")
df = pd.read_sql_query("SELECT * FROM books", conn)

st.title("📚 Book Scraper Dashboard")

# Show the full dataset
st.subheader("All Books")
st.dataframe(df)

# Filters
st.subheader("Filter by Rating")
rating_filter = st.slider("Minimum Rating", min_value=0.0, max_value=5.0, step=0.5, value=3.0)
filtered_df = df[df['rating'] >= rating_filter]
st.write(f"Books with rating >= {rating_filter}")
st.dataframe(filtered_df)

# Bar chart
st.subheader("📊 Book Prices Distribution")
st.bar_chart(df['price'])

conn.close()
