"""
Daily Financial Data Pipeline

This script automates the process of:
1. Fetching daily stock data using OpenBB.
2. Storing the data in a PostgreSQL database (hosted on Railway).
3. Generating insights using a Large Language Model (LLM) via Mistral API.
4. Saving those insights in the database.

Author: Gokulavasan A
"""

from openbb import obb
import psycopg2
from datetime import datetime, timedelta
from mistralai import Mistral

# === Configuration ===
STOCK_SYMBOL = "AAPL"
DB_CONFIG = {
    "dbname": "railway",
    "user": "postgres",
    "password": "OFdPMrdyDQHgPEeVVTJnFoOimDiccdCu",
    "host": "postgres.railway.internal",
    "port": "5432"
}
MISTRAL_API_KEY = "lWim9ElFQaZB43py6fgG3LJFjRuHY1X6"
MISTRAL_MODEL = "mistral-large-latest"

# === Step 1: Fetch yesterday's stock data ===
def get_daily_data():
    """
    Fetch historical stock data for the given symbol (1 day).
    """
    yesterday = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    data = obb.equity.price.historical(STOCK_SYMBOL, start_date=yesterday, end_date=yesterday)
    return data.to_dict("records")[0]  # Get only one day's record

# === Step 2: Insert data into PostgreSQL ===
def insert_data_to_postgres(data):
    """
    Insert stock data into the market_data table.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO market_data (symbol, date, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, date) DO NOTHING;
    """
    cursor.execute(insert_query, (
        STOCK_SYMBOL,
        data['date'],
        data['open'],
        data['high'],
        data['low'],
        data['close'],
        data['volume']
    ))

    conn.commit()
    cursor.close()
    conn.close()
    print(" Market data inserted")

# === Step 3: Generate LLM Insights and Store ===
def generate_llm_insight():
    """
    Generate stock insight using Mistral LLM and store the result in the database.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Get latest stock data
    cursor.execute("""
        SELECT symbol, date, open, high, low, close, volume 
        FROM market_data ORDER BY date DESC LIMIT 1;
    """)
    row = cursor.fetchone()

    if not row:
        print(" No market data available.")
        return

    symbol, date, open_price, high, low, close, volume = row

    # Construct prompt for LLM
    prompt = f"""
    You are a financial analyst. Analyze the following stock data for {symbol} on {date}:
    - Open: {open_price}
    - High: {high}
    - Low: {low}
    - Close: {close}
    - Volume: {volume}

    Provide a short summary of performance and 3 recommendations.
    """

    # Call Mistral API
    client = Mistral(api_key=MISTRAL_API_KEY)
    response = client.chat.complete(
        model=MISTRAL_MODEL,
        messages=[
            {"role": "system", "content": "You are a helpful assistant"},
            {"role": "user", "content": prompt},
        ]
    )
    
    output = response.choices[0].message.content
    summary = output.split("\n")[0]  # First line as summary
    recommendations = output  # Full response

    # Store insight into database
    cursor.execute("""
        INSERT INTO llm_insights (date, summary, recommendations)
        VALUES (%s, %s, %s)
        ON CONFLICT (date) DO UPDATE 
        SET summary = EXCLUDED.summary,
            recommendations = EXCLUDED.recommendations;
    """, (date, summary, recommendations))

    conn.commit()
    cursor.close()
    conn.close()
    print(" LLM insights stored")

# === Main Program ===
if __name__ == "__main__":
    daily_data = get_daily_data()
    insert_data_to_postgres(daily_data)
    generate_llm_insight()
