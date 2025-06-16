# daily_pipeline.py

from openbb import obb
import psycopg2
from datetime import datetime, timedelta
from openai import OpenAI
import openai
import os
import requests
from mistralai import Mistral

# === Step 1: Fetch yesterday's stock data ===
def get_daily_data():
    yesterday = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    data = obb.equity.price.historical("AAPL", start_date=yesterday, end_date=yesterday)
    return data.to_dict("records")[0]  # Returns data for one day

# === Step 2: Insert data into PostgreSQL ===
def insert_data_to_postgres(data):
    conn = psycopg2.connect(
        dbname="railway",
        user="postgres",
        password="OFdPMrdyDQHgPEeVVTJnFoOimDiccdCu",
        host="postgres.railway.internal",
        port="5432"
    )
    cursor = conn.cursor()

    symbol = "AAPL"
    date = data['date']
    open_price = data['open']
    high = data['high']
    low = data['low']
    close = data['close']
    volume = data['volume']

    insert_query = """
    INSERT INTO market_data (symbol, date, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, date) DO NOTHING;
    """

    cursor.execute(insert_query, (symbol, date, open_price, high, low, close, volume))
    conn.commit()
    cursor.close()
    conn.close()
    print("Market data inserted")

# === Step 3: Generate LLM Insights and Store ===
def generate_llm_insight():
    conn = psycopg2.connect(
        dbname="railway",
        user="postgres",
        password="OFdPMrdyDQHgPEeVVTJnFoOimDiccdCu",
        host="postgres.railway.internal",
        port="5432"
    )
    cursor = conn.cursor()

    cursor.execute("""
        SELECT symbol, date, open, high, low, close, volume 
        FROM market_data ORDER BY date DESC LIMIT 1;
    """)
    row = cursor.fetchone()

    if not row:
        print("No market data available.")
        return

    symbol, date, open_price, high, low, close, volume = row

    # Create prompt
    prompt = f"""
    You are a financial analyst. Analyze the following stock data for {symbol} on {date}:
    - Open: {open_price}
    - High: {high}
    - Low: {low}
    - Close: {close}
    - Volume: {volume}

    Provide a short summary of performance and 3 recommendations.
    """

 

    api_key = "lWim9ElFQaZB43py6fgG3LJFjRuHY1X6"
    model = "mistral-large-latest"

    # Set Gemini API Key
    client = Mistral(api_key=api_key)

    response = client.chat.complete(
    model=model,
    messages=[
        {"role": "system", "content": "You are a helpful assistant"},
        {"role": "user", "content": prompt},
    ]
)
    print(response)
    output = response.choices[0].message.content
    content = response.choices[0].message.content
    summary = content.split("\n")[0]  # First line
    recommendations = content  # Full text

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
    print("LLM insights stored")

# === Main Program ===
if __name__ == "__main__":
    daily_data = get_daily_data()
    insert_data_to_postgres(daily_data)
    generate_llm_insight()
