# daily_pipeline.py

from openbb import obb
import psycopg2
from datetime import datetime, timedelta
from openai import OpenAI
import openai
import os
import requests

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

    # Set your OpenAI API key
#     client = OpenAI(
#   base_url="https://openrouter.ai/api/v1/",
#     api_key="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",    
# )
#     openai.api_base = "https://openrouter.ai/api/v1"
#     openai.api_key = "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"

#     response = openai.ChatCompletion.create(
#   model="openai/gpt-3.5-turbo",
#     messages=[
#         {"role": "system", "content": "You are a financial analyst."},
#         {"role": "user", "content": prompt}
#     ]
# )

    headers = {
        "Authorization": f"Bearer {os.getenv('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')}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": "mistralai/mistral-7b-instruct",  # Free model
        "messages": [
        {"role": "system", "content": "You are a financial analyst."},
        {"role": "user", "content": prompt}
    ],
        "max_tokens": 500
    }
    
    response = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers=headers,
        json=payload
    ).json()

    output = response.choices[0].message.content
    content = response['choices'][0]['message']['content']
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
