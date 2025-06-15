from openbb import obb
import psycopg2
from datetime import datetime, timedelta
import os

# Fetch yesterday's data
def get_daily_data():
    yesterday = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    data = obb.equity.price.historical("AAPL", start_date=yesterday, end_date=yesterday)
    return data.to_dict("records")[0]

# Insert into PostgreSQL database
def insert_data_to_postgres(data):
    conn = psycopg2.connect(
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT")
    )
    cursor = conn.cursor()

    symbol = "AAPL"
    date = data['date']
    open_price = data['open']
    high = data['high']
    low = data['low']
    close = data['close']
    volume = data['volume']

    insert_query = f"""
    INSERT INTO market_data (symbol, date, open, high, low, close, volume)
    VALUES ('{symbol}', '{date}', {open_price}, {high}, {low}, {close}, {volume})
    ON CONFLICT (symbol, date) DO NOTHING;
    """

    cursor.execute(insert_query)
    conn.commit()
    cursor.close()
    conn.close()
    print("Data inserted successfully")

# Main
if __name__ == "__main__":
    data = get_daily_data()
    insert_data_to_postgres(data)
