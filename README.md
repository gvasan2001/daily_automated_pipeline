# 📈 Daily Financial Data Pipeline

This project automatically fetches stock market data, stores it in a PostgreSQL database (hosted on Railway), and uses a Large Language Model (LLM) to generate daily investment insights.

## 🔧 Tech Stack

- **Python**
- **OpenBB SDK** – for stock data
- **PostgreSQL** – for storage
- **Railway** – for cloud DB hosting
- **Mistral LLM API** – for generating insights

## 🚀 How It Works

1. Fetches daily stock data (AAPL)
2. Stores data in PostgreSQL
3. Sends latest data to an LLM
4. Stores LLM-generated insight and recommendations

## 📂 Tables Required

- `market_data(symbol, date, open, high, low, close, volume)`
- `llm_insights(date, summary, recommendations)`

---

Would you like help writing that `README.md` too, or preparing a GitHub description and tags?
