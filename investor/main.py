import os
import datetime as dt
import pandas as pd
import yfinance as yf
from flask import Flask, jsonify, request
from google.cloud import bigquery

PROJECT = os.environ.get("GCP_PROJECT")
DATASET = "fin_ai"
TABLE   = "ohlcv_daily"
TICKERS = ["AAPL","GOOGL","NFLX","MSFT","GS","MS","META","AMZN","NVDA","TSLA"]

app = Flask(__name__)
bq = bigquery.Client()

def load_prices():
    end = dt.date.today()
    start = end - dt.timedelta(days=400)  # enough history for ARIMA
    frames = []
    for t in TICKERS:
        df = yf.download(t, start=start, end=end, progress=False)
        if df.empty:
            continue
        df = df.reset_index().rename(columns=str.lower)
        df["ticker"] = t
        df["source"] = "yfinance"
        frames.append(df[["ticker","date","open","high","low","close","volume","source"]])
    if not frames:
        return 0
    out = pd.concat(frames)
    out["date"] = pd.to_datetime(out["date"]).dt.date

    job = bq.load_table_from_dataframe(
        out,
        f"{PROJECT}.{DATASET}.{TABLE}",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()
    return len(out)

@app.route("/ingest", methods=["POST","GET"])
def ingest():
    rows = load_prices()
    return jsonify({"inserted_rows": rows})

@app.route("/")
def health():
    return "ok"
