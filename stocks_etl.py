import yfinance as yf
import pandas as pd
from pandas_gbq import to_gbq
from google.oauth2 import service_account
import os
from dotenv import load_dotenv

# ------------------------------
# CONFIG
# ------------------------------
# Load .env variables
load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_CREDENTIALS_PATH = os.getenv("GCP_CREDENTIALS_PATH")
BQ_DATASET = os.getenv("BQ_DATASET")

# Stocks to pull
TICKERS = ["AAPL", "MSFT", "GOOGL", "UBER", "NVDA", "AMZN"]

# BigQuery credentials
credentials = service_account.Credentials.from_service_account_file(
    GCP_CREDENTIALS_PATH
)

# ------------------------------
# FUNCTIONS
# ------------------------------


def pull_price_data(ticker):
    print(f"Pulling data for {ticker}")
    stock = yf.Ticker(ticker)

    # -------------------
    # Historical prices (raw)
    # -------------------
    df_prices = stock.history(period="max", auto_adjust=False, actions=True)
    df_prices = df_prices.reset_index()
    df_prices["symbol"] = ticker

    # Rename known columns, keep others as-is
    rename_map = {
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
        "Dividends": "dividends",
        "Stock Splits": "splits",
    }
    df_prices = df_prices.rename(
        columns={k: v for k, v in rename_map.items() if k in df_prices.columns}
    )

    # Ensure adj_close exists
    if "adj_close" not in df_prices.columns:
        df_prices["adj_close"] = df_prices.get("close")

    # -------------------
    # Dividends (separate table)
    # -------------------
    df_div = stock.dividends.reset_index()
    if not df_div.empty:
        df_div["symbol"] = ticker
        df_div = df_div.rename(columns={"Date": "date", "Dividends": "dividend_amount"})

    # -------------------
    # Splits (separate table)
    # -------------------
    df_splits = stock.splits.reset_index()
    if not df_splits.empty:
        df_splits["symbol"] = ticker
        df_splits = df_splits.rename(
            columns={"Date": "date", "Stock Splits": "split_ratio"}
        )

    # -------------------
    # Company metadata (raw)
    # -------------------
    info = stock.info
    if info:
        df_company = pd.DataFrame([{**info, "symbol": ticker}])
    else:
        df_company = pd.DataFrame([{"symbol": ticker}])  # fallback if info is empty

    return df_prices, df_div, df_splits, df_company


# ------------------------------
# MAIN LOOP
# ------------------------------
all_prices = []
all_dividends = []
all_splits = []
all_companies = []

for ticker in TICKERS:
    prices, dividends, splits, company = pull_price_data(ticker)
    all_prices.append(prices)
    if not dividends.empty:
        all_dividends.append(dividends)
    if not splits.empty:
        all_splits.append(splits)
    all_companies.append(company)

# Concatenate all tickers
df_prices_all = pd.concat(all_prices, ignore_index=True)
df_dividends_all = (
    pd.concat(all_dividends, ignore_index=True) if all_dividends else pd.DataFrame()
)
df_splits_all = (
    pd.concat(all_splits, ignore_index=True) if all_splits else pd.DataFrame()
)
df_companies_all = pd.concat(all_companies, ignore_index=True)

# ------------------------------
# WRITE TO BIGQUERY
# ------------------------------
to_gbq(
    df_prices_all,
    f"{BQ_DATASET}.raw_stock_prices",
    project_id=GCP_PROJECT_ID,
    if_exists="replace",
    credentials=credentials,
)
if not df_dividends_all.empty:
    to_gbq(
        df_dividends_all,
        f"{BQ_DATASET}.raw_dividends",
        project_id=GCP_PROJECT_ID,
        if_exists="replace",
        credentials=credentials,
    )
if not df_splits_all.empty:
    to_gbq(
        df_splits_all,
        f"{BQ_DATASET}.raw_splits",
        project_id=GCP_PROJECT_ID,
        if_exists="replace",
        credentials=credentials,
    )
to_gbq(
    df_companies_all,
    f"{BQ_DATASET}.raw_company",
    project_id=GCP_PROJECT_ID,
    if_exists="replace",
    credentials=credentials,
)

print("Data successfully uploaded to BigQuery!")
