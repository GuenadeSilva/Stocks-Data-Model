import os
import yfinance as yf
import pandas as pd
from pandas_gbq import to_gbq
from google.oauth2 import service_account
from dotenv import load_dotenv

# ------------------------------
# LOAD ENV VARIABLES
# ------------------------------
load_dotenv()  # loads variables from .env file

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")
GCP_CREDENTIALS_PATH = os.getenv("GCP_CREDENTIALS_PATH")

TICKERS = ["AAPL", "MSFT", "GOOGL", "UBER", "NVDA", "AMZN"]  # extend as needed

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
    # Historical Prices
    # -------------------
    df_prices = stock.history(period="max").reset_index()
    df_prices["symbol"] = ticker
    df_prices = df_prices.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
            "Dividends": "dividends",
            "Stock Splits": "splits",
            "Adj Close": "adj_close",
        }
    )
    if "adj_close" not in df_prices.columns:
        df_prices["adj_close"] = df_prices["close"]

    # -------------------
    # Dividends
    # -------------------
    df_div = stock.dividends.reset_index()
    df_div["symbol"] = ticker
    df_div = df_div.rename(columns={"Date": "date", "Dividends": "dividend_amount"})

    # -------------------
    # Splits
    # -------------------
    df_splits = stock.splits.reset_index()
    df_splits["symbol"] = ticker
    df_splits = df_splits.rename(
        columns={"Date": "date", "Stock Splits": "split_ratio"}
    )

    # -------------------
    # Company metadata
    # -------------------
    info = stock.info
    df_company = pd.DataFrame(
        [
            {
                "symbol": ticker,
                "company_name": info.get("longName"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "country": info.get("country"),
                "market_cap": info.get("marketCap"),
                "currency": info.get("currency"),
                "exchange": info.get("exchange"),
            }
        ]
    )

    # -------------------
    # Annual Financials
    # -------------------
    df_fin_annual = stock.financials.T.reset_index().rename(columns={"index": "date"})
    df_fin_annual["symbol"] = ticker

    # -------------------
    # Quarterly Financials
    # -------------------
    df_fin_quarterly = stock.quarterly_financials.T.reset_index().rename(
        columns={"index": "date"}
    )
    df_fin_quarterly["symbol"] = ticker

    return df_prices, df_div, df_splits, df_company, df_fin_annual, df_fin_quarterly


# ------------------------------
# MAIN LOOP
# ------------------------------
all_prices, all_dividends, all_splits, all_companies = [], [], [], []
all_fin_annual, all_fin_quarterly = [], []

for ticker in TICKERS:
    prices, dividends, splits, company, fin_annual, fin_quarterly = pull_price_data(
        ticker
    )
    all_prices.append(prices)
    all_dividends.append(dividends)
    all_splits.append(splits)
    all_companies.append(company)
    all_fin_annual.append(fin_annual)
    all_fin_quarterly.append(fin_quarterly)

# Concatenate
df_prices_all = pd.concat(all_prices, ignore_index=True)
df_dividends_all = pd.concat(all_dividends, ignore_index=True)
df_splits_all = pd.concat(all_splits, ignore_index=True)
df_companies_all = pd.concat(all_companies, ignore_index=True)
df_fin_annual_all = pd.concat(all_fin_annual, ignore_index=True)
df_fin_quarterly_all = pd.concat(all_fin_quarterly, ignore_index=True)

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
to_gbq(
    df_dividends_all,
    f"{BQ_DATASET}.raw_dividends",
    project_id=GCP_PROJECT_ID,
    if_exists="replace",
    credentials=credentials,
)
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
to_gbq(
    df_fin_annual_all,
    f"{BQ_DATASET}.raw_financials_annual",
    project_id=GCP_PROJECT_ID,
    if_exists="replace",
    credentials=credentials,
)
to_gbq(
    df_fin_quarterly_all,
    f"{BQ_DATASET}.raw_financials_quarterly",
    project_id=GCP_PROJECT_ID,
    if_exists="replace",
    credentials=credentials,
)

print("Data successfully uploaded to BigQuery!")
