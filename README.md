# Stocks Data Model — dbt Project

A lightweight dbt project built as a **learning refresher** on core dbt concepts: models, seeds, sources, staging tables, fact/dimension design, and testing. It pulls stock market data via an ETL pipeline and transforms it into a clean analytics-ready warehouse schema.

## Quick Start

```sh
dbt debug
dbt run
dbt test
```

## ETL: Stocks API

The companion script `stocks_etl.py` performs a simple ETL to prepare upstream data for dbt models:

- **Extract**
  - Fetches historical prices, dividends, splits, and financial statements (annual & quarterly) from Yahoo Finance API.
  - Configured tickers: AAPL, MSFT, GOOGL, UBER, NVDA, AMZN.
  - Handles authentication and basic rate limiting.

- **Transform**
  - Normalizes column names and reshapes data.
  - Adds ticker symbols and timestamps.
  - Converts types and handles missing values.

- **Load**
  - Persists results to BigQuery raw datasets (`raw_stock_prices`, `raw_dividends`, `raw_splits`, `raw_company`, `raw_financials_annual`, `raw_financials_quarterly`).
  - Supports idempotent patterns to avoid duplicates.

- **Operational Notes**
  - Includes logging, error handling, and validation checks.
  - See `stocks_etl.py` for implementation details and configuration.

## CI/CD Workflow

GitHub Actions automates testing and deployment:

- **`ci.yml`**: Runs on pull requests — executes `dbt test` to validate data quality and model integrity before merge.
- **`cd.yml`**: Runs on main branch pushes — compiles and deploys dbt models to production.
- **`ci_teardown.yml`**: Cleans up temporary CI artifacts and test datasets.

## Project Structure

```
stocks_data_model/
├── models/               # SQL transformation models
├── macros/               # Custom dbt macros
├── tests/                # dbt tests (schema + data quality)
├── seeds/                # Static reference data
└── dbt_project.yml       # dbt configuration
```

---

**Note**: This is a refresher project emphasizing foundational dbt patterns and modular ETL design.

