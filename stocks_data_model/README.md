# Stocks Data Model — dbt project (simple refresher)

This is a small, minimal dbt project created to freshen up my memory on how to set up and use dbt. It's intended for practice and experimentation with basic models, seeds, sources, and tests.

Quick commands to try:
- dbt debug
- dbt run
- dbt test

## ETL: Stocks API

The companion script stocks_etl.py performs a simple ETL to prepare upstream data for dbt models:

- Extract
  - Pulls raw price and/or fundamentals data from configured sources (HTTP APIs, CSVs, or a database).
  - Handles authentication, basic rate limiting and paging where required.

- Transform
  - Cleans and normalizes fields (timestamps, tickers).
  - Converts types, fills or drops missing values, and computes derived fields (returns, moving averages, indicators).
  - Shapes output into staging-ready tables/files that dbt consumes.

- Load
  - Persists results to the target used by dbt (seed CSVs or staging tables in the warehouse).
  - Supports idempotent or incremental patterns to avoid duplicates.

- Operational notes
  - Includes logging, error handling, and simple validation checks (row counts, null thresholds).
  - Intended for local runs or lightweight automation (cron / GitHub Actions). See stocks_data_model/stocks_etl.py for implementation details and configuration.

