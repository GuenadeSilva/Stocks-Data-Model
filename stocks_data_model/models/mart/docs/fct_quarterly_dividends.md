{% docs fct_quarterly_dividends %}
Dividend fact table sourced from `stg__raw_dividends` and enriched using the
`dim_date` calendar dimension. Each record represents a dividend payout for a
specific company and aligns the payout date to the appropriate quarter-end date
to support consistent time-series and financial aggregation.

Use this model for dividend yield analysis, cash flow modeling,
quarter-over-quarter payout trends, and integration with equity research
reporting pipelines.
{% enddocs %}
