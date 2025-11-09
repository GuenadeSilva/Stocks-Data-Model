{% docs fct_financials_quarterly %}
Quarterly financial fact table sourced from `stg__raw_financials_quarterly`.
This model standardizes key income-statement metrics at a quarterly grain,
including total revenue, EBITDA, EBIT, net income, and gross profit. Each row
represents a single company-quarter pairing and is tracked using a surrogate
primary key.

Use this model to support quarterly financial trend analysis, valuation
frameworks requiring intra-year resolution, and downstream reporting pipelines
where timely financial granularity matters.
{% enddocs %}
