{% docs fct_financials_annual %}
Annual financial fact table derived from `int__financials_annual`.
This model standardizes and exposes key income-statement metrics at an annual
grain, including revenue, EBITDA, EBIT, net income, and gross profit. The table
is keyed by a surrogate primary key and includes an ETL timestamp for lineage
and data freshness auditing.

Use this model for macro-level financial trend analysis, valuation modeling,
and integration with downstream financial reporting datasets.
{% enddocs %}
