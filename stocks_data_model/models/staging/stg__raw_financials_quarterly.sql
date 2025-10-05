select *
from {{ source('stocks_model', 'raw_financials_quarterly') }}
